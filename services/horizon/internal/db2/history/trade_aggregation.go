package history

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/support/errors"
	strtime "github.com/stellar/go/support/time"
	"github.com/stellar/go/xdr"
)

// AllowedResolutions is the set of trade aggregation time windows allowed to be used as the
// `resolution` parameter.
var AllowedResolutions = map[time.Duration]struct{}{
	time.Minute:        {}, //1 minute
	time.Minute * 5:    {}, //5 minutes
	time.Minute * 15:   {}, //15 minutes
	time.Hour:          {}, //1 hour
	time.Hour * 24:     {}, //day
	time.Hour * 24 * 7: {}, //week
}

// StrictResolutionFiltering represents a simple feature flag to determine whether only
// predetermined resolutions of trade aggregations are allowed.
var StrictResolutionFiltering = true

// TradeAggregation represents an aggregation of trades from the trades table
type TradeAggregation struct {
	Timestamp     int64   `db:"timestamp"`
	TradeCount    int64   `db:"count"`
	BaseVolume    string  `db:"base_volume"`
	CounterVolume string  `db:"counter_volume"`
	Average       float64 `db:"avg"`
	HighN         int32   `db:"high_n"`
	HighD         int32   `db:"high_d"`
	LowN          int32   `db:"low_n"`
	LowD          int32   `db:"low_d"`
	OpenN         int32   `db:"open_n"`
	OpenD         int32   `db:"open_d"`
	CloseN        int32   `db:"close_n"`
	CloseD        int32   `db:"close_d"`
}

func (t TradeAggregation) High() xdr.Price {
	return xdr.Price{N: xdr.Int32(t.HighN), D: xdr.Int32(t.HighD)}
}

func (t TradeAggregation) Low() xdr.Price {
	return xdr.Price{N: xdr.Int32(t.LowN), D: xdr.Int32(t.LowD)}
}

func (t TradeAggregation) Open() xdr.Price {
	return xdr.Price{N: xdr.Int32(t.OpenN), D: xdr.Int32(t.OpenD)}
}

func (t TradeAggregation) Close() xdr.Price {
	return xdr.Price{N: xdr.Int32(t.CloseN), D: xdr.Int32(t.CloseD)}
}

// TradeAggregationsQ is a helper struct to aid in configuring queries to
// bucket and aggregate trades
type TradeAggregationsQ struct {
	baseAssetID    int64
	counterAssetID int64
	resolution     int64
	offset         int64
	startTime      strtime.Millis
	endTime        strtime.Millis
	pagingParams   db2.PageQuery
}

// GetTradeAggregationsQ initializes a TradeAggregationsQ query builder based on the required parameters
func (q Q) GetTradeAggregationsQ(baseAssetID int64, counterAssetID int64, resolution int64,
	offset int64, pagingParams db2.PageQuery) (*TradeAggregationsQ, error) {

	//convert resolution to a duration struct
	resolutionDuration := time.Duration(resolution) * time.Millisecond
	offsetDuration := time.Duration(offset) * time.Millisecond

	//check if resolution allowed
	if StrictResolutionFiltering {
		if _, ok := AllowedResolutions[resolutionDuration]; !ok {
			return &TradeAggregationsQ{}, errors.New("resolution is not allowed")
		}
	}
	// check if offset is allowed. Offset must be 1) a multiple of an hour 2) less than the resolution and 3)
	// less than 24 hours
	if offsetDuration%time.Hour != 0 || offsetDuration >= time.Hour*24 || offsetDuration > resolutionDuration {
		return &TradeAggregationsQ{}, errors.New("offset is not allowed.")
	}

	return &TradeAggregationsQ{
		baseAssetID:    baseAssetID,
		counterAssetID: counterAssetID,
		resolution:     resolution,
		offset:         offset,
		pagingParams:   pagingParams,
	}, nil
}

// WithStartTime adds an optional lower time boundary filter to the trades being aggregated.
func (q *TradeAggregationsQ) WithStartTime(startTime strtime.Millis) (*TradeAggregationsQ, error) {
	offsetMillis := strtime.MillisFromInt64(q.offset)
	var adjustedStartTime strtime.Millis
	// Round up to offset if the provided start time is less than the offset.
	if startTime < offsetMillis {
		adjustedStartTime = offsetMillis
	} else {
		adjustedStartTime = (startTime - offsetMillis).RoundUp(q.resolution) + offsetMillis
	}
	if !q.endTime.IsNil() && adjustedStartTime > q.endTime {
		return &TradeAggregationsQ{}, errors.New("start time is not allowed")
	} else {
		q.startTime = adjustedStartTime
		return q, nil
	}
}

// WithEndTime adds an upper optional time boundary filter to the trades being aggregated.
func (q *TradeAggregationsQ) WithEndTime(endTime strtime.Millis) (*TradeAggregationsQ, error) {
	// Round upper boundary down, to not deliver partial bucket
	offsetMillis := strtime.MillisFromInt64(q.offset)
	var adjustedEndTime strtime.Millis
	// the end time isn't allowed to be less than the offset
	if endTime < offsetMillis {
		return &TradeAggregationsQ{}, errors.New("end time is not allowed")
	} else {
		adjustedEndTime = (endTime - offsetMillis).RoundDown(q.resolution) + offsetMillis
	}
	if adjustedEndTime < q.startTime {
		return &TradeAggregationsQ{}, errors.New("end time is not allowed")
	} else {
		q.endTime = adjustedEndTime
		return q, nil
	}
}

func (q *TradeAggregationsQ) Select(ctx context.Context, d *Q) ([]TradeAggregation, error) {
	var records []TradeAggregation
	// Use a batch size of 100% of the expected range.
	// TODO: Should this be smaller to do more smaller queries?
	batchTime := time.Duration(q.resolution*int64(q.pagingParams.Limit)) * time.Millisecond
	batchStart := q.startTime
	batchEnd := q.startTime.Add(batchTime)
	// TODO: Pipeline these queries, and stream the responses, to avoid client-waiting-time
	for {
		// Try to fetch all remaining, in a single query.
		batchLimit := q.pagingParams.Limit - uint64(len(records))
		var batch []TradeAggregation
		if err := d.Select(ctx, batch, q.GetSql(batchStart, batchEnd, batchLimit)); err != nil {
			return nil, err
		}
		records = append(records, batch...)
		if uint64(len(records)) == q.pagingParams.Limit {
			// We've got enough
			break
		}
		if batchEnd >= q.endTime {
			// We've reached the end
			break
		}
		// More to fetch
		batchStart = batchEnd
		batchEnd = batchEnd.Add(batchTime)
	}
	return records, nil
}

// GetSql generates a sql statement to aggregate Trades based on given parameters
func (q *TradeAggregationsQ) GetSql(startTime, endTime strtime.Millis, limit uint64) sq.SelectBuilder {
	var orderPreserved bool
	orderPreserved, q.baseAssetID, q.counterAssetID = getCanonicalAssetOrder(q.baseAssetID, q.counterAssetID)

	var bucketSQL sq.SelectBuilder
	if orderPreserved {
		bucketSQL = bucketTrades(q.resolution, q.offset)
	} else {
		bucketSQL = reverseBucketTrades(q.resolution, q.offset)
	}

	bucketSQL = bucketSQL.From("history_trades").
		Where(sq.Eq{"base_asset_id": q.baseAssetID, "counter_asset_id": q.counterAssetID})

	// We limit the response to `limit` buckets so we can adjust
	// startTime and endTime to not go beyond the range visible to the user.
	// Note: we don't add/subtract q.offset because startTime and endTime are
	// already adjusted (see: WithStartTime, WithEndTime).
	if q.pagingParams.Order == db2.OrderAscending {
		maxEndTime := startTime.ToInt64() + int64(limit)*q.resolution
		if endTime.IsNil() || endTime.ToInt64() > maxEndTime {
			endTime = strtime.MillisFromInt64(maxEndTime)
		}
	} else /* db2.OrderDescending */ {
		if endTime.IsNil() {
			endTime = strtime.MillisFromSeconds(time.Now().Unix())
		}
		minStartTime := endTime.ToInt64() - int64(limit)*q.resolution
		if startTime.ToInt64() < minStartTime {
			startTime = strtime.MillisFromInt64(minStartTime)
		}
	}

	bucketSQL = bucketSQL.
		Where(sq.GtOrEq{"ledger_closed_at": startTime.ToTime()}).
		Where(sq.Lt{"ledger_closed_at": endTime.ToTime()})

	//ensure open/close order for cases when multiple trades occur in the same ledger
	bucketSQL = bucketSQL.OrderBy("history_operation_id ", "\"order\"")

	return sq.Select(
		"timestamp",
		"count(*) as count",
		"sum(base_amount) as base_volume",
		"sum(counter_amount) as counter_volume",
		"sum(counter_amount)/sum(base_amount) as avg",
		// We fetch N, D here directly because of lib/pq bug (stellar/go#3345).
		// (Note: [1] is the first array element)
		"(max_price(price))[1] as high_n",
		"(max_price(price))[2] as high_d",
		"(min_price(price))[1] as low_n",
		"(min_price(price))[2] as low_d",
		"(first(price))[1] as open_n",
		"(first(price))[2] as open_d",
		"(last(price))[1] as close_n",
		"(last(price))[2] as close_d",
	).
		FromSelect(bucketSQL, "htrd").
		GroupBy("timestamp").
		Limit(limit).
		OrderBy("timestamp " + q.pagingParams.Order)
}

// formatBucketTimestampSelect formats a sql select clause for a bucketed timestamp, based on given resolution
// and the offset. Given a time t, it gives it a timestamp defined by
// f(t) = ((t - offset)/resolution)*resolution + offset.
func formatBucketTimestampSelect(resolution int64, offset int64) string {
	return fmt.Sprintf("div((cast((extract(epoch from ledger_closed_at) * 1000 ) as bigint) - %d), %d)*%d + %d as timestamp",
		offset, resolution, resolution, offset)
}

// bucketTrades generates a select statement to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution and reversed base/counter.
func bucketTrades(resolution int64, offset int64) sq.SelectBuilder {
	return sq.Select(
		formatBucketTimestampSelect(resolution, offset),
		"history_operation_id",
		"\"order\"",
		"base_asset_id",
		"base_amount",
		"counter_asset_id",
		"counter_amount",
		"ARRAY[price_n, price_d] as price",
	)
}

// reverseBucketTrades generates a select statement to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution and reversed base/counter.
func reverseBucketTrades(resolution int64, offset int64) sq.SelectBuilder {
	return sq.Select(
		formatBucketTimestampSelect(resolution, offset),
		"history_operation_id",
		"\"order\"",
		"counter_asset_id as base_asset_id",
		"counter_amount as base_amount",
		"base_asset_id as counter_asset_id",
		"base_amount as counter_amount",
		"ARRAY[price_d, price_n] as price",
	)
}
