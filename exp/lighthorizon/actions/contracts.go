package actions

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/stellar/go/exp/lighthorizon/adapters"
	"github.com/stellar/go/exp/lighthorizon/archive"
	"github.com/stellar/go/exp/lighthorizon/index"
	hProtocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/toid"
)

func Contracts(archiveWrapper archive.Wrapper, indexStore index.Store) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// For _links rendering, imitate horizon.stellar.org links for horizon-cmp
		r.URL.Scheme = "http"
		r.URL.Host = "localhost:8080"

		if r.Method != "GET" {
			return
		}

		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			fmt.Fprintf(w, "Error: %v", err)
			return
		}

		var cursor int64
		if query.Get("cursor") == "" {
			cursor = toid.New(1, 1, 1).ToInt64()
		} else {
			cursor, err = strconv.ParseInt(query.Get("cursor"), 10, 64)
			if err != nil {
				fmt.Fprintf(w, "Error: %v", err)
				return
			}
		}

		var limit int64
		if query.Get("limit") == "" {
			limit = 10
		} else {
			limit, err = strconv.ParseInt(query.Get("limit"), 10, 64)
			if err != nil {
				fmt.Fprintf(w, "Error: %v", err)
				return
			}
		}

		if limit == 0 || limit > 200 {
			limit = 10
		}

		page := hal.Page{}
		page.Init()
		page.FullURL = r.URL

		// For now, use a query param for now to avoid dragging in chi-router. Not
		// really the point of the experiment yet.
		owner := query.Get("owner")
		id := query.Get("id")
		key := query.Get("key")
		if owner == "" {
			fmt.Fprint(w, "missing owner query param", err)
			return
		}
		if id == "" {
			fmt.Fprint(w, "missing id query param", err)
			return
		}
		if key == "" {
			fmt.Fprint(w, "missing key query param", err)
			return
		}

		// Skip the cursor ahead to the next active checkpoint for this account
		// TODO: do this in reverse order to fetch the latest change
		checkpoint, err := indexStore.NextActive(owner, fmt.Sprintf("contract_%s_%s", id, key), uint32(toid.Parse(cursor).LedgerSequence/64))
		if err == io.EOF {
			// never active. No results.
			page.PopulateLinks()

			encoder := json.NewEncoder(w)
			encoder.SetIndent("", "  ")
			err = encoder.Encode(page)
			if err != nil {
				fmt.Fprintf(w, "Error: %v", err)
				return
			}
			return
		} else if err != nil {
			fmt.Fprintf(w, "Error: %v", err)
			return
		}
		ledger := int32(checkpoint * 64)
		if ledger < 0 {
			// Check we don't overflow going from uint32 -> int32
			fmt.Fprintf(w, "Error: Ledger overflow")
			return
		}
		cursor = toid.New(ledger, 1, 1).ToInt64()

		// TODO: Keep fetching transactions until we hit limit or pass the last
		txns, err := archiveWrapper.GetTransactions(cursor, limit)
		if err != nil {
			fmt.Fprintf(w, "Error: %v", err)
			return
		}

		// TODO: Populate with the contract data here
		for _, txn := range txns {
			hash, err := txn.TransactionHash()
			if err != nil {
				fmt.Fprintf(w, "Error: %v", err)
				return
			}
			if id != "" && hash != id {
				continue
			}
			var response hProtocol.Transaction
			response, err = adapters.PopulateTransaction(r, &txn)
			if err != nil {
				fmt.Fprintf(w, "Error: %v", err)
				return
			}

			page.Add(response)
		}

		page.PopulateLinks()

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(page)
		if err != nil {
			fmt.Fprintf(w, "Error: %v", err)
			return
		}
	}
}
