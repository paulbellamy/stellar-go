package actions

import (
	"net/http"

	"github.com/stellar/go/services/horizon/internal/ledger"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/xdr"
)

// ContractDataQuery query struct for contract data end-points
type ContractDataQuery struct {
	// TODO: validation on these
	OwnerID    string `schema:"owner_id" valid:"-"`
	ContractID int64  `schema:"contract_id" valid:"-"`
	Key        string `schema:"key" valid:"-"`
}

// Validate runs extra validations on query parameters
func (qp ContractDataQuery) Validate() error {
	return nil
}

// GetContractDataHandler is the action handler for all end-points returning a list of operations.
type GetContractDataHandler struct {
	LedgerState *ledger.State
}

// GetResourcePage returns a page of operations.
func (handler GetContractDataHandler) GetResourcePage(w HeaderWriter, r *http.Request) ([]hal.Pageable, error) {

	pq, err := GetPageQuery(handler.LedgerState, r)
	if err != nil {
		return nil, err
	}

	err = validateCursorWithinHistory(handler.LedgerState, pq)
	if err != nil {
		return nil, err
	}

	qp := ContractDataQuery{}
	err = getParams(&qp, r)
	if err != nil {
		return nil, err
	}

	// TODO: Load this from a real data source
	str := "hello world"
	obj := &xdr.ScObject{
		Type: xdr.ScObjectTypeScoString,
		Str:  &str,
	}
	return []hal.Pageable{
		ContractData{
			Data: &xdr.ScVal{
				Type: xdr.ScValTypeScvObject,
				Obj:  &obj,
			},
		},
	}, nil
}

type ContractData struct {
	Data *xdr.ScVal `json:"data"`
}

func (d ContractData) PagingToken() string {
	// TODO: Implement this
	return ""
}
