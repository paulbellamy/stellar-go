// Package horizon contains the type definitions for all of horizon's
// response resources.
package horizon

import (
	"github.com/stellar/go/support/render/hal"
)

// ContractDataEntry represents a contract data entry value at a point in time
type ContractDataEntry struct {
	Links struct {
		Self       hal.Link `json:"self"`
		Owner      hal.Link `json:"owner"`
		Ledger     hal.Link `json:"ledger"`
		Operations hal.Link `json:"operations"`
		Effects    hal.Link `json:"effects"`
		Precedes   hal.Link `json:"precedes"`
		Succeeds   hal.Link `json:"succeeds"`
		// Temporarily include Transaction as a link so that Transaction
		// can be fully compatible with TransactionSuccess
		// When TransactionSuccess is removed from the SDKs we can remove this HAL link
		Transaction hal.Link `json:"transaction"`
	} `json:"_links"`
	Owner      string  `json:"owner"`
	ContractId string  `json:"contract_id"`
	Key        string  `json:"key"`
	ValueXdr   *string `json:"value_xdr"`
}

// PagingToken implementation for hal.Pageable
// TODO: Implement this
func (t ContractDataEntry) PagingToken() string {
	return ""
}
