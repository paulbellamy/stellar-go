package common

import (
	"encoding/hex"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

type Transaction struct {
	TransactionEnvelope *xdr.TransactionEnvelope
	TransactionResult   *xdr.TransactionResult
	LedgerHeader        *xdr.LedgerHeader
	TxIndex             int32
	Changes             []ingest.Change
}

func (o *Transaction) TransactionHash() (string, error) {
	hash, err := network.HashTransactionInEnvelope(*o.TransactionEnvelope, network.PublicNetworkPassphrase)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hash[:]), nil
}

func (o *Transaction) SourceAccount() xdr.MuxedAccount {
	return o.TransactionEnvelope.SourceAccount()
}

func (o *Transaction) TOID() int64 {
	return toid.New(
		int32(o.LedgerHeader.LedgerSeq),
		o.TxIndex+1,
		1,
	).ToInt64()
}

func (o *Transaction) ChangedContractDataKeys() []*xdr.LedgerKey {
	var keys []*xdr.LedgerKey
	for _, change := range o.Changes {
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}
		var entry *xdr.ContractDataEntry
		if change.Pre != nil {
			entry = change.Pre.Data.ContractData
		} else if change.Post != nil {
			entry = change.Post.Data.ContractData
		}
		if entry == nil {
			panic("invalid ledger entry change")
		}
		keys = append(keys, &xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.LedgerKeyContractData{
				Owner:      entry.Owner,
				ContractId: entry.ContractId,
				Key:        entry.Key,
			},
		})
	}
	return keys
}
