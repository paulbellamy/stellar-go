package main

import (
	"context"
	"fmt"
	"log"

	"github.com/stellar/go/bitmask/toid"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

func main() {
	ctx := context.Background()

	archivePool, err := historyarchive.NewArchivePool(
		[]string{"https://history.stellar.org/prd/core-testnet/core_testnet_001"},
		historyarchive.ConnectOptions{
			NetworkPassphrase:   network.TestNetworkPassphrase,
			CheckpointFrequency: historyarchive.DefaultCheckpointFrequency,
			Context:             ctx,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	state, err := archivePool.GetRootHAS()
	if err != nil {
		log.Fatal(err)
	}
	latestLedger := state.CurrentLedger
	latestBlock := (latestLedger / 64) * 64
	log.Println("Latest Block:", latestBlock)

	txMasks := map[string][]byte{}
	opMasks := map[string][]byte{}

	updateMask := func(m []byte, sequence uint32) []byte {
		archive := (sequence / 64) * 64
		byteOffset := archive / 8
		if len(m) < int(byteOffset) {
			needed := int(byteOffset) - len(m)
			m = append(m, make([]byte, needed, needed)...)
		}

		// TODO: No idea if this is right. I'm super jetlagged.
		m[byteOffset] &= 0x01 << (archive % 8)
		return m
	}

	addTransactionParticipants := func(
		sequence uint32,
		transaction ingest.LedgerTransaction,
	) error {
		transactionParticipants, err := participantsForTransaction(
			sequence,
			transaction,
		)
		if err != nil {
			return errors.Wrap(err, "Could not determine participants for transaction")
		}

		for _, participant := range transactionParticipants {
			address := participant.Address()
			txMasks[address] = updateMask(txMasks[address], sequence)
		}

		return nil
	}

	addOperationsParticipants := func(
		sequence uint32,
		transaction ingest.LedgerTransaction,
	) error {
		participants, err := operationsParticipants(transaction, sequence)
		if err != nil {
			return errors.Wrap(err, "could not determine operation participants")
		}

		for _, p := range participants {
			for _, participant := range p {
				address := participant.Address()
				opMasks[address] = updateMask(opMasks[address], sequence)
			}
		}

		return nil
	}

	processTransaction := func(ctx context.Context, sequence uint32, xdrTxn xdr.TransactionEnvelope, xdrResult xdr.TransactionResultPair) (err error) {

		transaction := ingest.LedgerTransaction{
			Index:    uint32(toid.New(int32(sequence), int32(xdrTxn.SeqNum()), 0).ToInt64()),
			Envelope: xdrTxn,
			Result:   xdrResult,

			// TODO: figure out if we need these. Maybe good enough for now.
			FeeChanges: xdr.LedgerEntryChanges{},
			UnsafeMeta: xdr.TransactionMeta{},
		}

		err = addTransactionParticipants(sequence, transaction)
		if err != nil {
			return err
		}

		err = addOperationsParticipants(sequence, transaction)
		if err != nil {
			return err
		}

		return nil
	}

	for start := uint32(0); start < latestBlock; start += 64 {
		ledgers, err := archivePool.GetLedgers(start, start+64)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Got Archive:", start, "->", start+64)
		for _, ledger := range ledgers {
			for i, tx := range ledger.Transaction.TxSet.Txs {
				if err := processTransaction(ctx, uint32(ledger.Transaction.LedgerSeq), tx, ledger.TransactionResult.TxResultSet.Results[i]); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

func participantsForChanges(
	changes xdr.LedgerEntryChanges,
) ([]xdr.AccountId, error) {
	var participants []xdr.AccountId

	for _, c := range changes {
		var participant *xdr.AccountId

		switch c.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			participant = participantsForLedgerEntry(c.MustCreated())
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			participant = participantsForLedgerKey(c.MustRemoved())
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			participant = participantsForLedgerEntry(c.MustUpdated())
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			participant = participantsForLedgerEntry(c.MustState())
		default:
			return nil, errors.Errorf("Unknown change type: %s", c.Type)
		}

		if participant != nil {
			participants = append(participants, *participant)
		}
	}

	return participants, nil
}

func participantsForLedgerEntry(le xdr.LedgerEntry) *xdr.AccountId {
	if le.Data.Type != xdr.LedgerEntryTypeAccount {
		return nil
	}
	aid := le.Data.MustAccount().AccountId
	return &aid
}

func participantsForLedgerKey(lk xdr.LedgerKey) *xdr.AccountId {
	if lk.Type != xdr.LedgerEntryTypeAccount {
		return nil
	}
	aid := lk.MustAccount().AccountId
	return &aid
}

func participantsForMeta(
	meta xdr.TransactionMeta,
) ([]xdr.AccountId, error) {
	var participants []xdr.AccountId
	if meta.Operations == nil {
		return participants, nil
	}

	for _, op := range *meta.Operations {
		var accounts []xdr.AccountId
		accounts, err := participantsForChanges(op.Changes)
		if err != nil {
			return nil, err
		}

		participants = append(participants, accounts...)
	}

	return participants, nil
}

func participantsForTransaction(
	sequence uint32,
	transaction ingest.LedgerTransaction,
) ([]xdr.AccountId, error) {
	participants := []xdr.AccountId{
		transaction.Envelope.SourceAccount().ToAccountId(),
	}
	if transaction.Envelope.IsFeeBump() {
		participants = append(participants, transaction.Envelope.FeeBumpAccount().ToAccountId())
	}

	p, err := participantsForMeta(transaction.UnsafeMeta)
	if err != nil {
		return nil, err
	}
	participants = append(participants, p...)

	p, err = participantsForChanges(transaction.FeeChanges)
	if err != nil {
		return nil, err
	}
	participants = append(participants, p...)

	for opi, op := range transaction.Envelope.Operations() {
		operation := transactionOperationWrapper{
			index:          uint32(opi),
			transaction:    transaction,
			operation:      op,
			ledgerSequence: sequence,
		}

		p, err := operation.Participants()
		if err != nil {
			return nil, errors.Wrapf(
				err, "could not determine operation %v participants", operation.ID(),
			)
		}
		participants = append(participants, p...)
	}

	return dedupeParticipants(participants), nil
}

// transactionOperationWrapper represents the data for a single operation within a transaction
type transactionOperationWrapper struct {
	index          uint32
	transaction    ingest.LedgerTransaction
	operation      xdr.Operation
	ledgerSequence uint32
}

// ID returns the ID for the operation.
func (operation *transactionOperationWrapper) ID() int64 {
	return toid.New(
		int32(operation.ledgerSequence),
		int32(operation.transaction.Index),
		int32(operation.index+1),
	).ToInt64()
}

// Order returns the operation order.
func (operation *transactionOperationWrapper) Order() uint32 {
	return operation.index + 1
}

// TransactionID returns the id for the transaction related with this operation.
func (operation *transactionOperationWrapper) TransactionID() int64 {
	return toid.New(int32(operation.ledgerSequence), int32(operation.transaction.Index), 0).ToInt64()
}

// SourceAccount returns the operation's source account.
func (operation *transactionOperationWrapper) SourceAccount() *xdr.MuxedAccount {
	sourceAccount := operation.operation.SourceAccount
	if sourceAccount != nil {
		return sourceAccount
	} else {
		ret := operation.transaction.Envelope.SourceAccount()
		return &ret
	}
}

// OperationType returns the operation type.
func (operation *transactionOperationWrapper) OperationType() xdr.OperationType {
	return operation.operation.Body.Type
}

func (operation *transactionOperationWrapper) getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
	if change.Type != xdr.LedgerEntryTypeAccount || change.Post == nil {
		return nil
	}

	preSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}

	account := change.Post.Data.MustAccount()
	postSigners := account.SponsorPerSigner()

	pre, preFound := preSigners[signerKey]
	post, postFound := postSigners[signerKey]

	if !postFound {
		return nil
	}

	if preFound {
		formerSponsor := pre.Address()
		newSponsor := post.Address()
		if formerSponsor == newSponsor {
			return nil
		}
	}

	return &post
}

func (operation *transactionOperationWrapper) getSponsor() (*xdr.AccountId, error) {
	changes, err := operation.transaction.GetOperationChanges(operation.index)
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := operation.getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
				return sponsorAccount, nil
			}
		}

		// Check Ledger key changes
		if c.Pre != nil || c.Post == nil {
			// We are only looking for entry creations denoting that a sponsor
			// is associated to the ledger entry of the operation.
			continue
		}
		if sponsorAccount := c.Post.SponsoringID(); sponsorAccount != nil {
			return sponsorAccount, nil
		}
	}

	return nil, nil
}

func (operation *transactionOperationWrapper) findInitatingBeginSponsoringOp() *transactionOperationWrapper {
	if !operation.transaction.Result.Successful() {
		// Failed transactions may not have a compliant sandwich structure
		// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
		// and thus we bail out since we could return incorrect information.
		return nil
	}
	sponsoree := operation.SourceAccount().ToAccountId()
	operations := operation.transaction.Envelope.Operations()
	for i := int(operation.index) - 1; i >= 0; i-- {
		if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
			beginOp.SponsoredId.Address() == sponsoree.Address() {
			result := *operation
			result.index = uint32(i)
			result.operation = operations[i]
			return &result
		}
	}
	return nil
}

func getLedgerKeyParticipants(ledgerKey xdr.LedgerKey) []xdr.AccountId {
	var result []xdr.AccountId
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result = append(result, ledgerKey.Account.AccountId)
	case xdr.LedgerEntryTypeClaimableBalance:
		// nothing to do
	case xdr.LedgerEntryTypeData:
		result = append(result, ledgerKey.Data.AccountId)
	case xdr.LedgerEntryTypeOffer:
		result = append(result, ledgerKey.Offer.SellerId)
	case xdr.LedgerEntryTypeTrustline:
		result = append(result, ledgerKey.TrustLine.AccountId)
	}
	return result
}

// Participants returns the accounts taking part in the operation.
func (operation *transactionOperationWrapper) Participants() ([]xdr.AccountId, error) {
	participants := []xdr.AccountId{}
	participants = append(participants, operation.SourceAccount().ToAccountId())
	op := operation.operation

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		participants = append(participants, op.Body.MustCreateAccountOp().Destination)
	case xdr.OperationTypePayment:
		participants = append(participants, op.Body.MustPaymentOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictReceive:
		participants = append(participants, op.Body.MustPathPaymentStrictReceiveOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictSend:
		participants = append(participants, op.Body.MustPathPaymentStrictSendOp().Destination.ToAccountId())
	case xdr.OperationTypeManageBuyOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreatePassiveSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetOptions:
		// the only direct participant is the source_account
	case xdr.OperationTypeChangeTrust:
		// the only direct participant is the source_account
	case xdr.OperationTypeAllowTrust:
		participants = append(participants, op.Body.MustAllowTrustOp().Trustor)
	case xdr.OperationTypeAccountMerge:
		participants = append(participants, op.Body.MustDestination().ToAccountId())
	case xdr.OperationTypeInflation:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageData:
		// the only direct participant is the source_account
	case xdr.OperationTypeBumpSequence:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreateClaimableBalance:
		for _, c := range op.Body.MustCreateClaimableBalanceOp().Claimants {
			participants = append(participants, c.MustV0().Destination)
		}
	case xdr.OperationTypeClaimClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		participants = append(participants, op.Body.MustBeginSponsoringFutureReservesOp().SponsoredId)
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			participants = append(participants, beginSponsorshipOp.SourceAccount().ToAccountId())
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			participants = append(participants, getLedgerKeyParticipants(*op.LedgerKey)...)
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			participants = append(participants, op.Signer.AccountId)
			// We don't add signer as a participant because a signer can be arbitrary account.
			// This can spam successful operations history of any account.
		}
	case xdr.OperationTypeClawback:
		op := operation.operation.Body.MustClawbackOp()
		participants = append(participants, op.From.ToAccountId())
	case xdr.OperationTypeClawbackClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.operation.Body.MustSetTrustLineFlagsOp()
		participants = append(participants, op.Trustor)
	case xdr.OperationTypeLiquidityPoolDeposit:
		// the only direct participant is the source_account
	case xdr.OperationTypeLiquidityPoolWithdraw:
		// the only direct participant is the source_account
	default:
		return participants, fmt.Errorf("Unknown operation type: %s", op.Body.Type)
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		participants = append(participants, *sponsor)
	}

	return dedupeParticipants(participants), nil
}

// dedupeParticipants remove any duplicate ids from `in`
func dedupeParticipants(in []xdr.AccountId) (out []xdr.AccountId) {
	set := map[string]xdr.AccountId{}
	for _, id := range in {
		set[id.Address()] = id
	}

	for _, id := range set {
		out = append(out, id)
	}
	return
}

// OperationsParticipants returns a map with all participants per operation
func operationsParticipants(transaction ingest.LedgerTransaction, sequence uint32) (map[int64][]xdr.AccountId, error) {
	participants := map[int64][]xdr.AccountId{}

	for opi, op := range transaction.Envelope.Operations() {
		operation := transactionOperationWrapper{
			index:          uint32(opi),
			transaction:    transaction,
			operation:      op,
			ledgerSequence: sequence,
		}

		p, err := operation.Participants()
		if err != nil {
			return participants, errors.Wrapf(err, "reading operation %v participants", operation.ID())
		}
		participants[operation.ID()] = p
	}

	return participants, nil
}
