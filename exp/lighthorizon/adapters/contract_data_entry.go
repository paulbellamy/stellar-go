package adapters

import (
	"encoding/base64"
	"fmt"
	"net/http"

	protocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/xdr"
)

func PopulateContractDataEntry(r *http.Request, entry *xdr.ContractDataEntry) (protocol.ContractDataEntry, error) {
	keyXdr, err := entry.Key.MarshalBinary()
	if err != nil {
		return protocol.ContractDataEntry{}, err
	}
	key := base64.URLEncoding.EncodeToString(keyXdr)

	var value *string
	if entry.Val != nil {
		valueXdr, err := entry.Val.MarshalBinary()
		if err != nil {
			return protocol.ContractDataEntry{}, err
		}
		valueStr := base64.URLEncoding.EncodeToString(valueXdr)
		value = &valueStr
	}

	base := protocol.ContractDataEntry{
		Owner:      entry.Owner.Address(),
		ContractId: fmt.Sprint(entry.ContractId),
		Key:        key,
		ValueXdr:   value,
	}

	lb := hal.LinkBuilder{Base: r.URL}
	self := fmt.Sprintf("/contracts/%s/%s/data/%s", base.Owner, base.ContractId, base.Key)
	base.Links.Self = lb.Link(self)
	base.Links.Owner = lb.Linkf("/accounts/%s", base.Owner)
	// TODO
	// base.Links.Ledger = lb.Linkf("/ledgers/TODO")
	// base.Links.Operations = lb.Linkf("/operations/TODO")
	// base.Links.Effects = lb.Link(self, "effects")
	// base.Links.Precedes = lb.Linkf("/effects?order=asc&cursor=%s", base.PT)
	// base.Links.Succeeds = lb.Linkf("/effects?order=desc&cursor=%s", base.PT)
	return base, nil
}
