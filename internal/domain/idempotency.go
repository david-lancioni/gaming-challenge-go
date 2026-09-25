package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// PayloadFields são os campos de negócio cobertos pelo hash de idempotência. A
// chave de idempotência, o envelope e os metadados de transporte (headers,
// messageId, occurredAt, correlation ids) ficam de fora de propósito, para que a
// mesma operação tenha o mesmo hash vindo por HTTP ou por SQS.
type PayloadFields struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           TransactionKind
	Money                          Money
	ReferenceExternalTransactionID string
}

func ComputePayloadHash(p PayloadFields) string {
	sum := sha256.Sum256(CanonicalPayload(p))
	return hex.EncodeToString(sum[:])
}

// CanonicalPayload gera o JSON canônico que é hasheado (SHA-256): objeto plano com
// chaves em ordem lexicográfica (encoding/json ordena as chaves de um map), UTF-8,
// sem espaços e sem escape de HTML. O valor entra na forma canônica de Money.
func CanonicalPayload(p PayloadFields) []byte {
	fields := map[string]string{
		"providerId":            p.ProviderID,
		"externalTransactionId": p.ExternalTransactionID,
		"playerId":              p.PlayerID,
		"walletId":              p.WalletID,
		"roundId":               p.RoundID,
		"gameId":                p.GameID,
		"kind":                  string(p.Kind),
		"amount":                p.Money.Amount(),
		"currency":              string(p.Money.Currency()),
	}
	if p.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = p.ReferenceExternalTransactionID
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(fields)
	return bytes.TrimRight(buf.Bytes(), "\n")
}
