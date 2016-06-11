package messages

const (
	// ResetMessageSignature is the signature byte for the RESET message
	ResetMessageSignature = 0x0F
)

// ResetMessage Represents an RESET message
type ResetMessage struct{}

// NewResetMessage Gets a new ResetMessage struct
func NewResetMessage() ResetMessage {
	return ResetMessage{}
}

// Signature gets the signature byte for the struct
func (i ResetMessage) Signature() int {
	return ResetMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i ResetMessage) AllFields() []interface{} {
	return []interface{}{}
}
