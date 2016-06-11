package messages

const (
	// AckFailureMessageSignature is the signature byte for the ACK_FAILURE message
	AckFailureMessageSignature = 0x0E
)

// AckFailureMessage Represents an ACK_FAILURE message
type AckFailureMessage struct{}

// NewAckFailureMessage Gets a new AckFailureMessage struct
func NewAckFailureMessage() AckFailureMessage {
	return AckFailureMessage{}
}

// Signature gets the signature byte for the struct
func (i AckFailureMessage) Signature() int {
	return AckFailureMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i AckFailureMessage) AllFields() []interface{} {
	return []interface{}{}
}
