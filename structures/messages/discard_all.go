package messages

const (
	// DiscardAllMessageSignature is the signature byte for the DISCARD_ALL message
	DiscardAllMessageSignature = 0x2F
)

// DiscardAllMessage Represents an DISCARD_ALL message
type DiscardAllMessage struct{}

// NewDiscardAllMessage Gets a new DiscardAllMessage struct
func NewDiscardAllMessage() DiscardAllMessage {
	return DiscardAllMessage{}
}

// Signature gets the signature byte for the struct
func (i DiscardAllMessage) Signature() int {
	return DiscardAllMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i DiscardAllMessage) AllFields() []interface{} {
	return []interface{}{}
}
