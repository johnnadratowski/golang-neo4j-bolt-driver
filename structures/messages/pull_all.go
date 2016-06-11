package messages

const (
	// PullAllMessageSignature is the signature byte for the PULL_ALL message
	PullAllMessageSignature = 0x3F
)

// PullAllMessage Represents an PULL_ALL message
type PullAllMessage struct{}

// NewPullAllMessage Gets a new PullAllMessage struct
func NewPullAllMessage() PullAllMessage {
	return PullAllMessage{}
}

// Signature gets the signature byte for the struct
func (i PullAllMessage) Signature() int {
	return PullAllMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i PullAllMessage) AllFields() []interface{} {
	return []interface{}{}
}
