package messages

import "fmt"

const (
	// FailureMessageSignature is the signature byte for the FAILURE message
	FailureMessageSignature = 0x7F
)

// FailureMessage Represents an FAILURE message
type FailureMessage struct {
	Metadata map[string]interface{}
}

// NewFailureMessage Gets a new FailureMessage struct
func NewFailureMessage(metadata map[string]interface{}) FailureMessage {
	return FailureMessage{
		Metadata: metadata,
	}
}

// Signature gets the signature byte for the struct
func (i FailureMessage) Signature() int {
	return FailureMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i FailureMessage) AllFields() []interface{} {
	return []interface{}{i.Metadata}
}

// Error is the implementation of the Golang error interface so a failure message
// can be treated like a normal error
func (i FailureMessage) Error() string {
	return fmt.Sprintf("%#v", i)
}
