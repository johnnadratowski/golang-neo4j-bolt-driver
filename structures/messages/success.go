package messages

const (
	// SuccessMessageSignature is the signature byte for the SUCCESS message
	SuccessMessageSignature = 0x70
)

// SuccessMessage Represents an SUCCESS message
type SuccessMessage struct {
	Metadata map[string]interface{}
}

// NewSuccessMessage Gets a new SuccessMessage struct
func NewSuccessMessage(metadata map[string]interface{}) SuccessMessage {
	return SuccessMessage{
		Metadata: metadata,
	}
}

// Signature gets the signature byte for the struct
func (i SuccessMessage) Signature() int {
	return SuccessMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i SuccessMessage) AllFields() []interface{} {
	return []interface{}{i.Metadata}
}
