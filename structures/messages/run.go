package messages

const (
	// RunMessageSignature is the signature byte for the RUN message
	RunMessageSignature = 0x10
)

// RunMessage Represents an RUN message
type RunMessage struct {
	statement  string
	parameters map[string]interface{}
}

// NewRunMessage Gets a new RunMessage struct
func NewRunMessage(statement string, parameters map[string]interface{}) RunMessage {
	return RunMessage{
		statement:  statement,
		parameters: parameters,
	}
}

// Signature gets the signature byte for the struct
func (i RunMessage) Signature() int {
	return RunMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i RunMessage) AllFields() []interface{} {
	return []interface{}{i.statement, i.parameters}
}
