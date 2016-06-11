package messages

const (
	// RecordMessageSignature is the signature byte for the RECORD message
	RecordMessageSignature = 0x71
)

// RecordMessage Represents an RECORD message
type RecordMessage struct {
	Fields []interface{}
}

// NewRecordMessage Gets a new RecordMessage struct
func NewRecordMessage(fields []interface{}) RecordMessage {
	return RecordMessage{
		Fields: fields,
	}
}

// Signature gets the signature byte for the struct
func (i RecordMessage) Signature() int {
	return RecordMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i RecordMessage) AllFields() []interface{} {
	return []interface{}{i.Fields}
}
