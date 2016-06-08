package structures

type Structure interface {
	Signature() int
	Fields() []interface{}
}
