package messages

import "io"

type Message interface {
	Signature() rune
	Marker() rune
	Write(io.Writer) (int64, error)
}
