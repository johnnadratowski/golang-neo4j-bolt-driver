package main

import "fmt"

import (
	"bytes"
	"encoding/binary"
	"math"
)

func main() {

	buf := bytes.NewBuffer([]byte{byte(0xf0)})
	var x int
	binary.Read(buf, binary.BigEndian, &x)
	fmt.Println("Marker: 0xf0 ", 0xf0, "INT: ", x)

	buf = bytes.NewBuffer([]byte{byte(0xff)})
	binary.Read(buf, binary.BigEndian, &x)
	fmt.Println("Marker: 0xff ", 0xff, "INT: ", x)

	buf = bytes.NewBuffer([]byte{byte(0x7f)})
	binary.Read(buf, binary.BigEndian, &x)
	fmt.Println("Marker: 0x7f ", 0x7f, "INT: ", x)

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int8(-1))
	fmt.Printf("BYTES: %#x\n", buf.Bytes())
	binary.Write(buf, binary.BigEndian, int8(127))
	fmt.Printf("BYTES: %#x\n", buf.Bytes())
	fmt.Printf("INT1: %#x\n", byte(1))
	fmt.Printf("INT34234234: %#x\n", byte(234))
	fmt.Println("DONE")

	fmt.Printf("MAX INT8: %d\n", math.MaxInt8)
	fmt.Printf("MAX INT16: %d\n", math.MaxInt16)
	fmt.Printf("MAX INT32: %d\n", math.MaxInt32)
	fmt.Printf("MAX INT64: %d\n", math.MaxInt64)

	fmt.Printf("MIN INT8: %d\n", math.MinInt8)
	fmt.Printf("MIN INT16: %d\n", math.MinInt16)
	fmt.Printf("MIN INT32: %d\n", math.MinInt32)
	fmt.Printf("MIN INT64: %d\n", math.MinInt64)

	fmt.Printf("MAX UINT8: %d\n", math.MaxUint8)
	fmt.Printf("MAX UINT16: %d\n", math.MaxUint16)
	fmt.Printf("MAX UINT32: %d\n", math.MaxUint32)
	y := fmt.Sprint(uint64(math.MaxUint64))
	fmt.Printf("MAX UINT64: %s\n", y)

	fmt.Println("Marker: 0xf0 ", 0xf0, "INT: ", int(byte(0xf0)))
	fmt.Println("Marker: 0xf0 byte{}", 0xf0, "INT: ", int(0xf0))
	_ = `b1 71 99 cb  80 0 0 0  0 0 0 0  ca 80 0 0

	 0 c9 80 0  c8 80 f0 7f  c9 7f ff ca  7f ff ff ff

	 cb 7f ff ff  ff ff ff ff  ff`
}
