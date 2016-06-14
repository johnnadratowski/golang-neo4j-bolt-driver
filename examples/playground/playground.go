package main

import "fmt"

import (
	"bytes"
	"encoding/binary"
	"math"
)

func main() {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int8(-1))
	fmt.Printf("BYTES: %#x\n", buf.Bytes())
	fmt.Printf("INT1: %#x\n", byte(1))
	fmt.Printf("INT34234234: %#x\n", byte(234))
	fmt.Println("DONE")

	fmt.Printf("MAX INT8: %d\n", math.MaxInt8)
	fmt.Printf("MAX INT16: %d\n", math.MaxInt16)
	fmt.Printf("MAX INT32: %d\n", math.MaxInt32)
	fmt.Printf("MAX INT64: %d\n", math.MaxInt64)

	fmt.Printf("MIN INT8: %d\n",  math.MinInt8)
	fmt.Printf("MIN INT16: %d\n", math.MinInt16)
	fmt.Printf("MIN INT32: %d\n", math.MinInt32)
	fmt.Printf("MIN INT64: %d\n", math.MinInt64)

	fmt.Printf("MAX UINT8: %d\n", math.MaxUint8)
	fmt.Printf("MAX UINT16: %d\n", math.MaxUint16)
	fmt.Printf("MAX UINT32: %d\n", math.MaxUint32)
	x := fmt.Sprint(uint64(math.MaxUint64))
	fmt.Printf("MAX UINT64: %s\n", x)
}
