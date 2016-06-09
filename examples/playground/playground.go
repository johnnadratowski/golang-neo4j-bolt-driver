package main

import "fmt"

import (
	"bytes"
	"encoding/binary"
)

func main() {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int8(-1))
	fmt.Printf("BYTES: %#x\n", buf.Bytes())
	fmt.Printf("INT1: %#x\n", byte(1))
	fmt.Printf("INT34234234: %#x\n", byte(234))
	fmt.Println("DONE")
}
