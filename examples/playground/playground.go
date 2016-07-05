package main

import "fmt"

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"math"
)

type test struct {
	b []byte
}

func main() {

	t := &test{[]byte{1, 2, 3}}
	t1 := &test{}
	fmt.Printf("TEE: %#v\n", t)
	fmt.Printf("TEE1: %#v\n", t1)
	*t1 = *t
	t = nil
	fmt.Printf("TEE: %#v\n", t)
	fmt.Printf("TEE1: %#v\n", t1)

	args := map[string]interface{}{
		"a": 1,
		"b": 34234.34323,
		"c": "string",
		"d": []interface{}{1, 2, 3},
		"e": true,
		"f": nil,
	}

	enc, err := encoding.Marshal(args)
	if err != nil {
		fmt.Printf("MARSHALLERR: %#v\n", err)
	}
	fmt.Printf("ENCODED: %#v\n", enc)

	decoded, err := encoding.Unmarshal(enc)
	if err != nil {
		fmt.Printf("UNMARSHALLERR: %#v\n", err)
	}

	fmt.Printf("DECODED: %#v\n", decoded)

	buf := &bytes.Buffer{}
	err = gob.NewEncoder(buf).Encode(args)
	if err != nil {
		fmt.Printf("GOBERR: %#v", err)
	}

	var y map[string]interface{}
	gob.NewDecoder(buf).Decode(&y)

	fmt.Printf("GOB: %#v\n", y)

	fmt.Printf("LenMake: %d\n", len(make([]interface{}, 15)))

	fmt.Println("test.b:", len(test{}.b))
	buf = bytes.NewBuffer([]byte{byte(0xf0)})
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
	z := fmt.Sprint(uint64(math.MaxUint64))
	fmt.Printf("MAX UINT64: %s\n", z)

	fmt.Println("Marker: 0xf0 ", 0xf0, "INT: ", int(byte(0xf0)))
	fmt.Println("Marker: 0xf0 byte{}", 0xf0, "INT: ", int(0xf0))
	_ = `b1 71 99 cb  80 0 0 0  0 0 0 0  ca 80 0 0

	 0 c9 80 0  c8 80 f0 7f  c9 7f ff ca  7f ff ff ff

	 cb 7f ff ff  ff ff ff ff  ff`
}
