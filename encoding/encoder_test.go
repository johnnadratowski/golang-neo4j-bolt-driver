package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/ONSdigital/golang-neo4j-bolt-driver/errors"
)

const (
	maxBufSize = math.MaxUint16
	maxKeySize = 10
)

func createNewTestEncoder() (Encoder, io.Reader) {
	buf := bytes.NewBuffer([]byte{})
	return NewEncoder(buf, maxBufSize), buf
}

func TestEncodeNil(t *testing.T) {
	encoder, buf := createNewTestEncoder()

	err := encoder.Encode(nil)

	if err != nil {
		t.Fatalf("Error while encoding: %v", err)
	}

	output := make([]byte, maxBufSize)
	outputCount, err := buf.Read(output)

	if err != nil {
		t.Fatalf("Error while reading output: %v", err)
	}

	expectedBuf := bytes.NewBuffer([]byte{})
	expected := make([]byte, maxBufSize)

	binary.Write(expectedBuf, binary.BigEndian, uint16(1))
	expectedBuf.Write([]byte{NilMarker})
	expectedBuf.Write(EndMessage)

	expectedCount, _ := expectedBuf.Read(expected)

	if !reflect.DeepEqual(output[:outputCount], expected[:expectedCount]) {
		t.Fatalf("Unexpected Nil encoding. Expected %v. Got %v", expected[:expectedCount], output[:outputCount])
	}
}

func TestEncodeBool(t *testing.T) {
	expected := func(val bool) []byte {
		expectedBuf := bytes.NewBuffer([]byte{})
		expected := make([]byte, maxBufSize)

		binary.Write(expectedBuf, binary.BigEndian, uint16(1))

		var marker byte

		if val == true {
			marker = TrueMarker
		} else {
			marker = FalseMarker
		}

		expectedBuf.Write([]byte{marker})
		expectedBuf.Write(EndMessage)

		expectedCount, _ := expectedBuf.Read(expected)

		return expected[:expectedCount]
	}

	result := func(val bool) []byte {
		encoder, buf := createNewTestEncoder()

		err := encoder.Encode(val)

		if err != nil {
			t.Fatalf("Error while encoding: %v", err)
		}

		output := make([]byte, maxBufSize)
		outputCount, err := buf.Read(output)

		if err != nil {
			t.Fatalf("Error while reading output: %v", err)
		}

		return output[:outputCount]
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func generateIntExpectedBuf(val int64) ([]byte, error) {
	expectedBuf := bytes.NewBuffer([]byte{})
	expected := make([]byte, maxBufSize)

	switch {
	case val >= math.MinInt64 && val < math.MinInt32:
		binary.Write(expectedBuf, binary.BigEndian, uint16(9))
		expectedBuf.Write([]byte{Int64Marker})
		binary.Write(expectedBuf, binary.BigEndian, int64(val))
	case val >= math.MinInt32 && val < math.MinInt16:
		binary.Write(expectedBuf, binary.BigEndian, uint16(5))
		expectedBuf.Write([]byte{Int32Marker})
		binary.Write(expectedBuf, binary.BigEndian, int32(val))
	case val >= math.MinInt16 && val < math.MinInt8:
		binary.Write(expectedBuf, binary.BigEndian, uint16(3))
		expectedBuf.Write([]byte{Int16Marker})
		binary.Write(expectedBuf, binary.BigEndian, int16(val))
	case val >= math.MinInt8 && val < -16:
		binary.Write(expectedBuf, binary.BigEndian, uint16(2))
		expectedBuf.Write([]byte{Int8Marker})
		binary.Write(expectedBuf, binary.BigEndian, int8(val))
	case val >= -16 && val <= math.MaxInt8:
		binary.Write(expectedBuf, binary.BigEndian, uint16(1))
		binary.Write(expectedBuf, binary.BigEndian, int8(val))
	case val > math.MaxInt8 && val <= math.MaxInt16:
		binary.Write(expectedBuf, binary.BigEndian, uint16(3))
		expectedBuf.Write([]byte{Int16Marker})
		binary.Write(expectedBuf, binary.BigEndian, int16(val))
	case val > math.MaxInt16 && val <= math.MaxInt32:
		binary.Write(expectedBuf, binary.BigEndian, uint16(5))
		expectedBuf.Write([]byte{Int32Marker})
		binary.Write(expectedBuf, binary.BigEndian, int32(val))
	case val > math.MaxInt32 && val <= math.MaxInt64:
		binary.Write(expectedBuf, binary.BigEndian, uint16(9))
		expectedBuf.Write([]byte{Int64Marker})
		binary.Write(expectedBuf, binary.BigEndian, int64(val))
	default:
		return nil, errors.New("Int too long to write: %d", val)
	}
	expectedBuf.Write(EndMessage)

	expectedCount, _ := expectedBuf.Read(expected)

	return expected[:expectedCount], nil
}

func generateIntResultBuf(val interface{}) ([]byte, error) {
	encoder, buf := createNewTestEncoder()
	err := encoder.Encode(val)

	if err != nil {
		return nil, errors.New("Error while encoding: %v", err)
	}

	output := make([]byte, maxBufSize)
	outputCount, err := buf.Read(output)

	if err != nil {
		return nil, errors.New("Error while reading output: %v", err)
	}

	return output[:outputCount], nil
}

func TestEncodeInt(t *testing.T) {
	expected := func(val int) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val int) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeUint(t *testing.T) {
	expected := func(val uint) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val uint) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeInt8(t *testing.T) {
	expected := func(val int8) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val int8) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeUint8(t *testing.T) {
	expected := func(val uint8) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val uint8) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeInt16(t *testing.T) {
	expected := func(val int16) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val int16) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeUint16(t *testing.T) {
	expected := func(val uint16) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val uint16) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeInt32(t *testing.T) {
	expected := func(val int32) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val int32) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeUint32(t *testing.T) {
	expected := func(val uint32) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val uint32) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeInt64(t *testing.T) {
	expected := func(val int64) []byte {
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val int64) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeUint64(t *testing.T) {
	expected := func(val uint64) []byte {
		if val > math.MaxInt64 {
			return nil
		}
		output, err := generateIntExpectedBuf(int64(val))

		if err != nil {
			t.Fatal(err)
		}

		return output
	}
	result := func(val uint64) []byte {
		output, err := generateIntResultBuf(val)

		if err != nil && val <= math.MaxInt64 {
			t.Fatal(err)
		}

		return output
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeFloat32(t *testing.T) {
	expected := func(val float32) []byte {
		expectedBuf := bytes.NewBuffer([]byte{})
		expected := make([]byte, maxBufSize)

		binary.Write(expectedBuf, binary.BigEndian, uint16(9))
		expectedBuf.Write([]byte{FloatMarker})
		binary.Write(expectedBuf, binary.BigEndian, float64(val))
		expectedBuf.Write(EndMessage)

		expectedCount, _ := expectedBuf.Read(expected)

		return expected[:expectedCount]
	}
	result := func(val float32) []byte {
		encoder, buf := createNewTestEncoder()
		err := encoder.Encode(val)

		if err != nil {
			t.Fatalf("Error while encoding: %v", err)
		}

		output := make([]byte, maxBufSize)
		outputCount, err := buf.Read(output)

		if err != nil {
			t.Fatalf("Error while reading output: %v", err)
		}

		return output[:outputCount]
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeFloat64(t *testing.T) {
	expected := func(val float64) []byte {
		expectedBuf := bytes.NewBuffer([]byte{})
		expected := make([]byte, maxBufSize)

		binary.Write(expectedBuf, binary.BigEndian, uint16(9))
		expectedBuf.Write([]byte{FloatMarker})
		binary.Write(expectedBuf, binary.BigEndian, float64(val))
		expectedBuf.Write(EndMessage)

		expectedCount, _ := expectedBuf.Read(expected)

		return expected[:expectedCount]
	}
	result := func(val float64) []byte {
		encoder, buf := createNewTestEncoder()
		err := encoder.Encode(val)

		if err != nil {
			t.Fatalf("Error while encoding: %v", err)
		}

		output := make([]byte, maxBufSize)
		outputCount, err := buf.Read(output)

		if err != nil {
			t.Fatalf("Error while reading output: %v", err)
		}

		return output[:outputCount]
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeString(t *testing.T) {
	expected := func(val string) []byte {
		expectedBuf := bytes.NewBuffer([]byte{})
		resultExpectedBuf := bytes.NewBuffer([]byte{})
		expected := make([]byte, maxBufSize)

		bytes := []byte(val)

		length := len(bytes)

		switch {
		case length <= 15:
			expectedBuf.Write([]byte{byte(TinyStringMarker + length)})
			expectedBuf.Write(bytes)
		case length > 15 && length <= math.MaxUint8:
			expectedBuf.Write([]byte{String8Marker})
			binary.Write(expectedBuf, binary.BigEndian, int8(length))
			expectedBuf.Write(bytes)
		case length > math.MaxUint8 && length <= math.MaxUint16:
			expectedBuf.Write([]byte{String16Marker})
			binary.Write(expectedBuf, binary.BigEndian, int16(length))
			expectedBuf.Write(bytes)
		case length > math.MaxUint16 && int64(length) <= math.MaxUint32:
			expectedBuf.Write([]byte{String32Marker})
			binary.Write(expectedBuf, binary.BigEndian, int32(length))
			expectedBuf.Write(bytes)
		default:
			t.Fatalf("String too long to write: %s", val)
		}

		binary.Write(resultExpectedBuf, binary.BigEndian, uint16(expectedBuf.Len()))
		resultExpectedBuf.ReadFrom(expectedBuf)
		resultExpectedBuf.Write(EndMessage)

		expectedCount, _ := resultExpectedBuf.Read(expected)

		return expected[:expectedCount]
	}

	result := func(val string) []byte {
		encoder, buf := createNewTestEncoder()
		err := encoder.Encode(val)

		if err != nil {
			t.Fatalf("Error while encoding: %v", err)
		}

		output := make([]byte, maxBufSize)
		outputCount, err := buf.Read(output)

		if err != nil {
			t.Fatalf("Error while reading output: %v", err)
		}

		return output[:outputCount]
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeInterfaceSlice(t *testing.T) {
	expected := func(val []bool) []byte {
		expectedBuf := bytes.NewBuffer([]byte{})
		resultExpectedBuf := bytes.NewBuffer([]byte{})
		expected := make([]byte, maxBufSize)
		length := len(val)

		switch {
		case length <= 15:
			expectedBuf.Write([]byte{byte(TinySliceMarker + length)})
		case length > 15 && length <= math.MaxUint8:
			expectedBuf.Write([]byte{Slice8Marker})
			binary.Write(expectedBuf, binary.BigEndian, int8(length))
		case length > math.MaxUint8 && length <= math.MaxUint16:
			expectedBuf.Write([]byte{Slice16Marker})
			binary.Write(expectedBuf, binary.BigEndian, int16(length))
		case length >= math.MaxUint16 && int64(length) <= math.MaxUint32:
			expectedBuf.Write([]byte{Slice32Marker})
			binary.Write(expectedBuf, binary.BigEndian, int32(length))
		default:
			t.Fatalf("Slice too long to write: %+v", val)
		}

		var marker byte

		for _, item := range val {
			if item == true {
				marker = TrueMarker
			} else {
				marker = FalseMarker
			}

			expectedBuf.Write([]byte{marker})
		}

		binary.Write(resultExpectedBuf, binary.BigEndian, uint16(expectedBuf.Len()))
		resultExpectedBuf.ReadFrom(expectedBuf)
		resultExpectedBuf.Write(EndMessage)

		expectedCount, _ := resultExpectedBuf.Read(expected)

		return expected[:expectedCount]
	}

	result := func(val []bool) []byte {
		encoder, buf := createNewTestEncoder()
		err := encoder.Encode(val)

		if err != nil {
			t.Fatalf("Error while encoding: %v", err)
		}

		output := make([]byte, maxBufSize)
		outputCount, err := buf.Read(output)

		if err != nil {
			t.Fatalf("Error while reading output: %v", err)
		}

		return output[:outputCount]
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeStringSlice(t *testing.T) {
	expected := func(val []string) []byte {
		expectedBuf := bytes.NewBuffer([]byte{})
		resultExpectedBuf := bytes.NewBuffer([]byte{})
		expected := make([]byte, maxBufSize)
		length := len(val)

		switch {
		case length <= 15:
			expectedBuf.Write([]byte{byte(TinySliceMarker + length)})
		case length > 15 && length <= math.MaxUint8:
			expectedBuf.Write([]byte{Slice8Marker})
			binary.Write(expectedBuf, binary.BigEndian, int8(length))
		case length > math.MaxUint8 && length <= math.MaxUint16:
			expectedBuf.Write([]byte{Slice16Marker})
			binary.Write(expectedBuf, binary.BigEndian, int16(length))
		case length >= math.MaxUint16 && int64(length) <= math.MaxUint32:
			expectedBuf.Write([]byte{Slice32Marker})
			binary.Write(expectedBuf, binary.BigEndian, int32(length))
		default:
			t.Fatalf("Slice too long to write: %+v", val)
		}

		for _, item := range val {
			bytes := []byte(item)

			length := len(bytes)

			switch {
			case length <= 15:
				expectedBuf.Write([]byte{byte(TinyStringMarker + length)})
				expectedBuf.Write(bytes)
			case length > 15 && length <= math.MaxUint8:
				expectedBuf.Write([]byte{String8Marker})
				binary.Write(expectedBuf, binary.BigEndian, int8(length))
				expectedBuf.Write(bytes)
			case length > math.MaxUint8 && length <= math.MaxUint16:
				expectedBuf.Write([]byte{String16Marker})
				binary.Write(expectedBuf, binary.BigEndian, int16(length))
				expectedBuf.Write(bytes)
			case length > math.MaxUint16 && int64(length) <= math.MaxUint32:
				expectedBuf.Write([]byte{String32Marker})
				binary.Write(expectedBuf, binary.BigEndian, int32(length))
				expectedBuf.Write(bytes)
			default:
				t.Fatalf("String too long to write: %s", val)
			}
		}

		binary.Write(resultExpectedBuf, binary.BigEndian, uint16(expectedBuf.Len()))
		resultExpectedBuf.ReadFrom(expectedBuf)
		resultExpectedBuf.Write(EndMessage)

		expectedCount, _ := resultExpectedBuf.Read(expected)

		return expected[:expectedCount]
	}

	result := func(val []string) []byte {
		encoder, buf := createNewTestEncoder()
		err := encoder.Encode(val)

		if err != nil {
			t.Fatalf("Error while encoding: %v", err)
		}

		output := make([]byte, maxBufSize)
		outputCount, err := buf.Read(output)

		if err != nil {
			t.Fatalf("Error while reading output: %v", err)
		}

		return output[:outputCount]
	}

	if err := quick.CheckEqual(expected, result, nil); err != nil {
		t.Fatal(err)
	}
}
