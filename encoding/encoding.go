package messages

import (
	"encoding/binary"
	"io"
)

type Encoder struct {
	io.Writer
}

func NewEncoder(w io.Writer) Encoder {
	return Encoder{Writer: w}
}

func (e Encoder) Encode(iVal interface{}) error {
	// TODO: How to handle pointers?
	//if reflect.TypeOf(iVal) == reflect.Ptr {
	//	return Encode(*iVal)
	//}

	var err error
	switch val := iVal.(type) {
	case nil:
		err = e.encodeNil()
	case bool:
		err = e.encodeBool(val)
	case int:
		err = e.encodeInt(int64(val))
	case int8:
		err = e.encodeInt(int64(val))
	case int16:
		err = e.encodeInt(int64(val))
	case int32:
		err = e.encodeInt(int64(val))
	case int64:
		err = e.encodeInt(int64(val))
	case uint:
		err = e.encodeInt(int64(val))
	case uint8:
		err = e.encodeInt(int64(val))
	case uint16:
		err = e.encodeInt(int64(val))
	case uint32:
		err = e.encodeInt(int64(val))
	case uint64:
		// TODO: Bolt docs only mention going up to int64, not uint64
		// Must confirm this, because this would truncate
		// Also, should this fail instead of truncate? - probably
		err = e.encodeInt(int64(val))
		//case float32:
		//	err = e.encodeFloat32(val)
		//case float64:
		//	err = e.encodeFloat64(val)
		//case string:
		//	err = e.encodeString(val)
		//case rune:
		//	err = e.encodeRune(val)
		//case byte:
		//	err = e.encodeRune(val)
	}

	return err
}

func (e Encoder) encodeNil() error {
	_, err := e.Write([]byte{0xC0})
	return err
}

func (e Encoder) encodeBool(val bool) error {
	var err error
	if val {
		_, err = e.Write([]byte{0xC3})
	} else {
		_, err = e.Write([]byte{0xC2})
	}
	return err
}

func (e Encoder) encodeInt(val int64) error {
	var err error
	switch val {
	case val >= -9223372036854775808 && val <= -2147483649:
		// Write as INT_64
		if _, err = e.Write([]byte{0xCB}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= -2147483648 && val <= -32769:
		// Write as INT_32
		if _, err = e.Write([]byte{0xCA}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= -32768 && val <= -129:
		// Write as INT_16
		if _, err = e.Write([]byte{0xC9}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= -128 && val <= -17:
		// Write as INT_8
		if _, err = e.Write([]byte{0xC8}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= -16 && val <= 127:
		// Write as TINY_INT
		err = binary.Write(e, binary.BigEndian, val)
	case val >= 128 && val <= 32767:
		// Write as INT_16
		if _, err = e.Write([]byte{0xC9}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= 32768 && val <= 2147483647:
		// Write as INT_32
		if _, err = e.Write([]byte{0xCA}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= 2147483648 && val <= 9223372036854775807:
		// Write as INT_64
		if _, err = e.Write([]byte{0xCB}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	}
	return err
}

//func encodeMap(m map[string]interface{}) (*bytes.Buffer, error) {
//	var output []byte
//	length := len(m)
//	switch length {
//	case length <= 15:
//		output = []byte{0xA + length}
//	case length > 15 && length <= 255:
//		output = []byte{0xD8, length}
//	case length > 255 && length <= 65535:
//		output = []byte{0xD9, length}
//	case length > 65535 && length <= 4294967295:
//		output = []byte{0xDA, length}
//	default:
//		return nil, fmt.Errorf("Map length too long. Max: 4294967295 Got: %d", length)
//	}
//
//	for k, v := range m {
//		encodedKey, err := Encode(k)
//		if err != nil {
//			return nil, err
//		}
//		encodedValue, err := Encode(v)
//		if err != nil {
//			return nil, err
//		}
//		output = append(output, encodedKey...)
//		output = append(output, encodedValue...)
//	}
//
//	return output
//}
