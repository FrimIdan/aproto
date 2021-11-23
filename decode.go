package aproto

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
)

// gRPC has some additional header - remove it
func skipGrpcHeaderIfNeeded(data []byte) []byte {
	pos := 0

	if (data[pos] != 0) {
		// no header
		return data
	}

	pos++;
	length := binary.BigEndian.Uint32(data[pos : pos+4]);
	pos += 4;
	if length > uint32(len(data[pos:])) {
		// something is wrong, return original data
		return data
	}

	return data[pos:]
}

func decodeChunk(data []byte) (chk Chunk, chunkLen uint64, e error) {
	pos := 0

	// Each key in the streamed message is a varint with the value (field_number << 3) | wire_type
	// in other words, the last three bits of the number store the wire type.
	keyType, keyTypeLen := proto.DecodeVarint(data)
	if keyTypeLen <= 0 || keyTypeLen > 16 {
		e = errors.New("malformed keyTypeLen")
		return
	}
	pos += keyTypeLen

	if pos >= len(data) {
		e = errors.New("not enough data for any furter wire type")
		return
	}

	fieldNumber := int(keyType >> 3)
	if fieldNumber > 536870911 { // max field: 2^29 - 1 == 536870911
		e = errors.New("field number > 2^29-1")
		return
	}

	wireType := keyType & 7

	keyTypeBytes := data[0:keyTypeLen]

	chunkLen += uint64(keyTypeLen)

	switch wireType {
	case proto.WireVarint: // int32, int64, uint32, uint64, sint32, sint64, bool, enum
		// overflow, not enough data
		if pos+1 > len(data) {
			e = errors.New("not enough data for wire type 0(varint)")
			return
		}

		u64, u64Len := proto.DecodeVarint(data[pos:])
		if u64 == 0 && u64Len == 0 {
			e = errors.New("fail DecodeVarint()")
			return
		}
		chk = &Varint{
			Value: u64,
			IdType: IdType{
				Id:   fieldNumber,
				Type: wireType,
				data: keyTypeBytes,
			},
		}

		chunkLen += uint64(u64Len)

	case proto.WireFixed64: // fixed64, sfixed64, double
		// overflow, not enough data
		if pos+8 > len(data) {
			// fmt.Println("pos: ", pos, ", len(data): ", len(data), ", sLen: ", int(sLen))
			e = errors.New("not enough data for wire type 1(fixed64)")
			return
		}
		u64 := binary.LittleEndian.Uint64(data[pos : pos+8])
		chk = &Fixed64{
			Value: u64,
			IdType: IdType{
				Id:   fieldNumber,
				Type: wireType,
				data: keyTypeBytes,
			},
		}

		chunkLen += 8

	case proto.WireStartGroup, proto.WireEndGroup, proto.WireBytes: // string, bytes, embedded messages, packed repeated fields
		sLen, sLenLen := proto.DecodeVarint(data[pos:])
		if sLen == 0 && sLenLen == 0 {
			e = errors.New("fail DecodeVarint()")
			return
		}
		chunkLen += uint64(sLenLen)
		pos += sLenLen

		// overflow, not enough data
		if uint64(pos)+sLen > uint64(len(data)) {
			e = errors.New("not enough data for wire type 2(string)")
			return
		}

		str := data[pos : pos+int(sLen)]

		_struct := &Struct{
			DataLen: len(str),
			IdType: IdType{
				Id:   fieldNumber,
				Type: wireType,
				data: keyTypeBytes,
			},
		}

		// try to decode as inner struct first
		chunks, err2 := DecodeAllChunks(str)

		// if decode success, treat as struct
		if err2 == nil {
			_struct.Children = chunks
			_struct.Str = str
			chk = _struct

			chunkLen += sLen
			return
		} else { // decode fail, just treat as string
			_struct.Str = str
			chk = _struct

			chunkLen += sLen
		}

	case proto.WireFixed32: // fixed32, sfixed32, float
		if pos+4 > len(data) {
			e = errors.New("not enough data for wire type 5(fixed32)")
			return
		}
		u32 := binary.LittleEndian.Uint32(data[pos : pos+4])

		chk = &Fixed32{
			value: u32,
			IdType: IdType{
				Id:   fieldNumber,
				Type: wireType,
				data: keyTypeBytes,
			},
		}

		chunkLen += 4

	//case : // deprecated
	//	return nil, chunkLen, nil

	default:
		e = errors.New(fmt.Sprintf("Unknown wire type %d of keyType %x", wireType, keyType))
		return
	}
	return
}
func DecodeAllChunks(data []byte) ([]Chunk, error) {
	var pos uint64 = 0
	var ret []Chunk

	data = skipGrpcHeaderIfNeeded(data)

	for pos < uint64(len(data)) {
		chunk, chunkLen, e := decodeChunk(data[pos:])
		if e != nil {
			return ret, e
		}

		if chunk != nil {
			ret = append(ret, chunk)
		}
		pos += chunkLen
	}

	return ret, nil
}

// dump with all kinds of Renderer
func TryDumpEx(data []byte, r Renderer) (string, error) {
	chunks, e := DecodeAllChunks(data)
	if e != nil {
		return ``, e
	}
	ret := ``

	for _, ch := range chunks {
		ret += ch.Render(``, r)
		ret += r.NEWLINE()
	}
	return ret, nil
}

// dump to Console
func TryDump(data []byte) (string, error) {
	return TryDumpEx(data, &ConsoleRenderer{})
}

// dump to Console and ignore error
func Dump(data []byte) string {
	ret, e := TryDump(data)
	if e != nil {
		return ""
	}
	return ret
}
