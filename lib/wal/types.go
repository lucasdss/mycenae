package wal

import (
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"time"
)

const (
	// BlockFloat64 designates a block encodes float64 values.
	BlockFloat64 = byte(0)

	// BlockInteger designates a block encodes int64 values.
	BlockInteger = byte(1)

	// BlockBoolean designates a block encodes boolean values.
	BlockBoolean = byte(2)

	// BlockString designates a block encodes string values.
	BlockString = byte(3)

	// BlockUnsigned designates a block encodes uint64 values.
	BlockUnsigned = byte(4)

	// encodedBlockHeaderSize is the size of the header for an encoded block.  There is one
	// byte encoding the type of the block.
	encodedBlockHeaderSize = 1

	// ZeroTime is the Unix nanosecond timestamp for no time.
	// This time is not used by the query engine or the storage engine as a valid time.
	ZeroTime = int64(math.MinInt64)

	// DefaultMaxPointsPerBlock is the maximum number of points in an encoded
	// block in a TSM file
	DefaultMaxPointsPerBlock = 1000
)

// Statistic is the representation of a statistic used by the monitoring service.
type Statistic struct {
	Name   string                 `json:"name"`
	Tags   map[string]string      `json:"tags"`
	Values map[string]interface{} `json:"values"`
}

func init() {
	// Prime the pools with one encoder/decoder for each available CPU.
	vals := make([]interface{}, 0, runtime.NumCPU())
	for _, p := range []*Generic{} {
		vals = vals[:0]
		// Check one out to force the allocation now and hold onto it
		for i := 0; i < runtime.NumCPU(); i++ {
			v := p.Get(DefaultMaxPointsPerBlock)
			vals = append(vals, v)
		}
		// Add them all back
		for _, v := range vals {
			p.Put(v)
		}
	}
}

// Value represents a TSM-encoded value.
type Value interface {
	// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
	UnixNano() int64

	// Value returns the underlying value.
	Value() interface{}

	// Size returns the number of bytes necessary to represent the value and its timestamp.
	Size() int

	// String returns the string representation of the value and its timestamp.
	String() string

	// internalOnly is unexported to ensure implementations of Value
	// can only originate in this package.
	internalOnly()
}

// Values represents a slice of  values.
type Values []Value

// NewValue returns a new Value with the underlying type dependent on value.
func NewValue(t int64, value interface{}) Value {
	switch v := value.(type) {
	case int64:
		return IntegerValue{unixnano: t, value: v}
	case uint64:
		return UnsignedValue{unixnano: t, value: v}
	case float64:
		return FloatValue{unixnano: t, value: v}
	case int:
		return FloatValue{unixnano: t, value: float64(v)}
	case int32:
		return FloatValue{unixnano: t, value: float64(v)}
	case float32:
		return FloatValue{unixnano: t, value: float64(v)}
	case uint32:
		return FloatValue{unixnano: t, value: float64(v)}
	case bool:
		return BooleanValue{unixnano: t, value: v}
	case string:
		return StringValue{unixnano: t, value: v}
	}
	return EmptyValue{}
}

// NewIntegerValue returns a new integer value.
func NewIntegerValue(t int64, v int64) Value {
	return IntegerValue{unixnano: t, value: v}
}

// NewUnsignedValue returns a new unsigned integer value.
func NewUnsignedValue(t int64, v uint64) Value {
	return UnsignedValue{unixnano: t, value: v}
}

// NewFloatValue returns a new float value.
func NewFloatValue(t int64, v float64) Value {
	return FloatValue{unixnano: t, value: v}
}

// NewBooleanValue returns a new boolean value.
func NewBooleanValue(t int64, v bool) Value {
	return BooleanValue{unixnano: t, value: v}
}

// NewStringValue returns a new string value.
func NewStringValue(t int64, v string) Value {
	return StringValue{unixnano: t, value: v}
}

// EmptyValue is used when there is no appropriate other value.
type EmptyValue struct{}

// UnixNano returns ZeroTime
func (e EmptyValue) UnixNano() int64 { return ZeroTime }

// Value returns nil.
func (e EmptyValue) Value() interface{} { return nil }

// Size returns 0.
func (e EmptyValue) Size() int { return 0 }

// String returns the empty string.
func (e EmptyValue) String() string { return "" }

func (_ EmptyValue) internalOnly()    {}
func (_ StringValue) internalOnly()   {}
func (_ IntegerValue) internalOnly()  {}
func (_ UnsignedValue) internalOnly() {}
func (_ BooleanValue) internalOnly()  {}
func (_ FloatValue) internalOnly()    {}

// Encode converts the values to a byte slice.  If there are no values,
// this function panics.
func (a Values) Encode(buf []byte) ([]byte, error) {
	if len(a) == 0 {
		panic("unable to encode block type")
	}

	switch a[0].(type) {
	case FloatValue:
		return encodeFloatBlock(buf, a)
	case IntegerValue:
		return encodeIntegerBlock(buf, a)
	case UnsignedValue:
		return encodeUnsignedBlock(buf, a)
	case BooleanValue:
		return encodeBooleanBlock(buf, a)
	case StringValue:
		return encodeStringBlock(buf, a)
	}

	return nil, fmt.Errorf("unsupported value type %T", a[0])
}

// BlockType returns the type of value encoded in a block or an error
// if the block type is unknown.
func BlockType(block []byte) (byte, error) {
	blockType := block[0]
	switch blockType {
	case BlockFloat64, BlockInteger, BlockUnsigned, BlockBoolean, BlockString:
		return blockType, nil
	default:
		return 0, fmt.Errorf("unknown block type: %d", blockType)
	}
}

// BlockCount returns the number of timestamps encoded in block.
func BlockCount(block []byte) int {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("count of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}
	// first byte is the block type
	_, _, err := unpackBlock(block[1:])
	if err != nil {
		panic(fmt.Sprintf("BlockCount: error unpacking block: %s", err.Error()))
	}
	return 1000
}

// DecodeBlock takes a byte slice and decodes it into values of the appropriate type
// based on the block.
func DecodeBlock(block []byte, vals []Value) ([]Value, error) {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("decode of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}

	blockType, err := BlockType(block)
	if err != nil {
		return nil, err
	}

	switch blockType {
	case BlockFloat64:
		var buf []FloatValue
		decoded, err := DecodeFloatBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err
	case BlockInteger:
		var buf []IntegerValue
		decoded, err := DecodeIntegerBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockUnsigned:
		var buf []UnsignedValue
		decoded, err := DecodeUnsignedBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockBoolean:
		var buf []BooleanValue
		decoded, err := DecodeBooleanBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockString:
		var buf []StringValue
		decoded, err := DecodeStringBlock(block, &buf)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = decoded[i]
		}
		return vals[:len(decoded)], err

	default:
		panic(fmt.Sprintf("unknown block type: %d", blockType))
	}
}

// FloatValue represents a float64 value.
type FloatValue struct {
	unixnano int64
	value    float64
}

// UnixNano returns the timestamp of the value.
func (v FloatValue) UnixNano() int64 {
	return v.unixnano
}

// Value returns the underlying float64 value.
func (v FloatValue) Value() interface{} {
	return v.value
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v FloatValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (v FloatValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.value)
}

func encodeFloatBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	var b []byte

	return b, nil
}

// DecodeFloatBlock decodes the float block from the byte slice
// and appends the float values to a.
func DecodeFloatBlock(block []byte, a *[]FloatValue) ([]FloatValue, error) {

	return []FloatValue{}, nil
}

// BooleanValue represents a boolean value.
type BooleanValue struct {
	unixnano int64
	value    bool
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v BooleanValue) Size() int {
	return 9
}

// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
func (v BooleanValue) UnixNano() int64 {
	return v.unixnano
}

// Value returns the underlying boolean value.
func (v BooleanValue) Value() interface{} {
	return v.value
}

// String returns the string representation of the value and its timestamp.
func (v BooleanValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func encodeBooleanBlock(buf []byte, values []Value) ([]byte, error) {

	return nil, nil
}

// DecodeBooleanBlock decodes the boolean block from the byte slice
// and appends the boolean values to a.
func DecodeBooleanBlock(block []byte, a *[]BooleanValue) ([]BooleanValue, error) {
	return nil, nil
}

// IntegerValue represents an int64 value.
type IntegerValue struct {
	unixnano int64
	value    int64
}

// Value returns the underlying int64 value.
func (v IntegerValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v IntegerValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v IntegerValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (v IntegerValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func encodeIntegerBlock(buf []byte, values []Value) ([]byte, error) {

	return nil, nil
}

// DecodeIntegerBlock decodes the integer block from the byte slice
// and appends the integer values to a.
func DecodeIntegerBlock(block []byte, a *[]IntegerValue) ([]IntegerValue, error) {
	return nil, nil
}

// UnsignedValue represents an int64 value.
type UnsignedValue struct {
	unixnano int64
	value    uint64
}

// Value returns the underlying int64 value.
func (v UnsignedValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v UnsignedValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v UnsignedValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (v UnsignedValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func encodeUnsignedBlock(buf []byte, values []Value) ([]byte, error) {

	return nil, nil
}

// DecodeUnsignedBlock decodes the unsigned integer block from the byte slice
// and appends the unsigned integer values to a.
func DecodeUnsignedBlock(block []byte, a *[]UnsignedValue) ([]UnsignedValue, error) {
	return nil, nil
}

// StringValue represents a string value.
type StringValue struct {
	unixnano int64
	value    string
}

// Value returns the underlying string value.
func (v StringValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v StringValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v StringValue) Size() int {
	return 8 + len(v.value)
}

// String returns the string representation of the value and its timestamp.
func (v StringValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func encodeStringBlock(buf []byte, values []Value) ([]byte, error) {

	return nil, nil
}

// DecodeStringBlock decodes the string block from the byte slice
// and appends the string values to a.
func DecodeStringBlock(block []byte, a *[]StringValue) ([]StringValue, error) {
	return nil, nil
}

func packBlock(buf []byte, typ byte, ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	sz := 1 + binary.MaxVarintLen64 + len(ts) + len(values)
	if cap(buf) < sz {
		buf = make([]byte, sz)
	}
	b := buf[:sz]
	b[0] = typ
	i := binary.PutUvarint(b[1:1+binary.MaxVarintLen64], uint64(len(ts)))
	i += 1

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	copy(b[i:], ts)
	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	copy(b[i+len(ts):], values)
	return b[:i+len(ts)+len(values)]
}

func unpackBlock(buf []byte) (ts, values []byte, err error) {
	// Unpack the timestamp block length
	tsLen, i := binary.Uvarint(buf)
	if i <= 0 {
		err = fmt.Errorf("unpackBlock: unable to read timestamp block length")
		return
	}

	// Unpack the timestamp bytes
	tsIdx := int(i) + int(tsLen)
	if tsIdx > len(buf) {
		err = fmt.Errorf("unpackBlock: not enough data for timestamp")
		return
	}
	ts = buf[int(i):tsIdx]

	// Unpack the value bytes
	values = buf[tsIdx:]
	return
}

// ZigZagEncode converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
func ZigZagEncode(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}
