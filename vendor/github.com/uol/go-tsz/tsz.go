// Package tsz implement time-series compression
/*

http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
with 32-bit floats and timestamps in seconds
*/
package tsz

import (
	"math"
	"sync"

	bits "github.com/uol/go-bits"
)

type Encoder struct {
	sync.Mutex

	t0    int64
	t     int64
	val32 float32

	bw       bstream
	leading  uint8
	trailing uint8
	finished bool

	tDelta int64
}

func NewEncoder(t0 int64) *Encoder {
	return &Encoder{
		t0:      t0,
		leading: ^uint8(0),
	}
}

func (s *Encoder) Encode(t int64, v float32) {

	s.Lock()
	defer s.Unlock()

	if s.t == 0 {
		// first point
		s.bw.writeBits(uint64(s.t0), 64)
		s.t = t
		s.val32 = v
		s.tDelta = t - s.t0

		s.bw.writeBits(uint64(s.tDelta), 14)

		s.bw.writeBits(uint64(math.Float32bits(v)), 32)
		return
	}

	tDelta := t - s.t

	dod := int64(tDelta - s.tDelta)

	switch {
	case dod == 0:
		s.bw.writeBit(zero)
	case -63 <= dod && dod <= 64:
		s.bw.writeBits(0x02, 2) // '10'
		s.bw.writeBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		s.bw.writeBits(0x06, 3) // '110'
		s.bw.writeBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2048:
		s.bw.writeBits(0x0e, 4) // '1110'
		s.bw.writeBits(uint64(dod), 12)
	default:
		s.bw.writeBits(0x0f, 4) // '1111'
		s.bw.writeBits(uint64(dod), 32)
	}

	vDelta := math.Float32bits(v) ^ math.Float32bits(s.val32)

	if vDelta == 0 {
		s.bw.writeBit(zero)
	} else {

		s.bw.writeBit(one)

		leading := uint8(bits.Clz32(uint64(vDelta)))
		trailing := uint8(bits.Ctz32(uint64(vDelta)))

		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 16 {
			leading = 15
		}

		// TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
		if s.leading != ^uint8(0) && leading >= s.leading && trailing >= s.trailing {

			s.bw.writeBit(zero)

			s.bw.writeBits(uint64(vDelta>>s.trailing), int(32-int(s.leading)-int(s.trailing)))

		} else {

			s.leading, s.trailing = leading, trailing

			s.bw.writeBit(one)

			s.bw.writeBits(uint64(leading), 4)

			// Note that if leading == trailing == 0, then sigbits == 32.  But that value doesn't actually fit into the 5 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 32 on unpacking.
			sigbits := 32 - leading - trailing
			s.bw.writeBits(uint64(sigbits), 5)

			s.bw.writeBits(uint64(vDelta>>trailing), int(sigbits))

		}
	}

	s.tDelta = tDelta
	s.t = t
	s.val32 = v

}

func (s *Encoder) Get() []byte {
	s.Lock()
	b := s.bw.clone()

	if !s.finished {
		b.writeBits(0x0f, 4)
		b.writeBits(0xffffffff, 32)
		b.writeBit(zero)
	}

	s.Unlock()

	return b.bytes()
}

func (s *Encoder) Close() ([]byte, error) {
	s.Lock()
	if !s.finished {
		s.bw.writeBits(0x0f, 4)

		s.bw.writeBits(0xffffffff, 32)

		s.bw.writeBit(zero)

		s.finished = true
	}

	s.Unlock()
	return s.bw.bytes(), nil
}

type Decoder struct {
	T0 int64

	t   int64
	val float32

	br       bstream
	leading  uint8
	trailing uint8

	finished bool

	tDelta int64
	err    error
}

func NewDecoder(b []byte) *Decoder {

	stream := make([]byte, len(b))

	copy(stream, b)

	br := bstream{
		count:  8,
		stream: stream,
	}

	return &Decoder{
		br: br,
	}
}

func (it *Decoder) Scan(t *int64, f *float32) bool {

	if it.err != nil || it.finished {
		return false
	}

	if it.t == 0 {
		t0, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.T0 = int64(t0)
		// read first t and v
		tDelta, err := it.br.readBits(14)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = int64(tDelta)
		it.t = it.T0 + it.tDelta
		v, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return false
		}

		it.val = math.Float32frombits(uint32(v))

		*t = it.t
		*f = it.val

		return true
	}

	// read delta-of-delta
	var d byte
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return false
		}
		if !bit {
			break
		}
		d |= 1
	}

	var dod int64
	var sz uint
	switch d {
	case 0x00:
		// dod == 0
	case 0x02:
		sz = 7
	case 0x06:
		sz = 9
	case 0x0e:
		sz = 12
	case 0x0f:
		sz = 32
	}

	if sz != 0 {
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return false
		}
		if bits == 0xffffffff {
			it.finished = true
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int64(bits)
	}

	tDelta := it.tDelta + dod

	it.tDelta = tDelta
	it.t = it.t + it.tDelta

	// read compressed value
	bit, err := it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if !bit {
		// it.val = it.val
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if !bit {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.readBits(4)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = uint8(bits)

			bits, err = it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 32; see comment in encoder
			if mbits == 0 {
				mbits = 32
			}
			it.trailing = 32 - it.leading - mbits
		}

		mbits := int(32 - it.leading - it.trailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := uint64(math.Float32bits(it.val))
		vbits ^= (bits << it.trailing)
		it.val = math.Float32frombits(uint32(vbits))
	}

	*t = it.t
	*f = it.val

	return true
}

// Err error at the current iterator position
func (it *Decoder) Close() error {
	return it.err
}
