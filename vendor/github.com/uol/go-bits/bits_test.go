package bits

import (
	"testing"
	"testing/quick"
)

func testQuick(t *testing.T, which string, ffast, fslow func(x uint64) uint64) {
	f := func(x uint32) bool {
		return ffast(uint64(x)) == fslow(uint64(x))
	}
	if err := quick.Check(f, nil); err != nil {
		t.Errorf("fast%v != slow%v: %v: ", which, which, err)
	}
}

func ctzSlow(x uint64) uint64 {
	var n uint64
	for x&1 == 0 {
		n++
		x >>= 1
	}
	return uint64(n)
}

func TestQuickCtz(t *testing.T) { testQuick(t, "ctz", Ctz32, ctzSlow) }

func clzSlow(x uint64) uint64 {
	var n uint64
	for x&0x80000000 == 0 {
		n++
		x <<= 1
	}
	return uint64(n)
}

func TestQuickClz(t *testing.T) { testQuick(t, "clz", Clz32, clzSlow) }
