package bits

// Clz counts leading zeroes
func Clz32(x uint64) uint64 {
	var n uint64

	n = 1

	if (x >> 16) == 0 {
		n = n + 16
		x = x << 16
	}

	if (x >> (16 + 8)) == 0 {
		n = n + 8
		x = x << 8
	}

	if (x >> (16 + 8 + 4)) == 0 {
		n = n + 4
		x = x << 4
	}

	if (x >> (16 + 8 + 4 + 2)) == 0 {
		n = n + 2
		x = x << 2
	}

	n = n - (x >> 31)
	return uint64(n)
}
