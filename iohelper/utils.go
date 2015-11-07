package iohelper

import (
	"bytes"
	"encoding/binary"
	"io"
)

var (
	cachedItob [][]byte
)

func init() {
	cachedItob = make([][]byte, 1024)
	for i := 0; i < len(cachedItob); i++ {
		var b bytes.Buffer
		writeVLong(&b, int64(i))
		cachedItob[i] = b.Bytes()
	}
}

func itob(i int) []byte {
	if i > 0 && i < len(cachedItob) {
		return cachedItob[i]
	}

	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, i)
	return b.Bytes()
}

func decodeVIntSize(value byte) int32 {
	if int32(value) >= -112 {
		return int32(1)
	} else {
		if int32(value) < -120 {
			return -119 - int32(value)
		} else {
			return -111 - int32(value)
		}
	}
}

func isNegativeVInt(value byte) bool {
	return int32(value) < -120 || int32(value) >= -112 && int32(value) < 0
}

func readVLong(r io.Reader) int64 {
	var firstByte byte
	binary.Read(r, binary.BigEndian, &firstByte)
	l := decodeVIntSize(firstByte)
	if l == 1 {
		return int64(firstByte)
	} else {
		var i int64
		var idx int32
		for idx = 0; idx < l-1; idx++ {
			var b byte
			binary.Read(r, binary.BigEndian, &b)
			i <<= 8
			i |= int64(b & 255)
		}
		if isNegativeVInt(firstByte) {
			return ^i
		} else {
			return i
		}
	}
}

func writeVLong(w io.Writer, i int64) {
	if i >= -112 && i <= 127 {
		binary.Write(w, binary.BigEndian, byte(i))
	} else {
		var l int32 = -112
		if i < 0 {
			i = ^i
			l = -120
		}
		var tmp int64
		for tmp = i; tmp != 0; l-- {
			tmp >>= 8
		}

		binary.Write(w, binary.BigEndian, byte(l))
		if l < -120 {
			l = -(l + 120)
		} else {
			l = -(l + 112)
		}

		var idx int32
		for idx = l; idx != 0; idx-- {
			var mask int64
			shiftbits := uint((idx - 1) * 8)
			mask = int64(255) << shiftbits
			binary.Write(w, binary.BigEndian, byte((i&mask)>>shiftbits))
		}
	}
}

func ReadVarBytes(r ByteMultiReader) ([]byte, error) {
	sz := readVLong(r)
	b := make([]byte, sz)
	_, err := r.Read(b)
	if err != nil {
		return nil, err
	}
	return b, err
}

func WriteVarBytes(w io.Writer, b []byte) error {
	_, err := w.Write(itob(len(b)))
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func ReadInt32(r io.Reader) (int32, error) {
	var n int32
	err := binary.Read(r, binary.BigEndian, &n)
	return n, err
}

func ReadN(r io.Reader, n int32) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

func ReadUint64(r io.Reader) (uint64, error) {
	var n uint64
	err := binary.Read(r, binary.BigEndian, &n)
	return n, err
}
