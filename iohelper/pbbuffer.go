package iohelper

import (
	"encoding/binary"

	pb "github.com/golang/protobuf/proto"
)

type PbBuffer struct {
	b []byte
}

func NewPbBuffer() *PbBuffer {
	b := []byte{}
	return &PbBuffer{
		b: b,
	}
}

func (b *PbBuffer) Bytes() []byte {
	return b.b
}

func (b *PbBuffer) Write(d []byte) (int, error) {
	b.b = append(b.b, d...)
	return len(d), nil
}

func (b *PbBuffer) WriteByte(d byte) error {
	return binary.Write(b, binary.BigEndian, d)
}

func (b *PbBuffer) WriteString(d string) error {
	return binary.Write(b, binary.BigEndian, d)
}

func (b *PbBuffer) WriteInt32(d int32) error {
	return binary.Write(b, binary.BigEndian, d)
}

func (b *PbBuffer) WriteInt64(d int64) error {
	return binary.Write(b, binary.BigEndian, d)
}

func (b *PbBuffer) WriteFloat32(d float32) error {
	return binary.Write(b, binary.BigEndian, d)
}

func (b *PbBuffer) WriteFloat64(d float64) error {
	return binary.Write(b, binary.BigEndian, d)
}

func (b *PbBuffer) WriteVarint32(n int32) error {
	for true {
		if (n & 0x7F) == 0 {
			b.WriteByte(byte(n))
			return nil
		} else {
			b.WriteByte(byte((n & 0x7F) | 0x80))
			n >>= 7
		}
	}

	return nil
}

func (b *PbBuffer) WritePBMessage(d pb.Message) error {
	buf, err := pb.Marshal(d)
	if err != nil {
		return err
	}

	_, err = b.Write(buf)
	return err
}

func (b *PbBuffer) WriteDelimitedBuffers(bufs ...*PbBuffer) error {
	totalLength := 0
	lens := make([][]byte, len(bufs))
	for i, v := range bufs {
		n := len(v.Bytes())
		lenb := pb.EncodeVarint(uint64(n))

		totalLength += len(lenb) + n
		lens[i] = lenb
	}

	b.WriteInt32(int32(totalLength))

	for i, v := range bufs {
		b.Write(lens[i])
		b.Write(v.Bytes())
	}

	return nil
}

func (b *PbBuffer) PrependSize() error {
	size := int32(len(b.b))
	newBuf := NewPbBuffer()

	err := newBuf.WriteInt32(size)
	if err != nil {
		return err
	}

	_, err = newBuf.Write(b.b)
	if err != nil {
		return err
	}

	*b = *newBuf
	return nil
}
