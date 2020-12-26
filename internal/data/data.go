package data

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

type SomeData struct {
	Value string
}

func (d *SomeData) Encode() []byte {
	buf := bytes.Buffer{}
	_ = gob.NewEncoder(&buf).Encode(*d)
	return buf.Bytes()
}

func (d *SomeData) EncodeTo(to []byte) error {
	buf := bytes.Buffer{}
	_ = gob.NewEncoder(&buf).Encode(*d)
	if len(to) < buf.Len() {
		return errors.New("cannot copy encoded into a smaller buffer")
	}
	copy(to, buf.Bytes())
	return nil
}

func Decode(from []byte) (*SomeData, error) {
	d := &SomeData{}
	dec := gob.NewDecoder(bytes.NewReader(from))
	err := dec.Decode(d)
	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "decoding data")
	}
	return d, nil
}

func I64toBytes(val uint64) []byte {
	r := make([]byte, 8)
	for i := uint64(0); i < 8; i++ {
		r[i] = byte((val >> (i * 8)) & 0xff)
	}
	return r
}

func Bytestoi64(val []byte) uint64 {
	r := uint64(0)
	for i := uint64(0); i < 8; i++ {
		r |= uint64(val[i]) << (8 * i)
	}
	return r
}
