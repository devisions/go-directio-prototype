package data

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

type SomeData struct {
	Text   string
	Number uint64
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
