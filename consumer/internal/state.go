package internal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/devisions/go-playground/go-directio/internal/data"
	"github.com/ncw/directio"
	"github.com/pkg/errors"
)

const STATE_FILE = "consumer.state"

type ConsumerState struct {
	ReadFilepath       string
	ReadBytes          int64
	saveStateFilepath  string
	saveStateBlocksize int
}

func (s *ConsumerState) encode(to []byte) error {
	buf := bytes.Buffer{}
	_ = gob.NewEncoder(&buf).Encode(*s)
	if len(to) < buf.Len() {
		return errors.New("cannot copy encoded into a smaller buffer")
	}
	copy(to, buf.Bytes())
	return nil
}

func decode(from []byte) (*ConsumerState, error) {
	s := &ConsumerState{}
	dec := gob.NewDecoder(bytes.NewReader(from))
	err := dec.Decode(s)
	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "decoding data")
	}
	return s, nil
}

func (s *ConsumerState) SaveToFile() error {
	f, err := data.OpenFileForWriting(s.saveStateFilepath, false)
	if err != nil {
		return errors.Wrap(err, "opening file for writing the state")
	}
	defer func() { _ = f.Close() }()
	block := directio.AlignedBlock(s.saveStateBlocksize)
	err = s.encode(block)
	if err != nil {
		return errors.Wrap(err, "encoding state")
	}
	_, err = f.Write(block)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("writing the state to file %s", s.saveStateFilepath))
	}
	return nil
}

func InitConsumerState(path string, saveBlocksize int) (*ConsumerState, error) {
	filepath := path + string(os.PathSeparator) + STATE_FILE
	f, err := data.OpenFileForReading(filepath)
	if err != nil {
		if os.IsNotExist(errors.Cause(err)) {
			// There is no `consumer.state` file. We'll return an empty object.
			// The file will eventually be created first time the state is saved.
			return &ConsumerState{
				saveStateFilepath:  filepath,
				saveStateBlocksize: saveBlocksize,
			}, nil
		}
		return nil, err
	}
	block := directio.AlignedBlock(saveBlocksize)
	_, err = f.Read(block)
	if err != nil {
		return nil, err
	}
	s, derr := decode(block)
	if derr != nil {
		return nil, derr
	}
	// Being private, these are not encoded. So let's add them.
	s.saveStateFilepath = filepath
	s.saveStateBlocksize = saveBlocksize
	return s, nil
}

func (s *ConsumerState) UseNew(filepath string, ReadBytes int64) {
	s.ReadFilepath = filepath
	s.ReadBytes = ReadBytes
}

func (s *ConsumerState) IsEmpty() bool {
	return s.ReadFilepath == ""
}

func (s *ConsumerState) SeekOffset() int64 {
	return s.ReadBytes
}
