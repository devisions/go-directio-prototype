package internal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/devisions/go-playground/go-directio/internal/data"
	"github.com/pkg/errors"
)

func GetInitialFileForWriting(path string, maxsize int64) (*os.File, error) {
	file, err := getLatestFileNameForWriting(path)
	if err != nil {
		if os.IsNotExist(err) {
			return openNewFileForWriting(path)
		}
		return nil, errors.Wrap(err, "trying to get new file for writing")
	}
	filepath := path + string(os.PathSeparator) + file
	// Checking the size before returning it.
	f, err := data.OpenFileForWriting(filepath, true)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "trying to get the current file info")
	}
	if fi.Size() >= maxsize {
		return openNewFileForWriting(path)
	}
	return f, nil
}

// CheckNextFileForWriting checks if a next file should be used for writing.
// If existing file reached the max size, it initializes a new file and returns it.
// Otherwise, it returns nil, meaning that `curr` file can still be used for writing.
func CheckNextFileForWriting(curr *os.File, path string, maxsize int64) (*os.File, error) {
	// First, let's check the current file size, if provided.
	if curr != nil {
		fi, err := curr.Stat()
		if err != nil {
			return nil, errors.Wrap(err, "trying to get the current file info")
		}
		if fi.Size() >= maxsize {
			return openNewFileForWriting(path)
		}
		return nil, nil
	}
	return nil, errors.New("GetNextFileForWriting needs a current file to start from")
}

func openNewFileForWriting(path string) (*os.File, error) {
	filepath := fmt.Sprintf("%s%s%d.dat", path, string(os.PathSeparator), time.Now().UTC().Unix())
	return data.OpenFileForWriting(filepath, false)
}

func getLatestFileNameForWriting(iopath string) (string, error) {
	fs, err := ioutil.ReadDir(iopath)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("looking for files on path '%s'", iopath))
	}
	var fi os.FileInfo
	for _, f := range fs {
		fname := f.Name()
		if ".dat" == path.Ext(fname) {
			fi = f
		}
	}
	if fi == nil {
		return "", os.ErrNotExist
	}
	return fi.Name(), nil
}
