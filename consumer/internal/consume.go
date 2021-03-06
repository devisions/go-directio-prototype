package internal

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/devisions/go-playground/go-directio/internal/data"
)

type ReadData struct {
	Data         *data.SomeData
	FromFilepath string
	ReadBytes    int64
}

// CheckFileForNextReading checks if the current or a next file should be used for reading.
// If current file reached the max size, it looks for a newer file and returns it.
// Otherwise, it returns nil, meaning that `curr` file can still be used for reading.
func CheckFileForNextReading(curr *os.File, path string, readBytes int64, maxsize int64) (*os.File, error) {
	if readBytes == maxsize {
		fname, err := GetNextFileNameForReading(path, curr.Name())
		if err != nil {
			return nil, err
		}
		// Closing the `curr`ent file.
		if err := curr.Close(); err != nil {
			log.Printf("[WARN] Failed to close existing file '%s'. Reason:%s\n", curr.Name(), err)
		}
		return data.OpenFileForReading(path + string(os.PathSeparator) + fname)
	}
	return nil, nil
}

func GetFirstFileNameForReading(iopath string) (string, error) {
	fs, err := ioutil.ReadDir(iopath)
	if err != nil {
		return "", err
	}
	for _, f := range fs {
		if ".dat" == path.Ext(f.Name()) {
			return f.Name(), nil
		}
	}
	return "", os.ErrNotExist
}

func GetNextFileNameForReading(iopath string, lastFilePath string) (string, error) {
	// fis, err := ioutil.ReadDir(iopath)
	f, err := os.Open(iopath)
	defer func() { _ = f.Close() }()
	if err != nil {
		return "", err
	}
	fnames, err := f.Readdirnames(0)
	if err != nil {
		return "", err
	}
	sort.Strings(fnames)
	intLastFilename, err := getIntOfFilenameWithoutExt(lastFilePath)
	if err != nil {
		return "", err
	}
	// log.Printf("[dbg] Readdirnames => %v\n", fnames)
	// log.Println("[dbg] getNextFileNameForReading intLastFilename:", intLastFilename)
	for _, fn := range fnames {
		if ".dat" == path.Ext(fn) {
			intCurrFilename, err := getIntOfFilenameWithoutExt(fn)
			if err != nil {
				// ignoring it, it doesn't follow the pattern {UTC}.dat (ex: 1609074647.dat)
				continue
			}
			if intCurrFilename > intLastFilename {
				return fn, nil
			}
		}
	}
	return "", os.ErrNotExist
}

func getIntOfFilenameWithoutExt(filepath string) (int, error) {
	filenameNoExt := strings.TrimSuffix(path.Base(filepath), ".dat")
	return strconv.Atoi(filenameNoExt)
}
