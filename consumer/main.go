package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/devisions/go-playground/go-directio/config"
	"github.com/devisions/go-playground/go-directio/consumer/internal"
	"github.com/devisions/go-playground/go-directio/internal/data"
	"github.com/ncw/directio"
	"github.com/pkg/errors"
)

var (
	// Global var: Block (re)used for reading.
	gBlock []byte

	// Global var: Size of the `block`.
	gBlocksize int

	// Global var: Path where the files to read from exist.
	gFilepathPrefix string

	// Global var: Maximum size of a file.
	gFileMaxsize int64

	// Global var: Read bytes from the current file.
	gReadBytes int64

	// Global var: The current file to read from.
	gIn *os.File

	// Global var: State of the consumer.
	gState *internal.ConsumerState
)

// Maximum value of encoded data length (65KB).
const MAX_EDL = 65 * 1024

func main() {
	// Setting up the graceful shutdown elements.
	stopCtx, cancelFn := context.WithCancel(context.Background())
	stopWg := &sync.WaitGroup{}
	stopWg.Add(2)

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("Failed to load config", err)
	}

	gBlock = directio.AlignedBlock(cfg.BlockSize)
	gBlocksize = len(gBlock)
	gFilepathPrefix = cfg.Path
	gFileMaxsize = cfg.MaxFileSizeBytes
	log.Printf("Using a %d bytes block, reading files from path %s\n", gBlocksize, cfg.Path)

	dataCh := make(chan *internal.ReadData, 1_000_000)

	gState, err = internal.InitConsumerState(cfg.Path, cfg.BlockSize)
	if err != nil {
		log.Fatalln("Failed to init state. Reason:", err)
	}
	if !gState.IsEmpty() {
		log.Printf("Starting with state { ReadFilepath: %s ReadBytes: %d }\n", gState.ReadFilepath, gState.ReadBytes)
	} else {
		log.Printf("Starting with an empty state.")
	}

	go consumer(dataCh, stopCtx, stopWg)
	go reader(dataCh, stopCtx, stopWg)

	waitingForGracefulShutdown(cancelFn, stopWg)
}

func reader(dataCh chan *internal.ReadData, stopCtx context.Context, stopWg *sync.WaitGroup) {

	var f *os.File
	var err error
	showInitialWarn := true
	initing := true
	for initing {
		select {
		case <-stopCtx.Done():
			log.Println("Reader has stopped.")
			stopWg.Done()
			return
		default:
			if !gState.IsEmpty() {
				f, err = data.OpenFileForReading(gState.ReadFilepath)
				if err != nil {
					if os.IsNotExist(errors.Cause(err)) {
						fname, err := internal.GetNextFileNameForReading(gFilepathPrefix, gState.ReadFilepath)
						if err == os.ErrNotExist {
							if gState.ReadBytes < gFileMaxsize && showInitialWarn {
								log.Println("[WARN] Last (not completely) read file is missing. Didn't found a next file yet ...")
								showInitialWarn = false
							} else if showInitialWarn {
								log.Println("Didn't found a next file yet ...")
								showInitialWarn = false

							}
						} else {
							fp := gFilepathPrefix + string(os.PathSeparator) + fname
							f, err = data.OpenFileForReading(fp)
							if err != nil {
								log.Fatalf("Could not use the next file found '%s'. Reason: %s\n", fp, err)
							}
							log.Println("Found the next file", fp)
							gState.ReadBytes = 0 // resetting for consistency
						}
					} else {
						log.Fatalln("Failed to open last read file (according to the state). Reason:", err)
					}
				}
			} else {
				// There is no last state, so let's start with the first file that might exist.
				fname, err := internal.GetFirstFileNameForReading(gFilepathPrefix)
				if err != nil {
					if err != os.ErrNotExist {
						log.Fatal("Failed trying to use the first file. Reason:", err)
					}
					if showInitialWarn {
						log.Println("Didn't found a next file yet ...")
						showInitialWarn = false
					}
				} else {
					log.Println("initing > GetFirstFileNameForReading => fname:", fname, " err:", err)
					fp := gFilepathPrefix + string(os.PathSeparator) + fname
					f, err = data.OpenFileForReading(fp)
					if err != nil {
						log.Fatalf("Could not use the first file found '%s'. Reason: %s\n", fp, err)
					}
				}
			}
			if f == nil {
				// No file exists, either (one of these):
				// - the last read, according to the state
				// - the next one, if last read file is missing
				// - first one, if there is no previous state
				time.Sleep(1 * time.Second)
				continue
			}
			initing = false
		}
	}

	if gState.ReadBytes > 0 {
		if _, err := f.Seek(gState.SeekOffset(), 0); err != nil {
			log.Fatalln("Failed to skip already read blocks. Reason", err)
		}
		gReadBytes = gState.ReadBytes
		log.Println("Reading from file", f.Name(), "and skipping", gState.ReadBytes, "bytes")
	} else {
		log.Println("Reading from file", f.Name())
	}
	gIn = f

	running := true
	for running {
		select {
		case <-stopCtx.Done():
			log.Println("Stopping the reader ...")
			err := gIn.Close()
			if err != nil {
				log.Printf("Failed to close the file. Reason: %s", err)
			}
			running = false
			break
		default:
			d, err := readIn()
			if err != nil {
				if err == os.ErrNotExist || err == io.EOF {
					// There is no new file to read from OR
					// nothing else to read on existing file. Let's wait ...
					time.Sleep(1 * time.Second)
					continue
				}
				if err != io.EOF {
					log.Fatalln("Failed to read from file. Reason:", err)
				}
				// Since we didn't get ErrNoExist it means that there are still some files to consume.
				continue
			}
			dataCh <- d
		}
	}
	log.Println("Reader has stopped.")
	stopWg.Done()
}

func readIn() (*internal.ReadData, error) {

	f, err := internal.CheckFileForNextReading(gIn, gFilepathPrefix, gReadBytes, gFileMaxsize)
	if err != nil {
		return nil, err
	}
	if f != nil {
		log.Println("Reading from new file", f.Name())
		gIn = f
		gReadBytes = 0
	}

	_, err = gIn.Read(gBlock)
	if err != nil {
		if err != io.EOF {
			log.Fatalln("Failed to read from file. Reason:", err)
		}
		return nil, io.EOF
	}
	gReadBytes += int64(gBlocksize)

	// First, let's get the encoded data length (edl) from the beginning of this 1st block.
	edl := int(data.BytesToI64(gBlock[:8]))
	if edl > MAX_EDL {
		log.Printf("[WARN] Cannot read from file '%s' since it contains data from a previous file. Skipping it...\n", gIn.Name())
		// Forcing to skip the current file and get the next one.
		gReadBytes = gFileMaxsize
		return nil, io.EOF
	}

	// Encoded data fits into one block.
	if gBlocksize >= 8+int(edl) {
		d, err := data.Decode(gBlock[8:])
		if err != nil {
			log.Fatalln("Failed to decode data", err)
		}
		log.Printf("[dbg]  edl: %d  read: %d  done.\n", edl, gBlocksize)
		return &internal.ReadData{
			Data:         d,
			FromFilepath: gIn.Name(),
			ReadBytes:    gReadBytes,
		}, nil
	}

	// Encoded data was written in multiple blocks.
	i := gBlocksize - 8
	ed := make([]byte, edl) // encoded data (ed) bytes
	copy(ed, gBlock[8:])
	for edl-i > 0 {
		f, err := internal.CheckFileForNextReading(gIn, gFilepathPrefix, gReadBytes, gFileMaxsize)
		if err != nil {
			return nil, err
		}
		if f != nil {
			log.Println("Reading from new file", f.Name())
			gIn = f
			gReadBytes = 0
		}
		// Read the next block of encoded data.
		_, err = gIn.Read(gBlock)
		if err != nil {
			log.Fatalln("Failed to read from file (next block of existing data). Reason:", err)
		}
		gReadBytes += int64(gBlocksize)

		if edl > i+gBlocksize {
			copy(ed[i:i+gBlocksize], gBlock)
			log.Printf("[dbg]  edl: %d  read: %d  next i: %d\n", edl, gBlocksize, i+gBlocksize)
			i = i + gBlocksize
		} else {
			copy(ed[i:], gBlock)
			log.Printf("[dbg]  edl: %d  read: %d  done.\n", edl, edl-i)
			i = edl
		}
	}
	// Finally, read all blocks of encoded data bytes. Let's decode it.
	d, err := data.Decode(ed)
	if err != nil {
		log.Fatalln("Failed to decode data", err)
	}
	return &internal.ReadData{
		Data:         d,
		FromFilepath: gIn.Name(),
		ReadBytes:    gReadBytes,
	}, nil
}

func consumer(dataCh chan *internal.ReadData, stopCtx context.Context, stopWg *sync.WaitGroup) {
	running := true
	for running {
		select {

		case <-stopCtx.Done():
			log.Println("Stopping the consumer ...")
			running = false
			break

		case cd := <-dataCh:
			log.Printf("Consumed Text: %d chars, Number: %d\n", len(cd.Data.Text), cd.Data.Number)
			tryDelete(gState.ReadFilepath, gFileMaxsize)
			if cd.FromFilepath != gState.ReadFilepath {
				gState.UseNew(cd.FromFilepath, cd.ReadBytes)
			} else {
				gState.ReadBytes = cd.ReadBytes
			}
			err := gState.SaveToFile()
			if err != nil {
				log.Fatalln("Failed to save state to file. Reason:", err)
			}

		default:
			time.Sleep(1 * time.Second)
		}
	}
	log.Println("Consumer has stopped.")
	stopWg.Done()
}

func tryDelete(filepath string, maxSize int64) bool {
	deleted, err := data.DeleteFileIfReachedMaxSize(filepath, maxSize)
	if err != nil {
		log.Println("[WARN] Failed while trying to check and delete the consumed file", filepath, "Reason:", err)
	}
	if deleted {
		// log.Println("Deleted the consumed file", gState.ReadFilepath)
		return true
	}
	return false
}

func waitingForGracefulShutdown(cancelFn context.CancelFunc, stopWg *sync.WaitGroup) {
	osStopChan := make(chan os.Signal, 1)
	signal.Notify(osStopChan, syscall.SIGINT, syscall.SIGTERM)
	<-osStopChan
	log.Println("Shutting down ...")
	cancelFn()
	stopWg.Wait()
}
