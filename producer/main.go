package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/devisions/go-playground/go-directio/config"
	"github.com/devisions/go-playground/go-directio/internal/data"
	"github.com/devisions/go-playground/go-directio/producer/internal"
	"github.com/ncw/directio"
	"github.com/pkg/errors"
)

var (
	// The block (re)used for writing.
	block []byte

	// Size of the `block`.
	blocksize int

	// Path where the files are written.
	filepathPrefix string

	// Maximum size of a file to write into.
	fileMaxsize int64

	// The current file to write into.
	out *os.File
)

func main() {
	// Setting up the graceful shutdown elements.
	stopCtx, cancelFn := context.WithCancel(context.Background())
	stopWg := &sync.WaitGroup{}
	stopWg.Add(2)

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("Failed to load config", err)
	}

	if done, err := data.MakePathIfNotExists(cfg.Path); err != nil {
		log.Fatalf("Failed to create (missing) path '%s' for writing files into. Reason: %s\n", cfg.Path, err)
	} else if done {
		log.Println("Created the (missing) path", cfg.Path)
	}

	block = directio.AlignedBlock(cfg.BlockSize)
	blocksize = len(block)
	filepathPrefix = cfg.Path
	fileMaxsize = cfg.MaxFileSizeBytes
	log.Printf("Using a %d bytes block, writing files in path %s\n", len(block), cfg.Path)

	dataCh := make(chan data.SomeData, 1_000_000)
	defer close(dataCh)

	go writer(dataCh, stopCtx, stopWg)
	go producer(dataCh, stopCtx, stopWg)

	waitingForGracefulShutdown(cancelFn, stopWg)
}

func writer(dataCh chan data.SomeData, stopCtx context.Context, stopWg *sync.WaitGroup) {

	f, err := internal.GetInitialFileForWriting(filepathPrefix, fileMaxsize)
	if err != nil {
		log.Fatalln("Failed to look for the next file to write into. Reason:", err)
	}
	if f == nil { // This should never happen; used just for safety.
		log.Fatalln("No file to write could be used.")
	}
	out = f
	log.Println("Ready to write on file", out.Name())

	running := true
	for running {
		select {
		case <-stopCtx.Done():
			log.Println("Stopping the writer ...")
			l := len(dataCh)
			if l > 0 {
				log.Printf("Draining the channel: writing to file the remaining %d data items ...", l)
				for len(dataCh) > 0 {
					d := <-dataCh
					if err := write(filepathPrefix, fileMaxsize, &d); err != nil {
						log.Println("Failed writing to file. Reason:", err)
						break
					}
				}
			}
			if out != nil { // Just for safety reasons.
				err := out.Close()
				if err != nil {
					log.Printf("Failed closing the file. Reason: %s", err)
				}
			}

			running = false
			break
		case d := <-dataCh:
			if err := write(filepathPrefix, fileMaxsize, &d); err != nil {
				log.Println("Failed writing to file. Reason:", err)
				running = false
				break
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Println("Writer stopped.")
	stopWg.Done()
}

func producer(dataCh chan data.SomeData, stopCtx context.Context, stopWg *sync.WaitGroup) {
	var i uint32
	log.Println("Starting to produce ...")
	running := true
	for running {
		select {
		case <-stopCtx.Done():
			log.Println("Stopping the producer ...")
			running = false
			break
		default:
			i++
			d := data.SomeData{Value: internal.RandStringMinMax(64, 601)}
			dataCh <- d
			// log.Printf("Produced (%d chars).\n", len(d.Value))
			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Println("Producer stopped.")
	stopWg.Done()
}

func write(filepathPrefix string, fileMaxsize int64, d *data.SomeData) error {
	ed := d.Encode()
	edl := len(ed)
	edlb := data.I64toBytes(uint64(edl))
	// Encoded data fits into one block.
	if blocksize >= 8+edl {
		copy(block, edlb)   // putting first the (bytes of the) encoded data length
		copy(block[8:], ed) // putting the encoded data
		if err := writeOut(block); err != nil {
			return err
		}
		log.Printf("[dbg]  chars: %d  edl: %d  wrote: %d  \n", len(d.Value), edl, 8+edl)
		return nil
	}
	// Encoded data must be written in multiple blocks.
	i := blocksize - 8
	// Same as one block case, in the 1st block we write the size and then the first part.
	copy(block, edlb) // putting first the (bytes of the) encoded data length
	copy(block[8:], ed[:i])
	if err := writeOut(block); err != nil {
		return err
	}
	log.Printf("[dbg]  chars: %d  edl: %d  wrote: %d  next i: %d\n", len(d.Value), edl, 8+i, i)
	// Next block(s).
	for edl-i > 0 {
		if edl > i+blocksize {
			copy(block, ed[i:i+blocksize])
			if err := writeOut(block); err != nil {
				return err
			}
			log.Printf("[dbg]  chars: %d  edl: %d  wrote: %d  next i: %d\n", len(d.Value), edl, 8+i, i+blocksize)
			i = i + blocksize
		} else {
			copy(block, ed[i:])
			if err := writeOut(block); err != nil {
				return err
			}
			log.Printf("[dbg]  chars: %d  edl: %d  wrote: %d  done.\n", len(d.Value), edl, edl-i)
			i = edl
		}
	}
	return nil
}

func writeOut(block []byte) error {
	f, err := internal.CheckNextFileForWriting(out, filepathPrefix, fileMaxsize)
	if err != nil {
		return err
	}
	// A new file has been provided, so close existing and start using it.
	if f != nil {
		if err := out.Close(); err != nil {
			log.Printf("[WARN] Failed to close existing file '%s'. Reason:%s\n", out.Name(), err)
		}
		log.Println("Writing to new file", f.Name())
		out = f
	}
	if _, err := out.Write(block); err != nil {
		return errors.Wrap(err, "writing to file")
	}
	return nil
}

func waitingForGracefulShutdown(cancelFn context.CancelFunc, stopWg *sync.WaitGroup) {
	osStopChan := make(chan os.Signal, 1)
	signal.Notify(osStopChan, syscall.SIGINT, syscall.SIGTERM)
	<-osStopChan
	log.Println("Shutting down ...")
	cancelFn()
	stopWg.Wait()
}
