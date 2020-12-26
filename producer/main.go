package main

import (
	"context"
	"fmt"
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

// Global block, reused for writing.
var block []byte

// The size of the block used for writing.
var blocksize int

// Global state of the current file to write into.
var out *os.File

func main() {
	// Preparing the graceful shutdown elements.
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
	blocksize = cfg.BlockSize
	log.Printf("Using blocksize %d, writing files in path %s\n", cfg.BlockSize, cfg.Path)

	dataCh := make(chan data.SomeData, 1_000_000)
	defer close(dataCh)

	go writer(cfg.Path, cfg.MaxFileSizeBytes, dataCh, stopCtx, stopWg)
	go producer(dataCh, stopCtx, stopWg)

	waitingForGracefulShutdown(cancelFn, stopWg)
}

func writer(filepathPrefix string, fileMaxsize int64, dataCh chan data.SomeData, stopCtx context.Context, stopWg *sync.WaitGroup) {

	f, err := internal.GetFileForWriting(nil, filepathPrefix, fileMaxsize)
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
					log.Println("Wrote", d)
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
			d := data.SomeData{Value: internal.RandStringMinMax(1, 101)}
			dataCh <- d
			log.Printf("Produced (%d chars) %+v\n", len(d.Value), d)
			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Println("Producer stopped.")
	stopWg.Done()
}

func write(filepathPrefix string, fileMaxsize int64, d *data.SomeData) error {

	f, err := internal.GetFileForWriting(out, filepathPrefix, fileMaxsize)
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
	// err := d.Encode(block)
	ed := d.Encode()
	edl := (len(ed))
	log.Println("[dbg] encoded data length:", edl)
	if blocksize >= edl {
		// Encoded data's length fits into the block.
		copy(block, ed)
		_, err = out.Write(block)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("writing to file (the item %+v)", *d))
		}
		return nil
	}
	// Encoded data's length must be written using multiple blocks.
	// TODO
	log.Fatal("Unimplemented writing encoded data in multiple blocks")
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
