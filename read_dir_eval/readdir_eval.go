package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)

func main() {

	// path := "/tmp/test-directio"
	path := "/tmp/cnlc_scd"

	var start time.Time
	var f *os.File
	var fnames []string
	var err error

	// --------------------------------------------
	// ioutil.ReadDir option
	// --------------------------------------------

	start = time.Now()
	_, err = ioutil.ReadDir(path)
	fmt.Println(">>> ioutil.ReadDir exec time:", time.Since(start))

	if err != nil {
		fmt.Println(">>> ioutil.ReadDir error:", err)
	}

	// --------------------------------------------
	// os.File.Readdir option
	// --------------------------------------------

	f, err = os.Open(path)
	if err != nil {
		fmt.Println(">>> os.Open error:", err)
	}

	start = time.Now()
	fnames, err = f.Readdirnames(0)
	fmt.Println(">>> os.File.Readdirnames exec time:", time.Since(start))

	if err != nil {
		fmt.Println(">>> os.File.Readdirnames error:", err)
	}

	fmt.Printf(">>> os.File.Readdirnames result has %d entries.\n", len(fnames))

	// The order does seem to be different (raised this: https://github.com/golang/go/issues/43435)
	// Ex:
	// $ ll | head
	// total 2.0G
	// -rw-rw-r-x 1 devisions devisions 2.0K Dec 30 15:21 1609334505470162730.dat
	// -rw-rw-r-x 1 devisions devisions 2.0K Dec 30 15:21 1609334505470307036.dat
	// -rw-rw-r-x 1 devisions devisions 2.0K Dec 30 15:21 1609334505470448185.dat
	// -rw-rw-r-x 1 devisions devisions 2.0K Dec 30 15:21 1609334505470588570.dat
	// -rw-rw-r-x 1 devisions devisions 2.0K Dec 30 15:21 1609334505470740061.dat
	//
	// Running this part below returns:
	// >>>  1609334524911536056.dat
	// >>>  1609334541905205283.dat
	// >>>  1609334569413747717.dat
	// >>>  1609334508584060783.dat
	// >>>  1609334515433846485.dat
	// >>>  1609334538224587818.dat

	// for i, fname := range fnames {
	// 	fmt.Println(">>> ", fname)
	// 	if i > 4 {
	// 		break
	// 	}
	// }

	// Let's see how much time is spent on sorting.

	start = time.Now()
	sort.Strings(fnames)
	fmt.Println(">>> sort exec time:", time.Since(start))

}
