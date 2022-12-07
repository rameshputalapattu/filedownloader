package main

import (
	"fmt"
	"io"
	"sync"

	"os"
	"path"

	"github.com/lucperkins/rek"
)

const URI = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%d-%02d.parquet"
const START_YEAR = 2009
const END_YEAR = 2021
const NUM_WORKERS = 100

type task interface {
	process()
	print()
}

type factory interface {
	make(line string) task
}

type fileItem struct {
	URL            string
	err            error
	downloadStatus string
}

func (fi *fileItem) process() {

	res, err := rek.Get(fi.URL)

	if err != nil {
		fi.downloadStatus = "failure"
		fi.err = err
		return
	}

	fileName := path.Base(fi.URL)

	fh, err := os.Create(fileName)

	if err != nil {
		fi.err = err
		fi.downloadStatus = "failure"
		return
	}

	defer fh.Close()
	defer res.Body().Close()
	_, err = io.Copy(fh, res.Body())

	if err != nil {
		fi.err = err
		fi.downloadStatus = "failure"
		return
	}

	fi.downloadStatus = "success"

}

func (fi *fileItem) print() {
	fmt.Printf("%s -> %s processed;status:%s\n", fi.URL, path.Base(fi.URL), fi.downloadStatus)
}

type filedownloadFactory struct {
}

func (*filedownloadFactory) make(line string) task {
	return &fileItem{
		URL: line,
	}
}

func run(f factory) {
	var wg sync.WaitGroup
	in := make(chan task)
	wg.Add(1)

	go func() {

		for year := START_YEAR; year <= END_YEAR; year++ {
			for month := 1; month <= 12; month++ {

				fileURL := fmt.Sprintf(URI, year, month)
				in <- f.make(fileURL)

			}

		}
		close(in)
		wg.Done()

	}()

	out := make(chan task)

	for i := 0; i < NUM_WORKERS; i++ {
		wg.Add(1)

		go func() {
			for t := range in {
				t.process()
				out <- t
			}
			wg.Done()
		}()

	}

	go func() {
		wg.Wait()
		close(out)
	}()

	for t := range out {
		t.print()
	}

}

func main() {
	var fdf filedownloadFactory
	run(&fdf)

}
