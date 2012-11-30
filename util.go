package gokvlite

import (
	"io"
)

type sectionWriter struct {
	file   io.WriterAt
	offset int64
}

func (sw *sectionWriter) Write(data []byte) (n int, err error) {
	n, err = sw.file.WriteAt(data, sw.offset)
	return
}

func newSectionWriter(w io.WriterAt, off int64) *sectionWriter {
	sw := new(sectionWriter)
	sw.file = w
	sw.offset = off
	return sw
}
