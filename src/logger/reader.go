package logger

import (
	"io"
)

type RecordReader struct {
	dest   io.Reader
	header []byte
}

func NewRecordReader(dest io.Reader) *RecordReader {
	return &RecordReader{dest: dest}
}

func (rr *RecordReader) Read(w io.Writer) (n int, err error) {
	rr.dest.Read(rr.header)
	return 0, nil
}
