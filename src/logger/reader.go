package logger

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

type RecordReader struct {
	src         io.Reader
	blockOffset uint32
	header      header

	hash hash.Hash32
	buf  []byte
}

func NewRecordReader(src io.Reader, srcLength int64) *RecordReader {
	return &RecordReader{src: src,
		header:      newHeader(),
		hash:        crc32.NewIEEE(),
		blockOffset: uint32(srcLength % blockSize),
		buf:         make([]byte, blockSize-recordHeaderSize)}
}

// Read reads from reader decodes record header, validates checksum and writes to the writer.
// This currently does not handle corrupted records or properly skips records that are wrong.
func (rr *RecordReader) Read(w io.Writer) (n int, err error) {
	if size := uint32(recordHeaderSize - 1); blockSize-rr.blockOffset == size {
		if _, err = io.ReadFull(rr.src, rr.buf[0:size]); err != nil {
			return n, err
		}
	}
	hasMore := true
	for hasMore {
		_, err := io.ReadFull(rr.src, rr.header)
		if err != nil {
			return 0, fmt.Errorf("could not read record header: %v", err)
		}

		buf := rr.buf[0:rr.header.Length()]
		_, err = io.ReadFull(rr.src, buf)
		if err != nil {
			return n, fmt.Errorf("count not read record body: %v", err)
		}

		rr.hash.Reset()
		rr.hash.Write(rr.header.RecordTypeByte())
		rr.hash.Write(buf)

		if checksum := rr.hash.Sum32(); rr.header.Checksum() != checksum {
			return n, fmt.Errorf("failed checksum for record fragment: %d != %d", rr.header.Checksum(), checksum)
		}

		if rr.header.RecordType() == FULL || rr.header.RecordType() == LAST {
			hasMore = false
		}

		count, err := w.Write(buf)
		n += count
	}
	return n, err
}
