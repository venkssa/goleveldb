package logger

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

type RecordReader struct {
	src *trackingReadSeeker

	header header

	hash hash.Hash32
	buf  []byte
}

func NewRecordReader(src io.ReadSeeker, srcLength int64) *RecordReader {
	src.Seek(srcLength, io.SeekStart)
	return &RecordReader{
		src:    newTrackingReadSeeker(src, srcLength),
		header: newHeader(),
		hash:   crc32.NewIEEE(),
		buf:    make([]byte, blockSize-recordHeaderSize)}
}

var errorHeaderEOF = fmt.Errorf("could not read record header: %v", io.EOF)
var errorBodyEOF = fmt.Errorf("count not read record body: %v", io.EOF)

// Read reads from reader decodes record header, validates checksum and writes to the writer.
func (rr *RecordReader) Read(w io.Writer) (int, error) {
	if err := rr.src.SkipEndOfBlock(); err != nil {
		return 0, err
	}

	hasMore := true
	prevRecordType := uninit
	var totalBytesWritten int

	for hasMore {
		if _, err := rr.src.ReadFull(rr.header); err != nil {
			return totalBytesWritten, errorHeaderEOF
		}

		buf := rr.buf[0:rr.header.Length()]
		if _, err := rr.src.ReadFull(buf); err != nil {
			return totalBytesWritten, errorBodyEOF
		}

		rr.hash.Reset()
		rr.hash.Write(rr.header.RecordTypeByte())
		rr.hash.Write(buf)
		if checksum := rr.hash.Sum32(); rr.header.Checksum() != checksum {
			return totalBytesWritten, fmt.Errorf("failed checksum for record fragment: %d != %d", rr.header.Checksum(), checksum)
		}

		expectMore, err := shouldExpectMoreRecordFragments(prevRecordType, rr.header.RecordType())
		if err != nil {
			return totalBytesWritten, err
		}
		hasMore = expectMore
		prevRecordType = rr.header.RecordType()

		count, err := w.Write(buf)
		totalBytesWritten += count
		if err != nil {
			return totalBytesWritten, fmt.Errorf("cannot write to buffer: %v", err)
		}
	}

	return 0, nil
}

func shouldExpectMoreRecordFragments(prev, curr recordType) (bool, error) {
	if prev == uninit && curr == FULL {
		return false, nil
	}
	if prev == uninit && curr == FIRST {
		return true, nil
	}
	if prev == FIRST && curr == MIDDLE {
		return true, nil
	}
	if (prev == FIRST || prev == MIDDLE) && curr == LAST {
		return false, nil
	}

	return false, RecordTypeMissmatchError{prev, curr}
}

type RecordTypeMissmatchError struct {
	prev, curr recordType
}

func (r RecordTypeMissmatchError) Error() string {
	return fmt.Sprintf("unexpected recordType, %v => %v not allowed", r.prev, r.curr)
}

const skippableEndOfBlockSize = uint32(recordHeaderSize - 1)

type trackingReadSeeker struct {
	io.ReadSeeker
	blockOffset uint32

	skippableEndOfBlockBuf []byte
}

func newTrackingReadSeeker(src io.ReadSeeker, seekOffset int64) *trackingReadSeeker {
	return &trackingReadSeeker{
		ReadSeeker:             src,
		blockOffset:            uint32(seekOffset % blockSize),
		skippableEndOfBlockBuf: make([]byte, skippableEndOfBlockSize),
	}
}

func (r *trackingReadSeeker) SkipEndOfBlock() error {
	if blockSize-r.blockOffset > skippableEndOfBlockSize {
		return nil
	}

	if _, err := r.ReadFull(r.skippableEndOfBlockBuf); err != nil {
		return err
	}
	return nil
}

func (r *trackingReadSeeker) ReadFull(buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	r.blockOffset += uint32(n)
	return n, err
}
