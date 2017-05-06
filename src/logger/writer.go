package logger

import (
	"hash"
	"hash/crc32"
	"io"
)

type RecordWriter struct {
	dest        *trackingWriteSeeker
	blockOffset uint32
	h           hash.Hash32

	header
}

// NewRecordWriter creates a writer that writes recods to the dest WriteSeeker.
// The WriteSeeker would be seeked only once and would be seeked to destLength relative to the start of the file.
func NewRecordWriter(dest io.WriteSeeker, destLength int64) *RecordWriter {
	return &RecordWriter{
		dest:        &trackingWriteSeeker{dest: dest, destLength: destLength},
		blockOffset: uint32(destLength % blockSize),
		h:           crc32.NewIEEE(),
	}
}

var sixEmptyBytes = []byte{0, 0, 0, 0, 0, 0}

// Write writes record to the dest writer.
//
// It returns the number of bytes written to the writer.
// This number > len(p) as it includes record header and each fragment as described in package doc.
func (w *RecordWriter) Write(p []byte) (int, error) {
	w.dest.Reset()

	var start uint32
	var end uint32
	isFirstRecord := true
	last := uint32(len(p))

	for end < last && w.dest.err == nil {
		switch remainingInBlock := blockSize - w.blockOffset; remainingInBlock {
		case 6:
			w.dest.Write(sixEmptyBytes)
			w.blockOffset = 0
			if w.dest.err != nil {
				break
			}
		case 0:
			w.blockOffset = 0
		}
		availableForData := blockSize - w.blockOffset - recordHeaderSize
		end = last
		if end-start > availableForData {
			end = start + availableForData
		}
		recordType := MIDDLE
		if isFirstRecord && end == last {
			recordType = FULL
		} else if isFirstRecord && end < last {
			recordType = FIRST
		} else if end == last {
			recordType = LAST
		}

		isFirstRecord = false
		w.writeRecordFragment(recordType, p[start:end])
		start = end
	}

	return w.dest.n, w.dest.err
}

func (w *RecordWriter) writeRecordFragment(rt recordType, p []byte) {
	w.header.SetLength(uint16(len(p)))
	w.header.SetRecordType(rt)

	w.h.Reset()
	w.h.Write(w.header.RecordTypeByte())
	w.h.Write(p)
	w.header.SetChecksum(w.h.Sum32())

	w.dest.Write(w.header[0:])
	w.dest.Write(p)
	w.blockOffset += recordHeaderSize + uint32(len(p))
}

type trackingWriteSeeker struct {
	dest          io.WriteSeeker
	destLength    int64
	alreadySeeked bool

	n   int
	err error
}

func (tw *trackingWriteSeeker) Write(p []byte) {
	if !tw.alreadySeeked {
		tw.dest.Seek(tw.destLength, io.SeekStart)
		tw.alreadySeeked = true
	}
	if tw.err == nil {
		_, tw.err = tw.dest.Write(p)
	}
	tw.n += len(p)
}

func (tw *trackingWriteSeeker) Reset() {
	tw.n = 0
	tw.err = nil
}
