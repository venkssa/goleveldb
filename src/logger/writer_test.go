package logger

import (
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"testing"
)

func TestWriteRecordInBlock(t *testing.T) {
	buf := new(OnlyOnceSeekableBuffer)
	w := NewRecordWriter(buf, 0)
	input := []byte("hello world")

	writtenLen := writeFailOnError(t, w, input)

	verifyRecord(t, buf.Bytes(), []byte(input), FULL)

	verifyBlockOffset(t, w.blockOffset, recordHeaderSize+uint32(len(input)))
	verifyWrittenLength(t, writtenLen, recordHeaderSize+len(input))
}

func TestWrite_WhenBlockHasLessThan6Bytes_ShouldFillEndOfBlockWith6ZeroBytes(t *testing.T) {
	buf := new(OnlyOnceSeekableBuffer)
	w := NewRecordWriter(buf, blockSize-(recordHeaderSize-1))
	input := []byte("hello world")

	writtenLen := writeFailOnError(t, w, input)

	if zeroFill := buf.Next(6); !reflect.DeepEqual(zeroFill, sixEmptyBytes) {
		t.Errorf("Expected '%v' but found '%v'", sixEmptyBytes, zeroFill)
	}

	verifyRecord(t, buf.Bytes(), []byte(input), FULL)

	verifyWrittenLength(t, writtenLen, recordHeaderSize+len(input)+6)
	verifyBlockOffset(t, w.blockOffset, recordHeaderSize+uint32(len(input)))
}

func TestWrite_WhenBlockHas7Bytes_ShouldCreateAZeroLengthRecordWithRecordTypeFirst(t *testing.T) {
	buf := new(OnlyOnceSeekableBuffer)
	w := NewRecordWriter(buf, blockSize-recordHeaderSize)

	input := []byte("hello world")

	writtenLen := writeFailOnError(t, w, input)

	verifyRecord(t, buf.Next(7), []byte{}, FIRST)

	verifyRecord(t, buf.Bytes(), []byte(input), LAST)

	verifyWrittenLength(t, writtenLen, recordHeaderSize+len(input)+7)
	verifyBlockOffset(t, w.blockOffset, recordHeaderSize+uint32(len(input)))
}

func TestWrite_WhenRecodsSpansOver3Blocks_ShouldCreateFirstMiddleLastRecods(t *testing.T) {
	buf := new(OnlyOnceSeekableBuffer)
	w := NewRecordWriter(buf, 0)

	writtenLen := writeFailOnError(t, w, make([]byte, 3*(blockSize-recordHeaderSize)))
	verifyWrittenLength(t, writtenLen, 3*blockSize)

	for _, expectedRecordType := range []recordType{FIRST, MIDDLE, LAST} {
		record := buf.Next(blockSize)
		verifyRecordType(t, record, expectedRecordType)
		verifyRecordLength(t, record, blockSize-recordHeaderSize)
	}
}

func BenchmarkWriteRecord(b *testing.B) {
	buf := new(OnlyOnceSeekableBuffer)
	w := NewRecordWriter(buf, 0)

	record := make([]byte, 1023)
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(record); err != nil {
			b.Fatal(err)
		}
		buf.Reset()
	}
}

func writeFailOnError(t *testing.T, w *RecordWriter, input []byte) int {
	writtenLen, err := w.Write(input)
	if err != nil {
		t.Fatal(err)
	}
	return writtenLen
}

func verifyRecord(t *testing.T, record []byte, input []byte, expectedRecordType recordType) {
	verifyRecordLength(t, record, len(input))
	verifyRecordTypeAndChecksum(t, record, expectedRecordType, input)
	verifyData(t, record, []byte(input))
}

func verifyRecordLength(t *testing.T, record []byte, length int) {
	if actualLength := binary.LittleEndian.Uint16(record[4:6]); actualLength != uint16(length) {
		t.Errorf("Expected %v length but got %v", length, actualLength)
	}
}

func verifyRecordTypeAndChecksum(t *testing.T, record []byte, recordType recordType, input []byte) {
	verifyRecordType(t, record, recordType)
	expectedCheckSum := crc32.ChecksumIEEE(append([]byte{record[6]}, input...))
	if actualCheckSum := binary.LittleEndian.Uint32(record[0:4]); expectedCheckSum != actualCheckSum {
		t.Errorf("Expected %v checksum but was %v", expectedCheckSum, actualCheckSum)
	}
}

func verifyRecordType(t *testing.T, record []byte, expectedRecordType recordType) {
	if actualRecordType := recordType(record[6]); actualRecordType != expectedRecordType {
		t.Errorf("Expected %v RecordType but got %v", expectedRecordType, actualRecordType)
	}
}

func verifyData(t *testing.T, record []byte, input []byte) {
	if recordData := string(record[7:]); recordData != string(input) {
		t.Errorf("Expected '%v' but got '%v'", string(input), recordData)
	}
}

func verifyWrittenLength(t *testing.T, writtenLen, expectedWrittenLen int) {
	if writtenLen != expectedWrittenLen {
		t.Errorf("Expected %v bytes to be written but was %v", expectedWrittenLen, writtenLen)
	}
}

func verifyBlockOffset(t *testing.T, blockOffset, expectedBlockOffset uint32) {
	if blockOffset != expectedBlockOffset {
		t.Errorf("Expected %v expectedBlockOffset but got %v", expectedBlockOffset, blockOffset)
	}
}
