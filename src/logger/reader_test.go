package logger

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRecordReader_ReadFullRecord(t *testing.T) {
	so := new(onlyOnceSeekableBuffer)
	input := "hello world"

	w := NewRecordWriter(so, 0)

	writeFailOnError(t, w, []byte(input))
	readRecordAndVerify(t, NewRecordReader(so, 0), []byte(input))
}

func TestMultipleRecords(t *testing.T) {
	so := new(onlyOnceSeekableBuffer)
	w := NewRecordWriter(so, 0)

	const bs = blockSize - recordHeaderSize
	input := make([]byte, 3*bs)
	fill(input[0:bs], 49)
	fill(input[bs:2*bs], 50)
	fill(input[2*bs:3*bs], 51)

	writeFailOnError(t, w, input)
	readRecordAndVerify(t, NewRecordReader(so, 0), input)
}

func TestRecordReader_ReadRecordStartingAtLast7Bytes(t *testing.T) {
	so := new(onlyOnceSeekableBuffer)
	w := NewRecordWriter(so, blockSize-recordHeaderSize)

	input := []byte("hello world")

	writeFailOnError(t, w, input)
	readRecordAndVerify(t, NewRecordReader(so, blockSize-recordHeaderSize), input)
}

func TestRead_WhenBlockHasLessThan6Bytes_ShouldSkip6BytesAndReadRecord(t *testing.T) {
	so := new(onlyOnceSeekableBuffer)
	w := NewRecordWriter(so, blockSize-(recordHeaderSize-1))
	input := []byte("hello world")

	writeFailOnError(t, w, input)
	readRecordAndVerify(t, NewRecordReader(so, blockSize-(recordHeaderSize-1)), input)
}

func fill(buf []byte, n byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = n
	}
}

func readRecordAndVerify(t *testing.T, r *RecordReader, input []byte) {
	resultBuf := new(bytes.Buffer)
	if _, err := r.Read(resultBuf); err != nil {
		t.Fatal(err)
	}

	if resultBuf.Len() != len(input) {
		t.Fatalf("Expected %v but was %v", len(input), resultBuf.Len())
	}

	if !reflect.DeepEqual(resultBuf.Bytes(), input) {
		t.Fatal("Expected contents to be equal but was not")
	}
}
