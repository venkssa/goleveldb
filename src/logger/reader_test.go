package logger

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRecordReader_Success(t *testing.T) {
	tests := map[string]struct {
		fileOffset int64
	}{
		"Read full record": {
			fileOffset: 0,
		},
		"Read record when block offset is in the last 7 bytes": {
			fileOffset: blockSize - recordHeaderSize,
		},
		"Read record should skip last 6 bytes of a block": {
			fileOffset: blockSize - (recordHeaderSize - 1),
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			buf := new(OnlyOnceSeekableBuffer)
			w := NewRecordWriter(buf, test.fileOffset)
			input := []byte("hello world")

			writeFailOnError(t, w, input)
			readRecordAndVerify(t, NewRecordReader(buf, test.fileOffset), input)
		})
	}
}

func TestMultipleRecords(t *testing.T) {
	buf := new(OnlyOnceSeekableBuffer)
	w := NewRecordWriter(buf, 0)

	const bs = blockSize - recordHeaderSize
	input := make([]byte, 3*bs)
	fill(input[0:bs], 49)
	fill(input[bs:2*bs], 50)
	fill(input[2*bs:3*bs], 51)

	writeFailOnError(t, w, input)
	readRecordAndVerify(t, NewRecordReader(buf, 0), input)
}

func TestRecordRead_Error(t *testing.T) {
	tests := map[string]struct {
		buf           *DiscardAfterBuffer
		input         string
		expectedError error
	}{
		"First fragment successful but dies before wiriting the second record body; should return errorBodyEOF": {
			buf:           &DiscardAfterBuffer{N: 14},
			input:         "hello",
			expectedError: errorBodyEOF,
		},
		"First fragment successful but dies before wiriting the second record header; should return errorHeaderEOF": {
			buf:           &DiscardAfterBuffer{N: 13},
			input:         "hello",
			expectedError: errorHeaderEOF,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			writeFailOnError(t, NewRecordWriter(test.buf, blockSize-recordHeaderSize), []byte(test.input))

			resultBuf := new(bytes.Buffer)
			_, err := NewRecordReader(test.buf, blockSize-(recordHeaderSize)).Read(resultBuf)
			if err != test.expectedError {
				t.Fatalf("Expected '%v' but got '%v'", test.expectedError, err)
			}
		})
	}
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
