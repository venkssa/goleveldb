package logger

import (
	"bytes"
	"fmt"
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
			buf.ResetSeeker()
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
	buf.ResetSeeker()
	readRecordAndVerify(t, NewRecordReader(buf, 0), input)
}

func TestRecordRead_Error(t *testing.T) {
	tests := map[string]struct {
		buf           *DiscardAfterBuffer
		input         string
		expectedError error
	}{
		"First fragment successful but dies before writing the second record body; should return errorBodyEOF": {
			buf:           &DiscardAfterBuffer{N: 14},
			input:         "hello",
			expectedError: errorBodyEOF,
		},
		"First fragment successful but dies before writing the second record header; should return errorHeaderEOF": {
			buf:           &DiscardAfterBuffer{N: 13},
			input:         "hello",
			expectedError: errorHeaderEOF,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			writeFailOnError(t, NewRecordWriter(test.buf, blockSize-recordHeaderSize), []byte(test.input))

			resultBuf := new(bytes.Buffer)
			test.buf.ResetSeeker()
			_, err := NewRecordReader(test.buf, blockSize-(recordHeaderSize)).Read(resultBuf)
			if err != test.expectedError {
				t.Fatalf("Expected '%v' but got '%v'", test.expectedError, err)
			}
		})
	}
}

// Given two records and the first record spans across multiple fragments and the writer dies after
// first record fragment and the writer sucessfully writes the second record as a full fragment,
// reading the records should fail with a RecordTypeMissmatchError.
func TestRecordRead_MultipleRecord_FirstPartialRecordSecondFullRecord_ShouldBeAnError(t *testing.T) {
	buf := &DiscardAfterBuffer{N: 8}
	rw := NewRecordWriter(buf, blockSize-recordHeaderSize-1)

	firstMultiFragmentRecord := "first"
	writeFailOnError(t, rw, []byte(firstMultiFragmentRecord))

	secondFullRecord := "second"
	buf.N = recordHeaderSize + len(secondFullRecord)
	writeFailOnError(t, rw, []byte(secondFullRecord))

	buf.ResetSeeker()
	resultBuf := new(bytes.Buffer)
	n, err := NewRecordReader(buf, blockSize-recordHeaderSize-1).Read(resultBuf)

	expectedErr := RecordTypeMissmatchError{FIRST, FULL}
	if err != expectedErr || n != 1 {
		t.Fatalf("Expected '(n=1, err=%v)' but got '(n=%v, err=%v)'", expectedErr, n, err)
	}
	if resultBuf.String() != "f" {
		t.Errorf("Expected 'f' but got %v", resultBuf)
	}
}

func TestShouldExpectMoreRecordFragment(t *testing.T) {
	tests := []struct {
		prev                     recordType
		current                  recordType
		expectedShouldExpectMore bool
	}{
		{
			prev:                     uninit,
			current:                  FULL,
			expectedShouldExpectMore: false,
		},
		{
			prev:                     uninit,
			current:                  FIRST,
			expectedShouldExpectMore: true,
		},
		{
			prev:                     FIRST,
			current:                  MIDDLE,
			expectedShouldExpectMore: true,
		},
		{
			prev:                     FIRST,
			current:                  LAST,
			expectedShouldExpectMore: false,
		},
		{
			prev:                     MIDDLE,
			current:                  LAST,
			expectedShouldExpectMore: false,
		},
	}
	for _, testData := range tests {
		testName := fmt.Sprintf("%v => %v should be valid", testData.prev, testData.current)
		t.Run(testName, func(t *testing.T) {
			shouldExpectMore, err := shouldExpectMoreRecordFragments(testData.prev, testData.current)
			if shouldExpectMore != testData.expectedShouldExpectMore || err != nil {
				t.Errorf("Expected (%v, %v) but got (%v, %v)",
					testData.expectedShouldExpectMore, nil, shouldExpectMore, err)
			}
		})
	}
}

func TestShouldExpectMoreRecordFragment_Error(t *testing.T) {
	tests := []struct {
		prev    recordType
		current recordType
	}{
		{prev: uninit, current: LAST},
	}
	for _, testData := range tests {
		testName := fmt.Sprintf("%v => %v should be invalid", testData.prev, testData.current)
		t.Run(testName, func(t *testing.T) {
			shouldExpectMore, err := shouldExpectMoreRecordFragments(testData.prev, testData.current)
			expectedErr := RecordTypeMissmatchError{testData.prev, testData.current}
			if shouldExpectMore != false || err != expectedErr {
				t.Errorf("Expected (%v, %v) but got (%v, %v)", false, expectedErr, shouldExpectMore, err)
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
