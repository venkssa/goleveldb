package logger

import "testing"
import "bytes"

func TestRecordReader_ReadFullRecord(t *testing.T) {
	buf := bytes.Buffer{}
	input := "hello world"

	w := NewRecordWriter(&onlyOnceSeekableBuffer{Buffer: buf}, 0)

	writeFailOnError(t, w, []byte(input))

	r := NewRecordReader(&buf)

	resultBuf := new(bytes.Buffer)
	if _, err := r.Read(resultBuf); err != nil {
		t.Fatal(err)
	}

	if resultBuf.String() != input {
		t.Errorf("Expected '%v' as record data but got '%v'", input, resultBuf.String())
	}
}
