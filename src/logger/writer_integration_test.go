// +build integration

package logger

import "testing"
import "io/ioutil"
import "os"

func TestWriteLargeRecordToTempFile(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(f.Name())
	w := NewRecordWriter(f, 0)

	writtenLen := writeFailOnError(t, w, make([]byte, 20*(blockSize-recordHeaderSize)))
	verifyWrittenLength(t, writtenLen, 20*blockSize)
}

func BenchmarkWriteLogToTempFile(b *testing.B) {
	f, err := ioutil.TempFile(os.TempDir(), b.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	w := NewRecordWriter(f, 0)

	record := make([]byte, 1023)
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(record); err != nil {
			b.Fatal(err)
		}
	}
}
