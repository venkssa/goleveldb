package logger

import "bytes"

type OnlyOnceSeeker struct {
	alreadySeeked bool
}

func (s *OnlyOnceSeeker) ResetSeeker() {
	s.alreadySeeked = false
}

func (s *OnlyOnceSeeker) Seek(offset int64, whence int) (int64, error) {
	if s.alreadySeeked {
		panic("Should be seeked only once. But was called more than once")
	}
	s.alreadySeeked = true
	return offset, nil
}

type OnlyOnceSeekableBuffer struct {
	bytes.Buffer
	OnlyOnceSeeker
}

type DiscardAfterBuffer struct {
	OnlyOnceSeekableBuffer
	N int
}

func (b *DiscardAfterBuffer) Write(p []byte) (int, error) {
	last := len(p)
	if last > b.N {
		last = b.N
	}
	if b.N > 0 {
		n, _ := b.Buffer.Write(p[0:last])
		b.N -= n
	}
	return len(p), nil
}
