package logger

import "encoding/binary"

const (
	// blockSize is the size of a block within the log.
	blockSize = 32 * 1024
	// recordHeaderSize is the size of the header for a record. Header has Checksum(uint32), Length(uint16) and RecordType (uint8)
	recordHeaderSize = 4 + 2 + 1
)

type recordType uint8

const (
	_ = iota
	// FULL is the type of record that contains the contents of an entire user record.
	FULL recordType = iota
	// FIRST is the type of record that contains first fragment of a user record.
	FIRST
	// MIDDLE is the type of record that contains middle fragments of a user record.
	MIDDLE
	// LAST is the type of record that contains last fragment of a user record.
	LAST
)

type header []byte

func newHeader() header {
	return make([]byte, 7)
}

func (h header) SetRecordType(rt recordType) {
	h[6] = byte(rt)
}

func (h header) RecordType() recordType {
	return recordType(h[6])
}

func (h header) RecordTypeByte() []byte {
	return h[6:7]
}

func (h header) SetLength(l uint16) {
	binary.LittleEndian.PutUint16(h[4:6], l)
}

func (h header) Length() uint16 {
	return binary.LittleEndian.Uint16(h[4:6])
}

func (h header) SetChecksum(checksum uint32) {
	binary.LittleEndian.PutUint32(h[0:4], checksum)
}

func (h header) Checksum() uint32 {
	return binary.LittleEndian.Uint32(h[0:4])
}
