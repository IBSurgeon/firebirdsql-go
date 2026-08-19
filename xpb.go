/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2023-2024 Artyom Smirnov <artyom_smirnov@me.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import "fmt"

const xpbPreallocBufSize = 16

// XPBReader is a cursor over a Firebird parameter block. The buffer may be
// server-supplied (e.g. a service-info response), so every read is
// bounds-checked: a read past the end consumes the rest of the buffer,
// returns zero, and sets a sticky error. Callers that surface a read value
// must check Err() — like bufio.Scanner, the cursor keeps returning zero
// once a read fails, so Next()/End() loops terminate.
type XPBReader struct {
	buf []byte
	pos int
	err error
}

type XPBWriter struct {
	buf []byte
}

func NewXPBReader(buf []byte) *XPBReader {
	return &XPBReader{buf: buf}
}

// fail records the first out-of-bounds read and parks the cursor at the end so
// later Next()/End() calls terminate any consumer loop.
func (pb *XPBReader) fail(format string, args ...any) {
	if pb.err == nil {
		pb.err = fmt.Errorf("firebirdsql: "+format, args...)
	}
	pb.pos = len(pb.buf)
}

// Err returns the first bounds violation encountered, or nil.
func (pb *XPBReader) Err() error {
	return pb.err
}

func (pb *XPBReader) Next() (have bool, value byte) {
	if pb.End() {
		return false, 0
	}
	b := pb.buf[pb.pos]
	pb.pos++
	return true, b
}

func (pb *XPBReader) Skip(count int) {
	if count < 0 || pb.pos+count > len(pb.buf) {
		pb.fail("parameter block skip of %d out of range", count)
		return
	}
	pb.pos += count
}

func (pb *XPBReader) End() bool {
	return pb.pos >= len(pb.buf)
}

func (pb *XPBReader) Get() byte {
	if pb.pos >= len(pb.buf) {
		pb.fail("parameter block read past end")
		return 0
	}
	return pb.buf[pb.pos]
}

func (pb *XPBReader) GetString() string {
	l := int(pb.GetInt16())
	if l < 0 || pb.pos+l > len(pb.buf) {
		pb.fail("parameter block string length %d out of range", l)
		return ""
	}
	s := bytes_to_str(pb.buf[pb.pos : pb.pos+l])
	pb.pos += l
	return s
}

func (pb *XPBReader) GetInt16() int16 {
	if pb.pos+2 > len(pb.buf) {
		pb.fail("parameter block int16 read past end")
		return 0
	}
	r := bytes_to_int16(pb.buf[pb.pos : pb.pos+2])
	pb.pos += 2
	return r
}

func (pb *XPBReader) GetInt32() int32 {
	if pb.pos+4 > len(pb.buf) {
		pb.fail("parameter block int32 read past end")
		return 0
	}
	r := bytes_to_int32(pb.buf[pb.pos : pb.pos+4])
	pb.pos += 4
	return r
}

func (pb *XPBReader) GetInt64() int64 {
	if pb.pos+8 > len(pb.buf) {
		pb.fail("parameter block int64 read past end")
		return 0
	}
	r := bytes_to_int64(pb.buf[pb.pos : pb.pos+8])
	pb.pos += 8
	return r
}

func (pb *XPBReader) Reset() {
	pb.pos = 0
	pb.err = nil
}

func NewXPBWriter() *XPBWriter {
	return &XPBWriter{
		buf: make([]byte, 0, xpbPreallocBufSize),
	}
}

func NewXPBWriterFromTag(tag byte) *XPBWriter {
	return NewXPBWriter().PutTag(tag)
}

func NewXPBWriterFromBytes(bytes []byte) *XPBWriter {
	return NewXPBWriter().PutBytes(bytes)
}

func (pb *XPBWriter) PutTag(tag byte) *XPBWriter {
	pb.buf = append(pb.buf, []byte{tag}...)
	return pb
}

func (pb *XPBWriter) PutByte(tag byte, val byte) *XPBWriter {
	pb.buf = append(pb.buf, []byte{tag, val}...)
	return pb
}

func (pb *XPBWriter) PutInt16(tag byte, val int16) *XPBWriter {
	pb.buf = append(pb.buf, []byte{tag}...)
	pb.buf = append(pb.buf, int16_to_bytes(val)...)
	return pb
}

func (pb *XPBWriter) PutInt32(tag byte, val int32) *XPBWriter {
	pb.buf = append(pb.buf, []byte{tag}...)
	pb.buf = append(pb.buf, int32_to_bytes(val)...)
	return pb
}

func (pb *XPBWriter) PutInt64(tag byte, val int64) *XPBWriter {
	pb.buf = append(pb.buf, []byte{tag}...)
	pb.buf = append(pb.buf, int64_to_bytes(val)...)
	return pb
}

func (pb *XPBWriter) PutString(tag byte, val string) *XPBWriter {
	strBytes := str_to_bytes(val)
	pb.buf = append(pb.buf, []byte{tag}...)
	pb.buf = append(pb.buf, int16_to_bytes(int16(len(strBytes)))...)
	pb.buf = append(pb.buf, strBytes...)
	return pb
}

func (pb *XPBWriter) PutBytes(bytes []byte) *XPBWriter {
	pb.buf = append(pb.buf, bytes...)
	return pb
}

func (pb *XPBWriter) Bytes() []byte {
	return pb.buf
}

func (pb *XPBWriter) Reset() *XPBWriter {
	pb.buf = make([]byte, 0, xpbPreallocBufSize)
	return pb
}
