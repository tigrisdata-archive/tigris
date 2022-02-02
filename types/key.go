// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

type Key interface {
	Partition() []byte
	Primary() []byte
	String() string
}

type intKey struct {
	key       uint64
	bits      int16
	primary   []byte
	partition []byte
}

// NewIntKey create integer key, with pbits MSB used as partition key.
// Partition key is rounded to 8,16,32,64 bits
// Primary and partition key are serialized to fix-length big-endian binary
// representation to maintain numerical order when compared as strings
func NewIntKey(i uint64, pbits int16) Key {
	ik := &intKey{key: i, bits: pbits}

	ik.primary = make([]byte, 8)
	binary.BigEndian.PutUint64(ik.primary, ik.key)

	ik.partition = make([]byte, (pbits+7)/8)

	switch len(ik.partition) {
	case 1:
		ik.partition[0] = uint8(i >> 56)
	case 2:
		binary.BigEndian.PutUint16(ik.partition, uint16(ik.key>>48))
	case 3, 4:
		binary.BigEndian.PutUint32(ik.partition, uint32(ik.key>>32))
	default:
		binary.BigEndian.PutUint64(ik.partition, ik.key)
	}

	return ik
}

func (i *intKey) Partition() []byte {
	return i.partition
}

func (i *intKey) Primary() []byte {
	return i.primary
}

func (i *intKey) String() string {
	return fmt.Sprintf("%v %d", i.partition, i.key)
}

// EncodeIntKey encodes integer key
// Primary and partition key are serialized to fix-length big-endian binary
// representation to maintain numerical order when compared as strings
func EncodeIntKey(i uint64, res []byte) []byte {
	res = append(res, byte(i>>56))
	res = append(res, byte(i>>48))
	res = append(res, byte(i>>40))
	res = append(res, byte(i>>32))
	res = append(res, byte(i>>24))
	res = append(res, byte(i>>16))
	res = append(res, byte(i>>8))
	res = append(res, byte(i))
	return res
}

type binaryKey struct {
	key       []byte
	encoded   []byte
	partition []byte
}

// EncodeBinaryKey encodes binary key
// In order to support lexicographical order of variable length keys with
// arbitrary data in it we need to escape special symbol, which denotes end
// of the key. The special symbol is 0x0000
// We append every 0x00 in the user input with 0xFF, after this no 0x0000
// sequence is possible in the user input.
// pbits contains the number of MSBs of they key to be used as partition key
func EncodeBinaryKey(key []byte, res []byte) []byte {
	for i := 0; i < len(key); i++ {
		res = append(res, key[i])
		if key[i] == 0x00 {
			res = append(res, 0xFF)
		}
	}

	res = append(res, 0x00)
	res = append(res, 0x00)

	return res
}

// NewBinaryKey creates key which can contain any data.
// In order to support lexicographical order of variable length keys with
// arbitrary data in it we need to escape special symbol, which denotes end
// of the key. The special symbol is 0x0000
// We append every 0x00 in the user input with 0xFF, after this no 0x0000
// sequence is possible in the user input.
// pbits contains the number of MSBs of they key to be used as partition key
func NewBinaryKey(b []byte, pbits int16) Key {
	bk := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		bk = append(bk, b[i])
		if b[i] == 0x00 {
			bk = append(bk, 0xFF)
		}
	}
	bk = append(bk, 0x00)
	bk = append(bk, 0x00)
	partition := make([]byte, 0, (pbits+7)/8)
	i := 0
	for ; pbits > 8; pbits -= 8 {
		partition = append(partition, bk[i])
		i++
	}
	if pbits > 0 {
		partition = append(partition, (bk[i]>>(8-pbits))<<(8-pbits))
	}
	log.Debug().Bytes("key", b).Bytes("partition", partition).Msg("NewBinaryKey")
	return &binaryKey{key: b, encoded: bk, partition: partition}
}

func (b *binaryKey) Partition() []byte {
	return b.partition
}

func (b *binaryKey) Primary() []byte {
	return b.encoded
}

func (b *binaryKey) String() string {
	return fmt.Sprintf("%v %v", string(b.partition), string(b.key))
}

type stringKey struct {
	key       string
	partition []byte
}

// NewStringKey creates string key.
// String assumes that there is no 0x00 can be part of it
// pbits contains the number of MSBs of they key to be used as partition key
func NewStringKey(s string, pbits int16) Key {
	partition := make([]byte, 0, (pbits+7)/8)
	i := 0
	for pbits > 8 {
		partition = append(partition, s[i])
		pbits -= 8
	}
	if pbits > 0 {
		partition = append(partition, (s[i]>>(8-pbits))<<(8-pbits))
	}
	return &stringKey{key: s, partition: partition}
}

func (s *stringKey) Partition() []byte {
	return s.partition
}

func (s *stringKey) Primary() []byte {
	return []byte(s.key)
}

func (s *stringKey) String() string {
	return fmt.Sprintf("%v %s", s.partition, s.key)
}

type partitionKeyIterator interface {
	Next() []byte
}

type prefixPartitionIterator struct {
	cur  uint64
	bits int16
	curB []byte
}

func NewPrefixPartitionKeyIterator(first []byte, bits int16) (partitionKeyIterator, error) {
	p := &prefixPartitionIterator{bits: bits}
	bytes := (bits + 7) / 8
	if len(first) < int(bytes) {
		return nil, fmt.Errorf("key is shorter then prefix bits")
	}
	switch bytes {
	case 1:
		p.cur = uint64(first[0])
	case 2:
		p.cur = uint64(binary.BigEndian.Uint16(first))
	case 4:
		p.cur = uint64(binary.BigEndian.Uint16(first))
	case 8:
		p.cur = uint64(binary.BigEndian.Uint16(first))
	}
	p.curB = make([]byte, bytes)
	if bits%8 != 0 {
		p.cur >>= 8 - bits%8
	}
	return p, nil
}

func (p *prefixPartitionIterator) Next() []byte {
	p.cur += 1
	cur := p.cur
	if p.bits%8 != 0 {
		cur <<= 8 - p.bits%8
	}
	switch len(p.curB) {
	case 1:
		p.curB[0] = byte(cur)
	case 2:
		binary.BigEndian.PutUint16(p.curB, uint16(cur))
	case 4:
		binary.BigEndian.PutUint32(p.curB, uint32(cur))
	case 8:
		binary.BigEndian.PutUint64(p.curB, cur)
	}
	return p.curB
}
