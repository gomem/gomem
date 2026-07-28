// Copyright 2019 Nick Poorman
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

import (
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gomem/gomem/internal/debug"
)

// BinaryValueIterator is an iterator for reading an Arrow Column
// value by value for variable-length binary ([]byte) values.
//
// Apache Arrow's array.Binary exposes a per-index Value(i) accessor but no
// bulk Values() method, so (like String) it is excluded from the codegen
// value/chunk iterators and hand-written here against the generic ChunkIterator.
type BinaryValueIterator struct {
	refCount      int64
	chunkIterator *ChunkIterator

	// Things we need to maintain for the iterator
	index int          // current value index
	ref   *array.Binary // the chunk reference
	done  bool         // there are no more elements for this iterator

	dataType arrow.DataType
}

// NewBinaryValueIterator creates a new BinaryValueIterator for reading an Arrow Column.
func NewBinaryValueIterator(col *arrow.Column) *BinaryValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewChunkIterator(col)

	return &BinaryValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index: 0,
		ref:   nil,

		dataType: col.DataType(),
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *BinaryValueIterator) Value() ([]byte, bool) {
	return vr.ref.Value(vr.index), vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *BinaryValueIterator) ValuePointer() *[]byte {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	value := vr.ref.Value(vr.index)
	return &value
}

// ValueInterface returns the value as an interface{}.
func (vr *BinaryValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.ref.Value(vr.index)
}

// ValueAsJSON returns the current value as an interface{} in it's JSON representation.
func (vr *BinaryValueIterator) ValueAsJSON() (interface{}, error) {
	if vr.ref.IsNull(vr.index) {
		return nil, nil
	}
	return binaryAsJSON(vr.ref.Value(vr.index))
}

func (vr *BinaryValueIterator) DataType() arrow.DataType {
	return vr.dataType
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *BinaryValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Move the index up
	vr.index++

	// Keep moving the chunk up until we get one with data
	for vr.ref == nil || vr.index >= vr.ref.Len() {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *BinaryValueIterator) nextChunk() bool {
	// Advance the chunk until we get one with data in it or we are done
	if !vr.chunkIterator.Next() {
		// No more chunks
		return false
	}

	// There was another chunk.
	// We maintain the ref and the values because the ref is going to allow us to retain the memory.
	ref := vr.chunkIterator.Chunk()
	ref.Retain()

	if vr.ref != nil {
		vr.ref.Release()
	}

	vr.ref = ref.(*array.Binary)
	vr.index = 0
	return true
}

// Retain keeps a reference to the BinaryValueIterator
func (vr *BinaryValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the BinaryValueIterator
func (vr *BinaryValueIterator) Release() {
	debug.Assert(atomic.LoadInt64(&vr.refCount) > 0, "too many releases")

	if atomic.AddInt64(&vr.refCount, -1) == 0 {
		if vr.chunkIterator != nil {
			vr.chunkIterator.Release()
			vr.chunkIterator = nil
		}

		if vr.ref != nil {
			vr.ref.Release()
			vr.ref = nil
		}
	}
}
