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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/gomem/gomem/internal/debug"
)

// StringValueIterator is an iterator for reading an Arrow Column
// value by value for variable-length UTF-8 strings.
type StringValueIterator struct {
	refCount      int64
	chunkIterator *ChunkIterator

	// Things we need to maintain for the iterator
	index int           // current value index
	ref   *array.String // the chunk reference
	done  bool          // there are no more elements for this iterator

	dataType arrow.DataType
}

// NewStringValueIterator creates a new StringValueIterator for reading an Arrow Column.
func NewStringValueIterator(col *array.Column) *StringValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewChunkIterator(col)

	return &StringValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index: 0,
		ref:   nil,

		dataType: col.DataType(),
	}
}

// Value will return the current value that the iterator is on and boolean value indicating if the value is actually null.
func (vr *StringValueIterator) Value() (string, bool) {
	return vr.ref.Value(vr.index), vr.ref.IsNull(vr.index)
}

// ValuePointer will return a pointer to the current value that the iterator is on. It will return nil if the value is actually null.
func (vr *StringValueIterator) ValuePointer() *string {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	value := vr.ref.Value(vr.index)
	return &value
}

// ValueInterface returns the value as an interface{}.
func (vr *StringValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.ref.Value(vr.index)
}

// ValueAsJSON returns the current value as an interface{} in it's JSON representation.
func (vr *StringValueIterator) ValueAsJSON() (interface{}, error) {
	if vr.ref.IsNull(vr.index) {
		return nil, nil
	}
	return stringAsJSON(vr.ref.Value(vr.index))
}

func (vr *StringValueIterator) DataType() arrow.DataType {
	return vr.dataType
}

// Next moves the iterator to the next value. This will return false
// when there are no more values.
func (vr *StringValueIterator) Next() bool {
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

func (vr *StringValueIterator) nextChunk() bool {
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

	vr.ref = ref.(*array.String)
	vr.index = 0
	return true
}

// Retain keeps a reference to the StringValueIterator
func (vr *StringValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the StringValueIterator
func (vr *StringValueIterator) Release() {
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
