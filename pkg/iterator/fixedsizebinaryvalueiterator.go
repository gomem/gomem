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

// FixedSizeBinaryValueIterator is an iterator for reading an Arrow Column
// value by value for fixed-width binary ([]byte) values. Like Binary/String it
// is hand-written because array.FixedSizeBinary exposes Value(i) but no bulk
// Values() accessor.
type FixedSizeBinaryValueIterator struct {
	refCount      int64
	chunkIterator *ChunkIterator

	index int                   // current value index
	ref   *array.FixedSizeBinary // the chunk reference
	done  bool

	dataType arrow.DataType
}

// NewFixedSizeBinaryValueIterator creates a new FixedSizeBinaryValueIterator for reading an Arrow Column.
func NewFixedSizeBinaryValueIterator(col *arrow.Column) *FixedSizeBinaryValueIterator {
	return &FixedSizeBinaryValueIterator{
		refCount:      1,
		chunkIterator: NewChunkIterator(col),
		dataType:      col.DataType(),
	}
}

// Value returns the current value and whether it is null.
func (vr *FixedSizeBinaryValueIterator) Value() ([]byte, bool) {
	return vr.ref.Value(vr.index), vr.ref.IsNull(vr.index)
}

// ValuePointer returns a pointer to the current value, or nil if it is null.
func (vr *FixedSizeBinaryValueIterator) ValuePointer() *[]byte {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	value := vr.ref.Value(vr.index)
	return &value
}

// ValueInterface returns the value as an interface{}.
func (vr *FixedSizeBinaryValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.ref.Value(vr.index)
}

// ValueAsJSON returns the current value in its JSON representation.
func (vr *FixedSizeBinaryValueIterator) ValueAsJSON() (interface{}, error) {
	if vr.ref.IsNull(vr.index) {
		return nil, nil
	}
	return fixedSizeBinaryAsJSON(vr.ref.Value(vr.index))
}

func (vr *FixedSizeBinaryValueIterator) DataType() arrow.DataType { return vr.dataType }

// Next moves the iterator to the next value. Returns false when there are no more values.
func (vr *FixedSizeBinaryValueIterator) Next() bool {
	if vr.done {
		return false
	}
	vr.index++
	for vr.ref == nil || vr.index >= vr.ref.Len() {
		if !vr.nextChunk() {
			vr.done = true
			return false
		}
	}
	return true
}

func (vr *FixedSizeBinaryValueIterator) nextChunk() bool {
	if !vr.chunkIterator.Next() {
		return false
	}
	ref := vr.chunkIterator.Chunk()
	ref.Retain()
	if vr.ref != nil {
		vr.ref.Release()
	}
	vr.ref = ref.(*array.FixedSizeBinary)
	vr.index = 0
	return true
}

// Retain keeps a reference to the FixedSizeBinaryValueIterator
func (vr *FixedSizeBinaryValueIterator) Retain() { atomic.AddInt64(&vr.refCount, 1) }

// Release removes a reference to the FixedSizeBinaryValueIterator
func (vr *FixedSizeBinaryValueIterator) Release() {
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
