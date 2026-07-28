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

// BinaryViewValueIterator is an iterator for reading an Arrow Column value by
// value for binary_view values. Like Binary it is hand-written because
// array.BinaryView exposes Value(i) but no bulk Values() accessor.
type BinaryViewValueIterator struct {
	refCount      int64
	chunkIterator *ChunkIterator

	index int             // current value index
	ref   *array.BinaryView // the chunk reference
	done  bool

	dataType arrow.DataType
}

// NewBinaryViewValueIterator creates a new BinaryViewValueIterator.
func NewBinaryViewValueIterator(col *arrow.Column) *BinaryViewValueIterator {
	return &BinaryViewValueIterator{
		refCount:      1,
		chunkIterator: NewChunkIterator(col),
		dataType:      col.DataType(),
	}
}

func (vr *BinaryViewValueIterator) Value() ([]byte, bool) {
	return vr.ref.Value(vr.index), vr.ref.IsNull(vr.index)
}

func (vr *BinaryViewValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}
	return vr.ref.Value(vr.index)
}

func (vr *BinaryViewValueIterator) ValueAsJSON() (interface{}, error) {
	if vr.ref.IsNull(vr.index) {
		return nil, nil
	}
	return binaryViewAsJSON(vr.ref.Value(vr.index))
}

func (vr *BinaryViewValueIterator) DataType() arrow.DataType { return vr.dataType }

func (vr *BinaryViewValueIterator) Next() bool {
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

func (vr *BinaryViewValueIterator) nextChunk() bool {
	if !vr.chunkIterator.Next() {
		return false
	}
	ref := vr.chunkIterator.Chunk()
	ref.Retain()
	if vr.ref != nil {
		vr.ref.Release()
	}
	vr.ref = ref.(*array.BinaryView)
	vr.index = 0
	return true
}

func (vr *BinaryViewValueIterator) Retain() { atomic.AddInt64(&vr.refCount, 1) }

func (vr *BinaryViewValueIterator) Release() {
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
