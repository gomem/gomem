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

// StructValueIterator iterates over the struct elements.
// {["f0:0" (null) "f0:9"] ["f1:0" (null) "f1:9"] [0 (null) 9]}
// It's a little different in that everything needs to be done from the field level.
type StructValueIterator struct {
	refCount      int64
	chunkIterator *ChunkIterator

	// Things we need to maintain for the iterator
	index int           // current field level value index
	ref   *array.Struct // the chunk reference
	done  bool          // there are no more elements for this iterator

	// We need iterators for each field
	fieldIterators []ValueIterator
	dataType       *arrow.StructType
	fieldNames     []string
}

func NewStructValueIterator(col *array.Column) *StructValueIterator {
	// We need a ChunkIterator to read the chunks
	chunkIterator := NewChunkIterator(col)

	dataType := col.DataType().(*arrow.StructType)
	fields := dataType.Fields()
	fieldNames := make([]string, len(fields))
	for i, field := range fields {
		fieldNames[i] = field.Name
	}

	return &StructValueIterator{
		refCount:      1,
		chunkIterator: chunkIterator,

		index: -1,
		ref:   nil,

		dataType:   dataType,
		fieldNames: fieldNames,
	}
}

// For this we return []ValueIterators so the user can do what they want with them.
func (vr *StructValueIterator) ValueInterface() interface{} {
	if vr.ref.IsNull(vr.index) {
		return nil
	}

	return vr.fieldIterators
}

// ValueAsJSON returns the current value as an interface{} in it's JSON representation.
func (vr *StructValueIterator) ValueAsJSON() (interface{}, error) {
	if vr.ref.IsNull(vr.index) {
		return nil, nil
	}

	obj := make(map[string]interface{})
	for i, fieldIterator := range vr.fieldIterators {
		jsonValue, err := fieldIterator.ValueAsJSON()
		if err != nil {
			return nil, err
		}
		obj[vr.fieldNames[i]] = jsonValue
	}

	return obj, nil
}

func (vr *StructValueIterator) DataType() arrow.DataType {
	return vr.dataType
}

func (vr *StructValueIterator) Next() bool {
	if vr.done {
		return false
	}

	// Keep moving the chunk up until we get one with data
	for vr.ref == nil || vr.advanceFieldIterators() {
		if !vr.nextChunk() {
			// There were no more chunks with data in them
			vr.done = true
			return false
		}
	}

	return true
}

func (vr *StructValueIterator) advanceFieldIterators() bool {
	vr.index++
	allItersDone := true
	for i := range vr.fieldIterators {
		itHasMore := vr.fieldIterators[i].Next()
		allItersDone = allItersDone && !itHasMore
	}
	return allItersDone
}

func (vr *StructValueIterator) nextChunk() bool {
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

	if vr.fieldIterators != nil {
		for i := range vr.fieldIterators {
			vr.fieldIterators[i].Release()
		}
	}

	vr.ref = ref.(*array.Struct)
	vr.index = -1

	// Create the field iterators
	vr.fieldIterators = make([]ValueIterator, vr.ref.NumField())
	for i := range vr.fieldIterators {
		vr.fieldIterators[i] = NewInterfaceValueIterator(vr.dataType.Field(i), vr.ref.Field(i))
	}

	return true
}

// Retain keeps a reference to the StructValueIterator
func (vr *StructValueIterator) Retain() {
	atomic.AddInt64(&vr.refCount, 1)
}

// Release removes a reference to the StructValueIterator
func (vr *StructValueIterator) Release() {
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

		if vr.fieldIterators != nil {
			for i := range vr.fieldIterators {
				vr.fieldIterators[i].Release()
			}
		}
	}
}
