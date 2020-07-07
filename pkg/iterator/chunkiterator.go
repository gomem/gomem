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

// ChunkIterator is a generic iterator for reading an Arrow Column chunk by chunk.
type ChunkIterator struct {
	refCount int64
	col      *array.Column

	// Things Chunked maintains. We're going to maintain it ourselves.
	chunks []array.Interface // cache the chunks on this iterator
	length int64             // this isn't set right on Chunked so we won't rely on it there. Instead we keep the correct value here.
	nulls  int64
	dtype  arrow.DataType

	// Things we need to maintain for the iterator
	currentIndex int             // current chunk
	currentChunk array.Interface // current chunk
}

// NewChunkIterator creates a new ChunkIterator for reading an Arrow Column.
func NewChunkIterator(col *array.Column) *ChunkIterator {
	col.Retain()

	// Chunked is not using the correct type to keep track of length so we have to recalculate it.
	columnChunks := col.Data().Chunks()
	chunks := make([]array.Interface, len(columnChunks))
	var length int64
	var nulls int64

	for i, chunk := range columnChunks {
		// Retain the chunk
		chunk.Retain()

		// Keep our own refs to chunks
		chunks[i] = chunk

		// Keep our own counters instead of Chunked's
		length += int64(chunk.Len())
		nulls += int64(chunk.NullN())
	}

	return &ChunkIterator{
		refCount: 1,
		col:      col,

		chunks: chunks,
		length: length,
		nulls:  nulls,
		dtype:  col.DataType(),

		currentIndex: 0,
		currentChunk: nil,
	}
}

// Chunk will return the current chunk that the iterator is on.
func (cr *ChunkIterator) Chunk() array.Interface { return cr.currentChunk }

// Next moves the iterator to the next chunk. This will return false
// when there are no more chunks.
func (cr *ChunkIterator) Next() bool {
	if cr.currentIndex >= len(cr.chunks) {
		return false
	}

	if cr.currentChunk != nil {
		cr.currentChunk.Release()
	}

	cr.currentChunk = cr.chunks[cr.currentIndex]
	cr.currentChunk.Retain()
	cr.currentIndex++

	return true
}

// Retain keeps a reference to the ChunkIterator
func (cr *ChunkIterator) Retain() {
	atomic.AddInt64(&cr.refCount, 1)
}

// Release removes a reference to the ChunkIterator
func (cr *ChunkIterator) Release() {
	debug.Assert(atomic.LoadInt64(&cr.refCount) > 0, "too many releases")
	ref := atomic.AddInt64(&cr.refCount, -1)
	if ref == 0 {
		cr.col.Release()
		for i := range cr.chunks {
			cr.chunks[i].Release()
		}
		if cr.currentChunk != nil {
			cr.currentChunk.Release()
			cr.currentChunk = nil
		}
		cr.col = nil
		cr.chunks = nil
		cr.dtype = nil
	}
}
