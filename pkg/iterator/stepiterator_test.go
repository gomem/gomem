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

package iterator_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/iterator"
)

func TestNewStepIteratorForColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	records, schema := buildRecords(pool, t)
	for i := range records {
		defer records[i].Release()
	}

	tbl := array.NewTableFromRecords(schema, records)
	defer tbl.Release()

	cols := make([]array.Column, 0, tbl.NumCols())
	for i := 0; i < int(tbl.NumCols()); i++ {
		cols = append(cols, *tbl.Column(i))
	}

	it := iterator.NewStepIteratorForColumns(cols)
	defer it.Release()
}
