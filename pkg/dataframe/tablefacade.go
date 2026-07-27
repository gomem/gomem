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

package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// I don't want to force the DataFrame API to conform to the TableReader API.
// (i.e. forcing NumCols to return int64 doesn't make sense in Go).
// So this is a facade the DataFrame TableReader expects.

// TableFacade is a simple facade for a TableReader.
type TableFacade interface {
	arrow.Table
}

type tableReaderFacade struct {
	df *DataFrame
}

// NewTableFacade creates a new TableFacade for a DataFrame.
func NewTableFacade(df *DataFrame) TableFacade {
	return &tableReaderFacade{
		df: df,
	}
}

func (f *tableReaderFacade) Schema() *arrow.Schema {
	return f.df.Schema()
}

func (f *tableReaderFacade) NumRows() int64 {
	return f.df.NumRows()
}

func (f *tableReaderFacade) NumCols() int64 {
	return int64(f.df.NumCols())
}

// Column is an immutable column data structure consisting of
// a field (type metadata) and a chunked data array.
func (f *tableReaderFacade) Column(i int) *arrow.Column {
	return f.df.ColumnAt(i)
}

// AddColumn satisfies the arrow.Table interface. It builds a brand-new table
// (an *array.Table) composed of the facade's current columns with the provided
// column and field inserted at the requested position. The returned table owns
// its own reference counts (array.NewTable retains every column it is given),
// so the caller is responsible for releasing it when done.
func (f *tableReaderFacade) AddColumn(pos int, field arrow.Field, col arrow.Column) (arrow.Table, error) {
	curCols := f.df.Columns()
	n := len(curCols)
	if pos < 0 || pos > n {
		return nil, fmt.Errorf("dataframe: invalid column position %d (have %d columns)", pos, n)
	}

	cols := make([]arrow.Column, 0, n+1)
	cols = append(cols, curCols[:pos]...)
	cols = append(cols, col)
	cols = append(cols, curCols[pos:]...)

	curFields := f.df.Schema().Fields()
	fields := make([]arrow.Field, 0, len(curFields)+1)
	fields = append(fields, curFields[:pos]...)
	fields = append(fields, field)
	fields = append(fields, curFields[pos:]...)
	schema := arrow.NewSchema(fields, nil)

	return array.NewTable(schema, cols, f.NumRows()), nil
}

func (f *tableReaderFacade) Retain() {
	f.df.Retain()
}

func (f *tableReaderFacade) Release() {
	f.df.Release()
}

// String satisfies the fmt.Stringer interface required by arrow.Table.
func (f *tableReaderFacade) String() string {
	return fmt.Sprintf("gomem dataframe table: %d cols x %d rows\n%s", f.NumCols(), f.NumRows(), f.df.Schema())
}
