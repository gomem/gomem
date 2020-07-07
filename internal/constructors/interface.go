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

package constructors

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/internal/cast"
	"fmt"
)

// NewInterfaceFromMem builds a new column from memory
// valid is an optional array of booleans. If not specified, all values are valid.
func NewInterfaceFromMem(mem memory.Allocator, name string, values interface{}, valid []bool) (array.Interface, *arrow.Field, error) {
	var arr array.Interface

	switch v := values.(type) {
	case []bool:
		bld := array.NewBooleanBuilder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []int8:
		bld := array.NewInt8Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []int16:
		bld := array.NewInt16Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []int32:
		bld := array.NewInt32Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []int64:
		bld := array.NewInt64Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []uint8:
		bld := array.NewUint8Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []uint16:
		bld := array.NewUint16Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []uint32:
		bld := array.NewUint32Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []uint64:
		bld := array.NewUint64Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []float32:
		bld := array.NewFloat32Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []float64:
		bld := array.NewFloat64Builder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []string:
		bld := array.NewStringBuilder(mem)
		defer bld.Release()

		bld.AppendValues(v, valid)
		arr = bld.NewArray()

	case []uint:
		bld := array.NewUint64Builder(mem)
		defer bld.Release()

		vs := make([]uint64, len(v))
		for i, e := range v {
			vs[i] = uint64(e)
		}

		bld.AppendValues(vs, valid)
		arr = bld.NewArray()

	case []int:
		bld := array.NewInt64Builder(mem)
		defer bld.Release()

		vs := make([]int64, len(v))
		for i, e := range v {
			vs[i] = int64(e)
		}

		bld.AppendValues(vs, valid)
		arr = bld.NewArray()

	case []interface{}:
		validDense := valid
		if len(validDense) == 0 {
			// build valid mask
			validDense = make([]bool, len(v))
			for idx, value := range v {
				validDense[idx] = value != nil
			}
		}
		ifaceDense, err := cast.DenseCollectionToInterface(v)
		if err != nil {
			return nil, nil, err
		}
		return NewInterfaceFromMem(mem, name, ifaceDense, validDense)

	// TODO(nickpoorman): Handle reflect.Map, and reflect.Struct

	default:
		err := fmt.Errorf("dataframe/interface: invalid data type for %q (%T)", name, v)
		return nil, nil, err
	}

	field := &arrow.Field{Name: name, Type: arr.DataType()}
	return arr, field, nil
}
