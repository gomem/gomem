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

package smartbuilder

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/gomem/gomem/internal/debug"
)

// SmartBuilder knows how to convert to the correct type when building.
type SmartBuilder struct {
	recordBuilder *array.RecordBuilder
}

// NewSmartBuilder creates a SmartBuilder that knows how to convert to the correct type when building.
func NewSmartBuilder(recordBuilder *array.RecordBuilder) *SmartBuilder {
	// lenFields := len(recordBuilder.Fields())
	sb := &SmartBuilder{
		recordBuilder: recordBuilder,
	}

	return sb
}

func (sb *SmartBuilder) Append(fieldIndex int, v interface{}) error {
	builder := sb.recordBuilder.Field(fieldIndex)
	debug.Assert(builder != nil, "Append/builder is nil")
	if v == nil {
		builder.AppendNull()
		return nil
	}
	return sb.appendValue(builder, v)
}

// If the type of v is a pointer return the pointer as a value,
// otherwise create a new pointer to the value.
// func reflectValueOfNonPointer(v interface{}) reflect.Value {
// 	var ptr reflect.Value
// 	value := reflect.ValueOf(v)
// 	if value.Type().Kind() == reflect.Ptr {
// 		ptr = value
// 	} else {
// 		ptr = reflect.New(reflect.TypeOf(v)) // create new pointer
// 		temp := ptr.Elem()                   // create variable to value of pointer
// 		temp.Set(value)                      // set value of variable to our passed in value
// 	}
// 	return ptr
// }
