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

package dataframe_test

import (
	"fmt"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/dataframe"
)

// This example demonstrates creating a new DataFrame from memory
// using a Dict and displaying the contents of it.
func Example_newDataFrameFromMemory() {
	pool := memory.NewGoAllocator()
	df, _ := dataframe.NewDataFrameFromMem(pool, dataframe.Dict{
		"col1": []int32{1, 2, 3, 4, 5},
		"col2": []float64{1.1, 2.2, 3.3, 4.4, 5},
		"col3": []string{"foo", "bar", "ping", "", "pong"},
		"col4": []interface{}{2, 4, 6, nil, 8},
	})
	defer df.Release()
	fmt.Printf("DataFrame:\n%s\n", df.Display(0))

	// Output:
	// DataFrame:
	// rec[0]["col1"]: [1 2 3 4 5]
	// rec[0]["col2"]: [1.1 2.2 3.3 4.4 5]
	// rec[0]["col3"]: ["foo" "bar" "ping" "" "pong"]
	// rec[0]["col4"]: [2 4 6 (null) 8]
}
