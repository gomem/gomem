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

package cast

import "fmt"

const inconsistentDataTypesErrMsg = "inconsistent data types for elements, expecting %v to be of type (%T)"

// SparseCollectionToInterface casts a slice of interfaces to an interface of the correct type
// for the provided sparse collection.
// This should be used for sparse as it should be faster for larger arrays.
func SparseCollectionToInterface(elms []interface{}, indexes []int, size int) (interface{}, error) {
	if len(elms) == 0 {
		return nil, nil
	}

	// find the first one that is not nil
	var first interface{}
	for i := range elms {
		if elms[i] != nil {
			first = elms[i]
			break
		}
	}

	if first == nil {
		return nil, nil
	}

	var ok bool
	switch v := first.(type) {
	case bool:
		arr := make([]bool, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(bool); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int8:
		arr := make([]int8, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int8); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int16:
		arr := make([]int16, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int16); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int32:
		arr := make([]int32, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int32); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int64:
		arr := make([]int64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(int64); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint8:
		arr := make([]uint8, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint8); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint16:
		arr := make([]uint16, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint16); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint32:
		arr := make([]uint32, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint32); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint64:
		arr := make([]uint64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint64); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case float32:
		arr := make([]float32, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(float32); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case float64:
		arr := make([]float64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(float64); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case string:
		arr := make([]string, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(string); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case uint:
		arr := make([]uint, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			if arr[idx], ok = e.(uint); !ok {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
		}
		return arr, nil

	case int:
		arr := make([]int64, size)
		for i, idx := range indexes {
			e := elms[i]
			if e == nil {
				continue
			}
			vv, okk := e.(int)
			if !okk {
				return nil, fmt.Errorf(inconsistentDataTypesErrMsg, e, v)
			}
			arr[idx] = int64(vv)
		}
		return arr, nil

	default:
		return nil, fmt.Errorf("dataframe/sparse: invalid data type for %v (%T)", elms, v)
	}
}
