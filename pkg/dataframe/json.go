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
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/gomem/gomem/pkg/metadata"
	"github.com/gomem/gomem/pkg/iterator"
)

// ToJSON writes the DataFrame as JSON.
// This will write newline delimited JSON with each line as a single record.
// This is equivaliant to Pandas to_json when you specify:
// orient='records' and lines=True.
func (df *DataFrame) ToJSON(w io.Writer) error {
	schema := df.Schema()
	fields := schema.Fields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	// TODO(nickpoorman): remove this once Map type is supported in Arrow
	// https://issues.apache.org/jira/browse/ARROW-5640
	isMap := make([]bool, len(fields))
	for i, field := range fields {
		isMap[i] = metadata.OriginalMapTypeMetadataExists(field.Metadata)
	}
	fmt.Printf("isMap: %v\n", isMap)

	// Iterate over the rows and extract one row at a time.
	it := iterator.NewStepIteratorForColumns(df.Columns())
	defer it.Release()

	enc := json.NewEncoder(w)

	for it.Next() {
		stepValue, err := it.ValuesJSON()
		if err != nil {
			return err
		}
		// At this point everything in stepValue is json.
		// We just have to build the object from it.
		jsonObj := make(map[string]interface{})
		for i, jsonValue := range stepValue.ValuesJSON {
			if isMap[i] {
				fmt.Printf("jsonValue: %#v\n", jsonValue)
				obj, ok := mergeKeyValuePairs(jsonValue)
				if !ok {
					fmt.Println("could not merge key value pairs")
					continue
				}
				jsonObj[names[i]] = obj
				continue
			}
			jsonObj[names[i]] = jsonValue
		}

		err = enc.Encode(jsonObj)
		if err != nil {
			return err
		}
	}

	return nil
}

func mergeKeyValuePairs(keyValuePairs interface{}) (*map[string]interface{}, bool) {
	if keyValuePairs == nil {
		return nil, true
	}
	kvps, ok := interfaceToArrayInterface(keyValuePairs)
	if !ok {
		fmt.Println("could not convert to array iface correctly")
		return nil, false
	}
	if kvps == nil {
		return nil, true
	}

	obj := make(map[string]interface{})
	for _, kvPairIface := range kvps {
		kvPair, ok := kvPairIface.(map[string]interface{})
		if !ok {
			fmt.Println("could not convert to map correctly")
			return nil, false
		}
		key, ok := kvPair["Key"].(string)
		if !ok {
			return nil, false
		}
		value := kvPair["Value"]
		obj[key] = value
	}
	return &obj, true
}

func interfaceToArrayInterface(a interface{}) ([]interface{}, bool) {
	v := reflect.ValueOf(a)
	if v.IsNil() {
		return nil, true
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		result := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			result[i] = v.Index(i).Interface()
		}
		return result, true
	default:
		return nil, false
	}
}
