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
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/gomem/gomem/pkg/object"
)

// Special conversions for asjson are implemented here
// TODO(nickpoorman): Write tests for all of these.

// TODO(nickpoorman): Generate these from ojbects.tmpldata

func int64AsJSON(v interface{}) (interface{}, error) {
	// TODO(nickpoorman): JSON doesn't support 64 bit integers.
	// https://issues.apache.org/jira/browse/ARROW-6517?filter=12346179
	// strconv.FormatInt(v.(int64), 10)
	return v, nil
}

func uint64AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func float64AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func int32AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func uint32AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func float32AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func int16AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func uint16AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func int8AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func uint8AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func timestampAsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func time32AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func time64AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func date32AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func date64AsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func durationAsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func monthIntervalAsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func float16AsJSON(v interface{}) (interface{}, error) {
	return v.(float16.Num).Float32(), nil
}

func decimal128AsJSON(v interface{}) (interface{}, error) {
	d128 := v.(decimal128.Num)
	// TODO: cleanup (remove)
	// return types.Signed128BitInteger{Lo: d128.LowBits(), Hi: d128.HighBits()}, nil
	return object.Decimal128(d128), nil
}

func dayTimeIntervalAsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func nullAsJSON(v interface{}) (interface{}, error) {
	return nil, nil
}

func booleanAsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

func stringAsJSON(v interface{}) (interface{}, error) {
	return v, nil
}

// func fixedSizeBinary(v interface{}) (interface{}, error) {
// 	// TODO(nickpoorman): Verify this is correct....
// 	// dt := dtype.(*arrow.FixedSizeBinaryType)
// 	// v := []byte(strings.ToUpper(hex.EncodeToString([]byte{value.(byte)})))
// 	// if len(v) != 2*dt.ByteWidth {
// 	// 	return nil, fmt.Errorf("dataframe/json: invalid hex-string length (got=%d, want=%d)", len(v), 2*dt.ByteWidth)
// 	// }
// 	// return string(v), nil // re-convert as string to prevent json.Marshal from base64-encoding it.
// 	panic("not yet implemented")
// }
