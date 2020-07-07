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

package object

import (
	"encoding/json"

	"github.com/apache/arrow/go/arrow/decimal128"
)

// NewDecimal128FromI64 returns a new signed 128-bit integer value from the provided int64 one.
func NewDecimal128FromInt64(v Int64) Decimal128 {
	return Decimal128(decimal128.FromI64(v.Value()))
}

// NewDecimal128FromU64 returns a new signed 128-bit integer value from the provided uint64 one.
func NewDecimal128FromU64(v Uint64) Decimal128 {
	return Decimal128(decimal128.FromU64(v.Value()))
}

func (e Decimal128) toI64() int64 {
	sign := e.Sign()
	switch {
	case sign > 0:
		return int64(e.LowBits())
	case sign < 0:
		return int64(e.LowBits())
	default:
		return 0
	}
}

// toU64 is an alias for LowBits()
func (e Decimal128) toU64() uint64 {
	return e.Value().LowBits()
}

func (e Decimal128) LowBits() uint64 {
	return e.Value().LowBits()
}

func (e Decimal128) HighBits() int64 {
	return e.Value().HighBits()
}

func (e Decimal128) Sign() int {
	return e.Value().Sign()
}

func (e Decimal128) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Lo uint64 `json:"lo"`
		Hi int64  `json:"hi"`
	}{
		Lo: e.Value().LowBits(),
		Hi: e.Value().HighBits(),
	})
}

func (e *Decimal128) UnmarshalJSON(data []byte) error {
	aux := &struct {
		Lo uint64 `json:"lo"`
		Hi int64  `json:"hi"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*e = Decimal128(decimal128.New(aux.Hi, aux.Lo))
	return nil
}
