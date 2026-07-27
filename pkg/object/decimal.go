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

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
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

// -----------------------------------------------------------------------------
// Decimal256 helpers
//
// Apache Arrow for Go's decimal256.Num is a 256-bit signed integer backed by
// four little-endian uint64 limbs (arr[0] is the lowest bits). The generated
// Object type (type Decimal256 decimal256.Num) needs the same hand-written
// helpers as Decimal128: constructor helpers used by the codegen casts, signed
// extraction helpers (toI64/toU64) used by the Decimal256 -> primitive casts,
// and a stable JSON representation used by the iterator/dataframe JSON path.
// -----------------------------------------------------------------------------

// NewDecimal256FromInt64 returns a new signed 256-bit integer value from the provided int64 one.
func NewDecimal256FromInt64(v Int64) Decimal256 {
	return Decimal256(decimal256.FromI64(v.Value()))
}

// NewDecimal256FromU64 returns a new signed 256-bit integer value from the provided uint64 one.
func NewDecimal256FromU64(v Uint64) Decimal256 {
	return Decimal256(decimal256.FromU64(v.Value()))
}

func (e Decimal256) toI64() int64 {
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
func (e Decimal256) toU64() uint64 {
	return e.Value().LowBits()
}

func (e Decimal256) LowBits() uint64 {
	return e.Value().LowBits()
}

func (e Decimal256) Sign() int {
	return e.Value().Sign()
}

// MarshalJSON renders a Decimal256 as its four little-endian limbs so the value
// round-trips through JSON without loss (mirroring the lo/hi encoding used for
// Decimal128). l0 holds the lowest bits and l3 the highest.
func (e Decimal256) MarshalJSON() ([]byte, error) {
	arr := e.Value().Array()
	return json.Marshal(&struct {
		L0 uint64 `json:"l0"`
		L1 uint64 `json:"l1"`
		L2 uint64 `json:"l2"`
		L3 uint64 `json:"l3"`
	}{
		L0: arr[0],
		L1: arr[1],
		L2: arr[2],
		L3: arr[3],
	})
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (e *Decimal256) UnmarshalJSON(data []byte) error {
	aux := &struct {
		L0 uint64 `json:"l0"`
		L1 uint64 `json:"l1"`
		L2 uint64 `json:"l2"`
		L3 uint64 `json:"l3"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	// decimal256.New(x1, x2, x3, x4) takes the highest bits first.
	*e = Decimal256(decimal256.New(aux.L3, aux.L2, aux.L1, aux.L0))
	return nil
}
