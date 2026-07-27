package object

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

func Test_toI64(t *testing.T) {
	cases := []struct {
		in   Decimal128
		want int64
	}{
		// https://github.com/apache/arrow/blob/eb8080e2a5946d7d279982be77be3eb138b04c53/cpp/src/arrow/util/decimal_test.cc#L424
		{NewDecimal128FromInt64(1234), 1234},
		{NewDecimal128FromInt64(-1234), -1234},
	}

	for _, c := range cases {
		got := c.in.toI64()
		if got != c.want {
			t.Errorf("%+v\ngot=%v\nwant=%v", c, got, c.want)
		}
	}
}

func Test_toU64(t *testing.T) {
	cases := []struct {
		in   Decimal128
		want uint64
	}{
		// https://github.com/apache/arrow/blob/eb8080e2a5946d7d279982be77be3eb138b04c53/cpp/src/arrow/util/decimal_test.cc#L424
		{NewDecimal128FromInt64(1234), 1234},
	}

	for _, c := range cases {
		got := c.in.toU64()
		if got != c.want {
			t.Errorf("%+v\ngot=%v\nwant=%v", c, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Decimal256
// ---------------------------------------------------------------------------

func TestNewDecimal256FromInt64(t *testing.T) {
	cases := []struct {
		in   int64
		want decimal256.Num
	}{
		{0, decimal256.Num{}},
		{1, decimal256.FromI64(1)},
		{-1, decimal256.FromI64(-1)},
		{123456789, decimal256.FromI64(123456789)},
	}
	for _, c := range cases {
		got := NewDecimal256FromInt64(Int64(c.in))
		if got.Value() != c.want {
			t.Errorf("NewDecimal256FromInt64(%d): got=%v, want=%v", c.in, got.Value(), c.want)
		}
	}
}

func TestNewDecimal256FromU64(t *testing.T) {
	got := NewDecimal256FromU64(Uint64(42))
	want := decimal256.FromU64(42)
	if got.Value() != want {
		t.Errorf("NewDecimal256FromU64(42): got=%v, want=%v", got.Value(), want)
	}
}

func TestDecimal256ToI64(t *testing.T) {
	cases := []struct {
		in   Decimal256
		want int64
	}{
		{NewDecimal256FromInt64(1234), 1234},
		{NewDecimal256FromInt64(-1234), -1234},
		{NewDecimal256FromInt64(0), 0},
	}
	for _, c := range cases {
		got := c.in.toI64()
		if got != c.want {
			t.Errorf("%+v\ngot=%v\nwant=%v", c, got, c.want)
		}
	}
}

func TestDecimal256ToU64(t *testing.T) {
	got := NewDecimal256FromInt64(1234).toU64()
	if want := uint64(1234); got != want {
		t.Errorf("toU64: got=%v, want=%v", got, want)
	}
}

func TestDecimal256Comparison(t *testing.T) {
	one := NewDecimal256FromInt64(1)
	two := NewDecimal256FromInt64(2)
	neg := NewDecimal256FromInt64(-1)

	cases := []struct {
		name string
		op   func(Object) (Boolean, error)
		lhs  Decimal256
		want bool
	}{
		{"1 < 2", one.Less, two, true},
		{"2 < 1", two.Less, one, false},
		{"1 < 1", one.Less, one, false},
		{"1 <= 2", one.LessEq, two, true},
		{"2 > 1", two.Greater, one, true},
		{"1 > 2", one.Greater, two, false},
		{"2 >= 1", two.GreaterEq, one, true},
		{"1 == 1", one.Eq, one, true},
		{"1 == 2", one.Eq, two, false},
		{"-1 < 1", neg.Less, one, true},
		{"1 > -1", one.Greater, neg, true},
	}
	for _, c := range cases {
		got, err := c.op(c.lhs)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", c.name, err)
		}
		if bool(got) != c.want {
			t.Errorf("%s: got=%v, want=%v", c.name, bool(got), c.want)
		}
	}
}

func TestDecimal256JSONRoundTrip(t *testing.T) {
	cases := []Decimal256{
		NewDecimal256FromInt64(0),
		NewDecimal256FromInt64(1),
		NewDecimal256FromInt64(-1),
		NewDecimal256FromInt64(9223372036854775807),  // max int64
		NewDecimal256FromU64(18446744073709551615),   // max uint64
		MaxDecimal256,                                // 2^255 - 1
	}
	for _, original := range cases {
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Marshal(%v): %v", original, err)
		}
		var roundTripped Decimal256
		if err := json.Unmarshal(data, &roundTripped); err != nil {
			t.Fatalf("Unmarshal(%s): %v", string(data), err)
		}
		eq, err := original.Eq(roundTripped)
		if err != nil {
			t.Fatalf("Eq after round-trip: %v", err)
		}
		if !bool(eq) {
			t.Errorf("round-trip mismatch: %v -> %s -> %v", original, string(data), roundTripped.Value())
		}
	}
}

// TestDecimal256CastFromPrimitives verifies the codegen-generated CastToDecimal256
// covers the implemented source types (Int64, Uint64, Boolean, decimal256.Num).
func TestDecimal256CastFromPrimitives(t *testing.T) {
	t.Run("Int64", func(t *testing.T) {
		got, ok := CastToDecimal256(Int64(7))
		if !ok {
			t.Fatal("expected ok")
		}
		want := NewDecimal256FromInt64(7)
		eq, _ := got.Eq(want)
		if !bool(eq) {
			t.Errorf("got=%v, want=%v", got, want)
		}
	})
	t.Run("Uint64", func(t *testing.T) {
		got, ok := CastToDecimal256(Uint64(9))
		if !ok {
			t.Fatal("expected ok")
		}
		want := NewDecimal256FromU64(9)
		eq, _ := got.Eq(want)
		if !bool(eq) {
			t.Errorf("got=%v, want=%v", got, want)
		}
	})
	t.Run("Boolean true", func(t *testing.T) {
		got, ok := CastToDecimal256(Boolean(true))
		if !ok {
			t.Fatal("expected ok")
		}
		want := NewDecimal256FromU64(1)
		eq, _ := got.Eq(want)
		if !bool(eq) {
			t.Errorf("got=%v, want=%v", got, want)
		}
	})
	t.Run("Boolean false", func(t *testing.T) {
		got, ok := CastToDecimal256(Boolean(false))
		if !ok {
			t.Fatal("expected ok")
		}
		want := NewDecimal256FromU64(0)
		eq, _ := got.Eq(want)
		if !bool(eq) {
			t.Errorf("got=%v, want=%v", got, want)
		}
	})
}
