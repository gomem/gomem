package object

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestMonthDayNanoIntervalConstruction(t *testing.T) {
	v := arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3}
	got := NewMonthDayNanoInterval(v)
	if got.Value() != v {
		t.Errorf("got=%v, want=%v", got.Value(), v)
	}
}

func TestMonthDayNanoIntervalComparison(t *testing.T) {
	a := NewMonthDayNanoInterval(arrow.MonthDayNanoInterval{Months: 1, Days: 0, Nanoseconds: 0})
	b := NewMonthDayNanoInterval(arrow.MonthDayNanoInterval{Months: 2, Days: 0, Nanoseconds: 0})
	c := NewMonthDayNanoInterval(arrow.MonthDayNanoInterval{Months: 1, Days: 5, Nanoseconds: 0})
	same := NewMonthDayNanoInterval(arrow.MonthDayNanoInterval{Months: 1, Days: 0, Nanoseconds: 0})

	cases := []struct {
		name string
		op   func(Object) (Boolean, error)
		lhs  MonthDayNanoInterval
		want bool
	}{
		{"a == same", same.Eq, a, true},
		{"a == b", b.Eq, a, false},
		{"a < b", a.Less, b, true},
		{"a < c (days tiebreak)", a.Less, c, true},
		{"c > a (days tiebreak)", c.Greater, a, true},
		{"b >= a", b.GreaterEq, a, true},
		{"a <= b", a.LessEq, b, true},
	}
	for _, tc := range cases {
		got, err := tc.op(tc.lhs)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if bool(got) != tc.want {
			t.Errorf("%s: got=%v, want=%v", tc.name, bool(got), tc.want)
		}
	}
}

func TestMonthDayNanoIntervalCastFromPrimitives(t *testing.T) {
	t.Run("native", func(t *testing.T) {
		v := arrow.MonthDayNanoInterval{Months: 3, Days: 4, Nanoseconds: 5}
		got, ok := CastToMonthDayNanoInterval(v)
		if !ok {
			t.Fatal("expected ok")
		}
		if got.Value() != v {
			t.Errorf("got=%v, want=%v", got.Value(), v)
		}
	})
	t.Run("Boolean true", func(t *testing.T) {
		got, ok := CastToMonthDayNanoInterval(Boolean(true))
		if !ok {
			t.Fatal("expected ok")
		}
		want := arrow.MonthDayNanoInterval{Months: 1}
		if got.Value() != want {
			t.Errorf("got=%v, want=%v", got.Value(), want)
		}
	})
}
