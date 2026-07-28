package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

// TestCastElementCoversAllTypes ensures CastElement does not panic for any of
// the data types that have a generated Element implementation. This is a
// regression test for a gap where newly-added types (Decimal256, Binary,
// FixedSizeBinary, LargeString, MonthDayNanoInterval, and several pre-existing
// types) were unreachable from the dataframe equality/comparison path.
func TestCastElementCoversAllTypes(t *testing.T) {
	cases := []struct {
		name  string
		dtype arrow.DataType
		v     interface{}
	}{
		{"Boolean", arrow.FixedWidthTypes.Boolean, true},
		{"Decimal128", &arrow.Decimal128Type{Precision: 10, Scale: 1}, int32(1)},
		{"Decimal256", &arrow.Decimal256Type{Precision: 10, Scale: 0}, decimal256.FromI64(1)},
		{"Binary", arrow.BinaryTypes.Binary, []byte("x")},
		{"FixedSizeBinary", &arrow.FixedSizeBinaryType{ByteWidth: 1}, []byte{1}},
		{"String", arrow.BinaryTypes.String, "x"},
		{"LargeString", arrow.BinaryTypes.LargeString, "x"},
		{"StringView", arrow.BinaryTypes.StringView, "x"},
		{"BinaryView", arrow.BinaryTypes.BinaryView, []byte("x")},
		{"Float16", arrow.FixedWidthTypes.Float16, int16(1)},
		{"Time32", arrow.FixedWidthTypes.Time32s, int32(1)},
		{"Time64", arrow.FixedWidthTypes.Time64ns, int64(1)},
		{"Timestamp", arrow.FixedWidthTypes.Timestamp_s, int64(1)},
		{"Duration", arrow.FixedWidthTypes.Duration_s, int64(1)},
		{"MonthInterval", arrow.FixedWidthTypes.MonthInterval, int32(1)},
		{"DayTimeInterval", arrow.FixedWidthTypes.DayTimeInterval, arrow.DayTimeInterval{Days: 1}},
		{"MonthDayNanoInterval", arrow.FixedWidthTypes.MonthDayNanoInterval, arrow.MonthDayNanoInterval{Months: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Must not panic.
			el := CastElement(c.dtype, c.v)
			if el == nil {
				t.Fatalf("CastElement returned nil for %s", c.name)
			}
		})
	}
}
