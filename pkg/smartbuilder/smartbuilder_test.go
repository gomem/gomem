package smartbuilder

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/gomem/gomem/pkg/metadata"
)

func TestNewSmartBuilderTypes(t *testing.T) {
	for _, testCase := range GenerateSmartBuilderTestCases() {
		func() {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			got, err := buildRecord(pool, testCase.Dtype, testCase.Values)
			if err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(got, testCase.Want) {
				t.Errorf("\ngot=\n%v\nwant=\n%v", got, testCase.Want)
			}
		}()
	}
}

func buildRecord(pool *memory.CheckedAllocator, dtype arrow.DataType, vals []interface{}) (string, error) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: fmt.Sprintf("col-%s", dtype.Name()), Type: dtype},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	smartBuilder := NewSmartBuilder(b)
	for i := range schema.Fields() {
		for j := range vals {
			if err := smartBuilder.Append(i, vals[j]); err != nil {
				return "", fmt.Errorf("build df: %w", err)
			}
		}
		if err := smartBuilder.Append(i, nil); err != nil {
			return "", fmt.Errorf("build df: %w", err)
		}
	}

	rec1 := b.NewRecord()
	defer rec1.Release()

	return fmt.Sprintf("%v", rec1), nil
}

type keyValue struct {
	Key   interface{}
	Value interface{}
}

// convert {"foo": "bar", "ping": 0} to [{"Key": "foo", "Value": "bar"}, {"Key": "ping", "Value": 0}]
func convertMapToKeyValueTuples(m reflect.Value) reflect.Value {
	length := m.Len()
	list := make([]keyValue, 0, length)
	iter := m.MapRange()
	for iter.Next() {
		k := iter.Key()   // foo
		v := iter.Value() // bar
		list = append(list, keyValue{Key: k.Interface(), Value: v.Interface()})
	}
	// The iterator over the map key values is not stable so we sort to keep tests consistent
	sort.SliceStable(list, func(i, j int) bool { return list[i].Key.(string) < list[j].Key.(string) })
	return reflect.ValueOf(list)
}

func addMapAsListOfStructsUsingSmartBuilder(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()

	data := []map[string]float64{
		{"field_a": float64(0), "field_b": float64(0), "field_c": float64(0)},
		nil,
		{"field_a": float64(2), "field_b": float64(2), "field_c": float64(2)},
		{"field_a": float64(3), "field_b": float64(3), "field_c": float64(3)},
		{"field_a": float64(4), "field_b": float64(4), "field_c": float64(4)},
	}

	smartBuilder := NewSmartBuilder(recordBuilder)
	for i, d := range data {
		if d == nil || !valids[i] {
			if err := smartBuilder.Append(fi, nil); err != nil {
				panic(err)
			}
			continue
		}
		o := reflect.ValueOf(d)
		v := convertMapToKeyValueTuples(o)
		kv := v.Interface()
		if err := smartBuilder.Append(fi, kv); err != nil {
			panic(err)
		}
	}
}

func TestSmartBuilderMaps(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{
				Name: "col15-los",
				Type: arrow.ListOf(arrow.StructOf([]arrow.Field{
					{Name: "field_a", Type: arrow.BinaryTypes.String},
					{Name: "field_b", Type: arrow.BinaryTypes.String},
					{Name: "field_c", Type: arrow.PrimitiveTypes.Float64},
				}...)),
			},
			{
				Name: "col16-los-sb",
				Type: arrow.ListOf(arrow.StructOf([]arrow.Field{
					{Name: "Key", Type: arrow.BinaryTypes.String},
					{Name: "Value", Type: arrow.PrimitiveTypes.Float64},
				}...)),
				// map[string]float64
				// Add some metadata to let consumers of this know that we really want a map logical type.
				Metadata: metadata.AppendOriginalMapTypeMetadata(arrow.Metadata{}),
			},
		},
		nil,
	)

	recordBuilder := array.NewRecordBuilder(pool, schema)
	defer recordBuilder.Release()

	valids := []bool{true, true, true, false, true}

	// list of struct
	addListOfStructs(0, t, recordBuilder, valids)

	// list of struct using smart builder
	addMapAsListOfStructsUsingSmartBuilder(1, t, recordBuilder, valids)

	rec1 := recordBuilder.NewRecord()
	defer rec1.Release()

	got := fmt.Sprintf("%v", rec1)
	// TODO(nickpoorman): Need to verify this is correct. It probably is so let's leave it for now.
	want := `record:
	schema:
	fields: 2
	- col15-los: type=list<item: struct<field_a: utf8, field_b: utf8, field_c: float64>>
	- col16-los-sb: type=list<item: struct<Key: utf8, Value: float64>>
	metadata: ["GOMEM_DATAFRAME_ORIGINAL_TYPE": "MAP", "LogicalType": "MAP"]
	rows: 5
	col[0][col15-los]: [{["r0:s0:e0" "r0:s1:e1"] ["r0:s0:e0" "r0:s1:e1"] [0 1]} {["r1:s0:e2" "r1:s1:e3"] ["r1:s0:e2" "r1:s1:e3"] [0 1]} {["r2:s0:e4" "r2:s1:e5"] ["r2:s0:e4" "r2:s1:e5"] [0 1]} {["r3:s0:e6" "r3:s1:e7"] ["r3:s0:e6" "r3:s1:e7"] [0 1]} {["r4:s0:e8" "r4:s1:e9"] ["r4:s0:e8" "r4:s1:e9"] [0 1]}]
	col[1][col16-los-sb]: [{["field_a" "field_b" "field_c"] [0 0 0]} (null) {["field_a" "field_b" "field_c"] [2 2 2]} (null) {["field_a" "field_b" "field_c"] [4 4 4]}]`
	gotT := trimLines(got)
	wantT := trimLines(want)
	if gotT != wantT {
		t.Errorf("\ngot=\n%s\nwant=\n%s\n", gotT, wantT)
	}

	// 	df, err := NewDataFrameFromRecord(pool, rec1)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	defer df.Release()

	// 	var b bytes.Buffer
	// 	err = df.ToJSON(&b)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	toJSONSmartBuilderResult := `{"col15-los":[{"field_a":"r0:s0:e0","field_b":"r0:s0:e0","field_c":0},{"field_a":"r0:s1:e1","field_b":"r0:s1:e1","field_c":1}],"col16-los-sb":{"field_a":0,"field_b":0,"field_c":0}}
	// {"col15-los":[{"field_a":"r1:s0:e2","field_b":"r1:s0:e2","field_c":0},{"field_a":"r1:s1:e3","field_b":"r1:s1:e3","field_c":1}],"col16-los-sb":null}
	// {"col15-los":[{"field_a":"r2:s0:e4","field_b":"r2:s0:e4","field_c":0},{"field_a":"r2:s1:e5","field_b":"r2:s1:e5","field_c":1}],"col16-los-sb":{"field_a":2,"field_b":2,"field_c":2}}
	// {"col15-los":[{"field_a":"r3:s0:e6","field_b":"r3:s0:e6","field_c":0},{"field_a":"r3:s1:e7","field_b":"r3:s1:e7","field_c":1}],"col16-los-sb":null}
	// {"col15-los":[{"field_a":"r4:s0:e8","field_b":"r4:s0:e8","field_c":0},{"field_a":"r4:s1:e9","field_b":"r4:s1:e9","field_c":1}],"col16-los-sb":{"field_a":4,"field_b":4,"field_c":4}}
	// `

	// 	if got, want := b.String(), toJSONSmartBuilderResult; got != want {
	// 		t.Fatalf("\ngot=\n%s\nwant=\n%s\n", got, want)
	// 	}
}

func trimLines(s string) string {
	lines := strings.Split(s, "\n")
	var newLines []string
	for _, line := range lines {
		trimed := strings.TrimSpace(line)
		if trimed != "" {
			newLines = append(newLines, trimed)
		}
	}
	return strings.Join(newLines, "\n")
}

func addListOfStructs(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()
	lb := recordBuilder.Field(fi).(*array.ListBuilder)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	fb0 := sb.FieldBuilder(0).(*array.StringBuilder)
	fb1 := sb.FieldBuilder(1).(*array.StringBuilder)
	fb2 := sb.FieldBuilder(2).(*array.Float64Builder)

	element := 0

	for i := range valids { // add 5 lists
		lb.Append(true)

		// Inside each list, add 2 structs
		for j := 0; j < 2; j++ {
			sb.Append(true)
			if true {
				fb0.Append(fmt.Sprintf("r%d:s%d:e%d", i, j, element)) // 1 field per struct
				fb1.Append(fmt.Sprintf("r%d:s%d:e%d", i, j, element))
				fb2.Append(float64(j))
				element++
			}
		}
	}
}
