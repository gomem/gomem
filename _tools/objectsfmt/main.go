package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

func errExit(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}
func main() {
	var (
		dataArg = flag.String("data", "", "input JSON data")
	)

	flag.Parse()
	if *dataArg == "" {
		errExit("data option is required")
	}

	data := mustReadAll(*dataArg)

	// Get the names
	names := getNames(data)

	// Read all the types in
	var js []OrderedMap
	err := json.Unmarshal(data, &js)
	if err != nil {
		errExit("unmarshal file error: %v", err)
	}

	// Sort the Types by name
	sort.SliceStable(js, func(i, j int) bool {
		l, lok := js[i].Map["Name"].(string)
		r, rok := js[j].Map["Name"].(string)
		if !lok {
			errExit("could not get name for type: %v", js[i].Map)
		}
		if !rok {
			errExit("could not get name for type: %v", js[j].Map)
		}
		return l < r
	})

	for i := range js {
		kind := js[i].Map["Name"].(string)
		castTos, ok := js[i].Map["CastTo"].([]interface{})
		if !ok {
			// This type didn't have a CastTo prop.
			castTos = make([]interface{}, 0, len(names))
		}

		// Make sure we have a CastTo for each type we have
		for _, name := range names {
			index := indexOfCastTo(castTos, name)
			if index == -1 {
				via := fmt.Sprintf("%s(t)", kind)
				// We need to create a new one
				castTo := map[string]interface{}{
					"From":           name,
					"Via":            via,
					"NotImplemented": true,
				}
				castTos = append(castTos, castTo)
				fmt.Printf("Adding CastTo for: Kind = %s | From: %s | Via: %s\n", kind, name, via)
			}
		}

		// Sort each of the CastTos by From
		sort.SliceStable(castTos, func(i, j int) bool {
			liface := castTos[i].(map[string]interface{})
			riface := castTos[j].(map[string]interface{})
			l, lok := liface["From"].(string)
			r, rok := riface["From"].(string)
			if !lok {
				errExit("could not get From for castTo: %v", liface)
			}
			if !rok {
				errExit("could not get From for castTo: %v", riface)
			}
			return l < r
		})

		js[i].Set("CastTo", castTos)
	}

	// Write them out into a new file
	newBytes, err := json.MarshalIndent(js, "", "  ")
	if err != nil {
		errExit("marshal data error: %v", err)
	}

	// The encoder replaces our non UTF-8 characters,
	// but we want the json to be human readable.
	unescapedBytes, err := _UnescapeUnicodeCharactersInJSON(newBytes)
	if err != nil {
		errExit("unescape bytes data error: %v", err)
	}

	err = ioutil.WriteFile(fmt.Sprintf("%s_%d.json", *dataArg, time.Now().Unix()), unescapedBytes, 0644)
	if err != nil {
		errExit("write file error: %v", err)
	}
}

func indexOfCastTo(castTos []interface{}, name string) int {
	for i := 0; i < len(castTos); i++ {
		iface := castTos[i].(map[string]interface{})
		if iface["From"] == name {
			return i
		}
	}
	return -1
}

func getNames(data []byte) []string {
	result := gjson.GetBytes(data, "#.Name")
	results := result.Array()
	names := make([]string, len(results))
	for i, name := range results {
		names[i] = name.String()
	}
	sort.Strings(names)
	return names
}

func mustReadAll(path string) []byte {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		errExit(err.Error())
	}

	return data
}

type OrderedMap struct {
	Order []string
	Map   map[string]interface{}
}

func (om *OrderedMap) Set(key string, value interface{}) {
	om.Map[key] = value
	for _, k := range om.Order {
		if k == key {
			return
		}
	}
	om.Order = append(om.Order, key)
}

func (om *OrderedMap) UnmarshalJSON(b []byte) error {
	err := json.Unmarshal(b, &om.Map)
	if err != nil {
		return err
	}

	index := make(map[string]int)
	for key := range om.Map {
		om.Order = append(om.Order, key)
		esc, err := json.Marshal(key) //Escape the key
		if err != nil {
			return err
		}
		index[key] = bytes.Index(b, esc)
	}

	sort.Slice(om.Order, func(i, j int) bool { return index[om.Order[i]] < index[om.Order[j]] })
	return nil
}

func (om OrderedMap) MarshalJSON() ([]byte, error) {
	var b []byte
	buf := bytes.NewBuffer(b)
	buf.WriteRune('{')
	l := len(om.Order)
	for i, key := range om.Order {
		km, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		buf.Write(km)
		buf.WriteRune(':')
		vm, err := json.Marshal(om.Map[key])
		if err != nil {
			return nil, err
		}
		buf.Write(vm)
		if i != l-1 {
			buf.WriteRune(',')
		}
	}
	buf.WriteRune('}')
	return buf.Bytes(), nil
}

func _UnescapeUnicodeCharactersInJSON(_jsonRaw json.RawMessage) (json.RawMessage, error) {
	str, err := strconv.Unquote(strings.Replace(strconv.Quote(string(_jsonRaw)), `\\u`, `\u`, -1))
	if err != nil {
		return nil, err
	}
	return []byte(str), nil
}
