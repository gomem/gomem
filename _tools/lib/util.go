package lib

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
)

func errExit(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func StripPackage(pkg string, s string) string {
	re := regexp.MustCompile(fmt.Sprintf("^%s\\.", pkg))
	return re.ReplaceAllString(s, "")
}

func Contains(list []interface{}, s interface{}) bool {
	for _, e := range list {
		if reflect.DeepEqual(s, e) {
			return true
		}
	}
	return false
}

func getProp(data map[string]interface{}, prop string) string {
	tt, ok := data[prop].(string)
	if ok {
		return tt
	}
	errExit("could not get %s for: %+v", prop, data)
	return ""
}

type TypeSpec struct {
	data interface{}
}

type TemplateData struct {
	Data  interface{}
	Types interface{}
	Casts interface{}
}

// TODO(nickpoorman): I copied this from a previous implementation.
//   This is far more complicated than it needs to be.
//   Clean up the macgyvered code and get rid of some of it.
func BuildKinds(kinds interface{}) []TemplateData {
	types := LoadTypes(kinds)
	td := make([]TemplateData, 0, len(types))
	for _, kind := range types {
		td = append(td, BuildTemplateData(kind, types))
	}
	return td
}

func LoadTypes(types interface{}) []TypeSpec {
	tt, ok := types.([]interface{})
	if !ok {
		errExit("types must be an array but is: %T", tt)
	}

	specs := make([]TypeSpec, 0)
	for _, t := range tt {
		specs = append(specs, TypeSpec{data: t})
	}
	return specs
}

func BuildTemplateData(spec TypeSpec, specs []TypeSpec) TemplateData {
	// Attach the cast data to the spec
	casts := make([]map[string]interface{}, 0)
	data := spec.data.(map[string]interface{})
	td := TemplateData{
		Data:  data,
		Types: specs,
	}

	// For this type
	thisType := getProp(data, "Name")
	thisCastTos, ok := data["CastTo"].([]interface{})
	if !ok {
		thisCastTos = make([]interface{}, 0)
	}

	// Go through all the types and
	for _, st := range specs {
		thatData := st.data.(map[string]interface{})
		castTos, ok := thatData["CastTo"].([]interface{})
		if !ok {
			// There are no CastTo
			continue
		}
		for _, castTo := range castTos {
			ct := castTo.(map[string]interface{})
			from := ct["From"].(string)
			// if the type has a CastTo where From is this type
			if from == thisType {
				// Then create a cast for it
				cast := make(map[string]interface{})
				// Need to lookup this type now
				cast["ToName"] = thatData["Name"]
				cast["ToTestConstructor"] = thatData["TestConstructor"]
				cast["ToDefault"] = thatData["Default"]
				cast["ToBitWidth"] = thatData["BitWidth"]

				via, ok := ct["Via"].(string)
				if ok {
					cast["Via"] = via
				} else {
					cast["ViaBlock"] = ct["ViaBlock"].(string)
				}
				cast["NotImplemented"] = ct["NotImplemented"]
				cast["ReverseNotImplemented"] = GetCastToNotImplemented(thisCastTos, IndexOfCastTo(thisCastTos, getProp(thatData, "Name")))
				cast["ToComment"] = ct["Comment"]
				cast["Overflow"] = ct["Overflow"]
				cast["ToType"] = getProp(thatData, "Name")
				cast["ToDefault"] = thatData["Default"]
				casts = append(casts, cast)
			}
		}
	}

	td.Casts = casts
	return td
}

func GetCastToNotImplemented(castTos []interface{}, index int) bool {
	if index == -1 {
		// If the castTo we are looking for does not exist
		// Then it's not implemented
		return true
	}
	castTo := castTos[index]
	ct := castTo.(map[string]interface{})
	notImplemented, ok := ct["NotImplemented"].(bool)
	if !ok {
		// If the prop NotImplemented is not on this cat to then it's implemented
		return false
	}
	return notImplemented
}

func IndexOfCastTo(castTos []interface{}, name string) int {
	for i := 0; i < len(castTos); i++ {
		iface := castTos[i].(map[string]interface{})
		if iface["From"] == name {
			return i
		}
	}
	return -1
}

func PrintfAll(format string, replacement interface{}) string {
	re := regexp.MustCompile("%s")
	return re.ReplaceAllLiteralString(format, fmt.Sprintf("%v", replacement))
}

// Anything that starts with a capital for it's InternalType or Type is struct based.
// aa.Bar" = true
// aa.Ping" = true
// aa.Bar.Pong" = true
// bar" = false
// Foo" = true
func IsStructBased(kind string) bool {
	spl := strings.Split(kind, ".")
	last := spl[len(spl)-1]
	firstChar := string(last[0])
	return firstChar == strings.ToUpper(firstChar)
}
