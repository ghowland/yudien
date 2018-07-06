package yudienutil

import (
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	. "github.com/ghowland/yudien/yudiencore"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"io/ioutil"
	"github.com/mitchellh/copystructure"
	"reflect"
	"text/template"
	"encoding/base64"
)


const (
	part_unknown  = iota
	part_function = iota
	part_item     = iota
	part_string   = iota
	part_compound = iota
	part_list     = iota
	part_map      = iota
	part_map_key  = iota
)

const (
	type_int          = iota
	type_float        = iota
	type_string       = iota
	type_array        = iota // []interface{} - takes: lists, arrays, maps (key/value tuple array, strings (single element array), ints (single), floats (single)
	type_map          = iota // map[string]interface{}
)

const ( // order matters for log levels
	log_off   = iota
	log_error = iota
	log_warn  = iota
	log_info  = iota
	log_debug = iota
	log_trace = iota
)

func GetResult(input interface{}, type_value int) interface{} {
	//fmt.Printf("GetResult: %d: %s\n", type_value, SnippetData(input, 60))

	type_str := fmt.Sprintf("%T", input)

	// Unwrap UdnResult, if it is wrapped
	if type_str == "main.UdnResult" {
		input = input.(UdnResult).Result
	} else if type_str == "*main.UdnResult" {
		input = input.(*UdnResult).Result
	}

	switch type_value {
	case type_int:
		switch input.(type) {
		case string:
			result, err := strconv.ParseInt(input.(string), 10, 64)
			if err != nil {
				fmt.Printf("\nGetResult: int: ERROR: %v (%T): %s\n\n", input, input, err)

				log.Panicf("\nGetResult: int: ERROR: %v (%T): %s\n\n", input, input, err)
				return int64(0)
			}
			return result
		case int:
			return int64(input.(int))
		case int8:
			return int64(input.(int8))
		case int16:
			return int64(input.(int16))
		case int32:
			return int64(input.(int32))
		case int64:
			return input
		case uint:
			return int64(input.(uint))
		case uint8:
			return int64(input.(uint8))
		case uint16:
			return int64(input.(uint16))
		case uint32:
			return int64(input.(uint32))
		case uint64:
			return int64(input.(uint64))
		case float64:
			return int64(input.(float64))
		case float32:
			return int64(input.(float32))
		default:
			fmt.Printf("\nGetResult: int: default: %v (%T)\n\n", input, input)
			return int64(0)
		}
	case type_float:
		switch input.(type) {
		case string:
			result, err := strconv.ParseFloat(input.(string), 64)
			if err != nil {
				return float64(0)
			}
			return result
		case float64:
			return input
		case float32:
			return float64(input.(float32))
		case int:
			return float64(input.(int))
		case int8:
			return float64(input.(int8))
		case int16:
			return float64(input.(int16))
		case int32:
			return float64(input.(int32))
		case int64:
			return float64(input.(int64))
		case uint:
			return float64(input.(uint))
		case uint8:
			return float64(input.(uint8))
		case uint16:
			return float64(input.(uint16))
		case uint32:
			return float64(input.(uint32))
		case uint64:
			return float64(input.(uint64))
		default:
			return float64(0)
		}
	case type_string:
		switch input.(type) {
		case string:
			return input
		default:
			if input == nil {
				return ""
			}

			json, err := JsonDumpIfValid(input)
			json = strings.TrimSpace(json)


			if err == nil {
				return json
			} else {
				return fmt.Sprintf("%v", input)
			}
		}
	case type_map:
		//fmt.Printf("GetResult: Map: %s\n", type_str)

		// If this is already a map, return it
		if type_str == "map[string]interface {}" {
			return input
		} else if type_str == "*list.List" {
			// Else, if this is a list, convert the elements into a map, with keys as string indexes values ("0", "1")
			result := make(map[string]interface{})

			count := 0
			for child := input.(*list.List).Front(); child != nil; child = child.Next() {
				count_str := strconv.Itoa(count)

				// Add the index as a string, and the value to the map
				result[count_str] = child.Value
				count++
			}

			return result

		} else if strings.HasPrefix(type_str, "[]") {
			// Else, if this is an array, convert the elements into a map, with keys as string indexes values ("0", "1")
			result := make(map[string]interface{})

			for count, value := range input.([]interface{}) {
				count_str := strconv.Itoa(count)

				// Add the index as a string, and the value to the map
				result[count_str] = value
			}

			return result

		} else {
			// Else, this is not a map yet, so turn it into one, of the key "value"
			result := make(map[string]interface{})

			if input != nil {
				result["value"] = input
			}

			return result
		}
	case type_array:
		// If this is already an array, return it as-is
		if type_str == "[]map[string]interface {}" {
			new_array := make([]interface{}, 0)
			for _, item := range input.([]map[string]interface{}) {
				new_array = AppendArray(new_array, item)
			}
			return new_array
		} else if strings.HasPrefix(type_str, "[]string") {
			new_array := make([]interface{}, 0)
			for _, item := range input.([]string) {
				new_array = AppendArray(new_array, item)
			}
			return new_array
		} else if strings.HasPrefix(type_str, "[]") {
			return input
		} else if type_str == "*list.List" {
			// Else, if this is a List, then create an array and store all the list elements into the array
			result := make([]interface{}, input.(*list.List).Len())

			count := 0
			for child := input.(*list.List).Front(); child != nil; child = child.Next() {
				// Add the index as a string, and the value to the map
				result[count] = child.Value
				count++
			}
			return result

		} else if type_str == "map[string]interface {}" {
			// Else, if this is a Map, then create an array and all the key/values as a single item map, with keys: "key", "value"
			result := make([]interface{}, len(input.(map[string]interface{})))

			count := 0
			for key, value := range input.(map[string]interface{}) {
				// Make a tuple array
				item := make(map[string]interface{})
				item["key"] = key
				item["value"] = value

				// Save the tuple to our array
				result[count] = item

				count++
			}

			return result

		} else {
			if input != nil {
				// Just make a single item array and stick it in
				result := make([]interface{}, 1)
				result[0] = input
				return result
			} else {
				// Empty array
				result := make([]interface{}, 0)
				return result
			}
		}
	}

	return nil
}

func SnippetData(data interface{}, size int) string {
	data_str := fmt.Sprintf("%v", data)
	if len(data_str) > size {
		data_str = data_str[0:size] + "..."
	}

	// Get rid of newlines, they make snippets hard to read
	data_str = strings.Replace(data_str, "\n", "", -1)

	data_str = fmt.Sprintf("%s (%T)", data_str, data)
	//size_str := fmt.Sprint("%v", data)	//TODO(g):PERFORMANCE: Need to test sizes, super slow!
	//data_str = fmt.Sprintf("%s (%T:%d)", data_str, data, len(size_str))

	return data_str
}

func AppendArray(slice []interface{}, data ...interface{}) []interface{} {
	//fmt.Printf("AppendArray: Start: %v\n", slice)
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) { // if necessary, reallocate
		// allocate double what's needed, for future growth.
		newSlice := make([]interface{}, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0:n]
	copy(slice[m:n], data)
	//fmt.Printf("AppendArray: End: %v (%T)\n", slice, slice[0])
	return slice
}

func AppendArrayMap(slice []map[string]interface{}, data ...map[string]interface{}) []map[string]interface{} {
	// Same as AppendArray but for map[string]interface{}

	m := len(slice)
	n := m + len(data)

	if n > cap(slice) { // if necessary, reallocate
		// allocate double what's needed, for future growth.
		newSlice := make([]map[string]interface{}, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0:n]
	copy(slice[m:n], data)

	return slice
}

func MapKeys(data map[string]interface{}) []string {
	// Get the slice of keys
	keys := make([]string, len(data))
	i := 0
	for k := range data {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	return keys
}

func ArrayIntMax(ints []int) int {
	highest := ints[0]

	for _, cur_int := range ints {
		if cur_int > highest {
			highest = cur_int
		}
	}

	return highest
}

// This function takes a string like "some.elements.here", and makes it into a list of ["some", "elements", here"]
func SimpleDottedStringToUdnResultList(arg_str string) list.List {
	args := list.New()

	arg_array := strings.Split(arg_str, ".")

	for _, arg := range arg_array {
		arg_trimmed := strings.Trim(arg, ".")

		udn_result := UdnResult{}
		udn_result.Result = arg_trimmed

		args.PushBack(&udn_result)
	}

	return *args
}

// This function takes a string like "some.elements.here", and makes it into a list of ["some", "elements", here"]
func SimpleDottedStringToArray(arg_str string, separator string) []interface{} {
	args := make([]interface{}, 0)

	arg_array := strings.Split(arg_str, separator)

	for _, arg := range arg_array {
		arg_trimmed := strings.Trim(arg, separator)

		//args.PushBack(&udn_result)
		args = AppendArray(args, arg_trimmed)
	}

	return args
}

func SprintList(items list.List) string {
	output := ""

	for item := items.Front(); item != nil; item = item.Next() {
		item_str := fmt.Sprintf("'%v'", item.Value)

		if output != "" {
			output += " -> "
		}

		output += item_str
	}

	return output
}

// We take an input element, and a count, and will walk the list elements, until the count is complete
func ConvertListToMap(input *list.Element, count int) map[string]interface{} {
	result := make(map[string]interface{})

	index := 0
	for count >= 0 {
		index_str := fmt.Sprintf("%d", index)

		if input != nil {
			result[index_str] = input.Value

			// Go to the next input
			input = input.Next()
		} else {
			fmt.Printf("ConvertListToMap: %d: Input is nil\n", index)
			result[index_str] = nil
		}

		count--
		index++
	}

	return result
}

func ConvertMapArrayToMap(map_array []map[string]interface{}, key string) map[string]interface{} {
	// Flip the values to strings in the keys, so anything can go in.  Use %v.
	result := make(map[string]interface{})

	for _, item := range map_array {
		result_key := fmt.Sprintf("%v", item[key])

		result[result_key] = item
	}

	return result
}

func SprintMap(map_data map[string]interface{}) string {
	output := ""

	for key, value := range map_data {
		output += fmt.Sprintf("'%s' -> %v\n", key, SnippetData(value, 40))
	}

	return output
}

func PrettyPrint(data interface{}) string {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		UdnError(nil, "PrettyPrint ERROR: " + err.Error() + "\n")
	}

	return string(b)
}

func ReadPathData(path string) string {
	file, err := os.Open(path)
	if err == nil {
		defer file.Close()

		file_info, err := file.Stat()
		if err != nil {
			log.Fatal(err)
		}

		// If this isnt a directory
		if !file_info.IsDir() {
			size := file_info.Size()

			data := make([]byte, size)
			_, err := file.Read(data)
			if err != nil {
				log.Fatal(err)
			}

			return string(data)
		}
	}

	return ""
}

func WritePathData(path string, text string) {
	err := ioutil.WriteFile(path, []byte(text), 0644)
	if err != nil {
		panic(err)
	}
}

func MapCopy(input map[string]interface{}) map[string]interface{} {
	new_map := make(map[string]interface{})

	for k, v := range input {
		new_map[k] = v
	}

	return new_map
}

func JsonDumpIfValid(value interface{}) (string, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(value)
	if err != nil {
		return "", err
	}

	return buffer.String(), nil
}

func JsonDump(value interface{}) string {
	json, err := JsonDumpIfValid(value)
	if err != nil {
		UdnError(nil,"JsonDump ERROR: " + err.Error() + "\n")
		json = fmt.Sprintf("\"%v\"", value)
	}

	return json
}

func JsonLoadMap(text string) (map[string]interface{}, error) {
	new_map := make(map[string]interface{})

	err := json.Unmarshal([]byte(text), &new_map)

	return new_map, err
}

func JsonLoadMapIfValid(text *string) (map[string]interface{}, error) {
	new_map := make(map[string]interface{})

	if text == nil {
		return new_map, fmt.Errorf("Text is nil, cannot load map")
	}

	err := json.Unmarshal([]byte(*text), &new_map)

	return new_map, err
}

func JsonLoadArray(text string) ([]interface{}, error) {
	new_array := make([]interface{}, 0)

	err := json.Unmarshal([]byte(text), &new_array)

	return new_array, err
}

func MapListToDict(map_array []map[string]interface{}, key string) map[string]interface{} {
	// Build a map of all our web site page widgets, so we can
	output_map := make(map[string]interface{})

	for _, map_item := range map_array {
		output_map[map_item[key].(string)] = map_item
	}

	return output_map
}

// Take an array of maps, and make a single map, from one of the keys
func MapArrayToMap(map_array []map[string]interface{}, key string) map[string]interface{} {
	result := make(map[string]interface{})

	for _, item := range map_array {
		//fmt.Printf("MapArrayToMap: %s: %s: %v\n", key, item[key].(string), SnippetData(item, 600))
		result[item[key].(string)] = item
	}

	return result
}

func MapKeysToUdnMapForHtmlSelect(position_location string, data map[string]interface{}) string {
	keys := MapKeys(data)

	fmt.Printf("MapKeysToUdnMapForHtmlSelect: %v\n  Keys: %v\n", data, keys)

	map_values := make([]string, 0)

	for index, key := range keys {
		new_position := fmt.Sprintf("%s.%d", position_location, index)

		map_values = append(map_values, fmt.Sprintf("{name='%s',value='%s'}", key, new_position))
	}

	map_value_str := strings.Join(map_values, ",")

	udn_final := fmt.Sprintf("[%s]", map_value_str)

	fmt.Printf("MapKeysToUdnMapForHtmlSelect: Result: %s\n", udn_final)

	return udn_final
}

func MapKeysToUdnMap(data map[string]interface{}) string {
	keys := MapKeys(data)

	fmt.Printf("MapKeysToUdnMap: %v\n  Keys: %v\n", data, keys)

	map_values := make([]string, 0)

	for _, key := range keys {
		map_values = append(map_values, fmt.Sprintf("%s='%s'", key, key))
	}

	map_value_str := strings.Join(map_values, ",")

	udn_final := fmt.Sprintf("{%s}", map_value_str)

	fmt.Printf("MapKeysToUdnMap: Result: %s\n", udn_final)

	return udn_final
}


func MapArrayFind(map_array []map[string]interface{}, key string, value interface{}) map[string]interface{} {
	var return_item map[string]interface{}

	for _, item := range map_array {
		//UdnLogLevel(nil, log_trace, "Map Array Find: %v == %v\n", item[key], value)
		if item[key] == value {
			return_item = item
			//UdnLogLevel(nil, log_trace, "Map Array Find: %v == %v: Matched\n", item[key], value)
			break
		}
	}

	return return_item
}

func MapArrayToToUdnMap(map_array []map[string]interface{}, key_key string, value_key string) string {
	map_values := make([]string, 0)

	for _, data := range map_array {
		map_values = append(map_values, fmt.Sprintf("%s='%s'", data[key_key], data[value_key]))
	}

	map_value_str := strings.Join(map_values, ",")

	udn_final := fmt.Sprintf("{%s}", map_value_str)

	return udn_final
}

type StringFile struct {
	String string
}

func (s *StringFile) Write(ingress []byte) (count int, err error) {
	s.String += string(ingress)

	return len(ingress), nil
}

func NewTextTemplateMap() *TextTemplateMap {
	return &TextTemplateMap{
		Map: make(map[string]interface{}),
	}
}

func NewTextTemplateMapItem() TextTemplateMap {
	return TextTemplateMap{
		Map: make(map[string]interface{}),
	}
}

type TextTemplateMap struct {
	Map map[string]interface{}
}

func IsStringInArray(text string, arr []string) bool {
	for _, v := range arr {
		if text == v {
			return true
		}
	}
	return false
}

func IsValueInArray(value interface{}, arr []interface{}) bool {
	for _, v := range arr {
		if value == v {
			return true
		}
	}
	return false
}

//TODO(g):PACKAGE:REFLECT: Using refect here, to not be string specific, evaluate removing this in the future
func InArray(val interface{}, array interface{}) (exists bool, index int) {
	exists = false
	index = -1

	switch reflect.TypeOf(array).Kind() {
		case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				index = i
				exists = true
				return
			}
		}
	}

	return
}

func DeepCopy(v interface{}) interface{} {
    // copystructure won't take a nil, so early return
    if v == nil {
        return nil
    }
    // might save a few cycles if we test for unmutable types
    // and return early?
    v_copy, err := copystructure.Copy(v)
    if err != nil {
        fmt.Print(err)
        return v
    }
    return v_copy
}

func TemplateMap(template_map map[string]interface{}, text string) string {
	new_template := NewTextTemplateMap()
	new_template.Map = template_map

	item_template := template.Must(template.New("text").Parse(text))

	item := StringFile{}
	err := item_template.Execute(&item, new_template)
	if err != nil {
		log.Panic(err)
	}

	result := item.String

	return result
}

func TemplateInterface(template_item interface{}, text string) string {
	item_template := template.Must(template.New("text").Parse(text))

	item := StringFile{}
	err := item_template.Execute(&item, template_item)
	if err != nil {
		log.Panic(err)
	}

	result := item.String

	return result
}


func UseArgArrayOrFirstArgString(args []interface{}) []interface{} {
	// If we were given a single dotted string, expand it into our arg array
	if len(args) == 1 {
		switch args[0].(type) {
		case string:
			// If this has dots in it, then it can be exploded to become an array of args
			if strings.Contains(args[0].(string), ".") {
				new_args := SimpleDottedStringToArray(args[0].(string), ".")

				return new_args
			}
		}
	}

	return args
}


func GetChildResult(parent interface{}, child interface{}) DynamicResult {
	type_str := fmt.Sprintf("%T", parent)
	//fmt.Printf("\n\nGetChildResult: %s: %s: %v\n\n", type_str, child, SnippetData(parent, 300))

	result := DynamicResult{}

	// Check if the parent is an array or a map
	if strings.HasPrefix(type_str, "[]") {
		// Array access - check what type of array the parent is
		switch parent.(type) {
		case []string:
			parent_array := parent.([]string)
			index := GetResult(child, type_int).(int64)

			if index >= 0 && index < int64(len(parent_array)) {
				result.Result = parent_array[index]
			}
		case []interface{}:
			parent_array := parent.([]interface{})
			index := GetResult(child, type_int).(int64)

			if index >= 0 && index < int64(len(parent_array)) {
				result.Result = parent_array[index]
			}
		case []map[string]interface{}:
			parent_array := parent.([]map[string]interface{})
			index := GetResult(child, type_int).(int64)

			if index >= 0 && index < int64(len(parent_array)) {
				result.Result = parent_array[index]
			}
		default:
			// Array type not recognized - return parent for now
			result.Result = parent
		}

		result.Type = type_array

		return result

	} else {
		child_str := GetResult(child, type_string).(string)

		// Map access
		parent_map := parent.(map[string]interface{})

		result.Result = parent_map[child_str]
		result.Type = type_map

		return result
	}
}

func SetChildResult(parent interface{}, child interface{}, value interface{}) {
	type_str := fmt.Sprintf("%T", parent)
	UdnLogLevel(nil, log_trace, "SetChildResult: %s: %v: %v\n\n", type_str, child, SnippetData(parent, 300))

	// Check if the parent is an array or a map
	if strings.HasPrefix(type_str, "[]") {
		// Array access - check what type of array the parent is
		switch parent.(type) {
		case []string:
			parent_array := parent.([]string)
			index := GetResult(child, type_int).(int64)

			if index >= 0 && index < int64(len(parent_array)) {
				parent_array[index] = value.(string)
			}
		case []interface{}:
			parent_array := parent.([]interface{})
			index := GetResult(child, type_int).(int64)

			if index >= 0 && index < int64(len(parent_array)) {
				parent_array[index] = value
			}
		case []map[string]interface{}:
			parent_array := parent.([]map[string]interface{})
			index := GetResult(child, type_int).(int64)

			if index >= 0 && index < int64(len(parent_array)) {
				parent_array[index] = value.(map[string]interface{})
			}
		default:
			// type is not recognized - do nothing for now
		}

	} else {
		child_str := GetResult(child, type_string).(string)

		// Map access
		parent_map := parent.(map[string]interface{})

		// Set the value
		parent_map[child_str] = DeepCopy(value)
	}
}

func GetArgsFromArgsOrStrings(args []interface{}) []interface{} {
	out_args := make([]interface{}, 0)

	for _, arg := range args {
		switch arg.(type) {
		case string:
			// If this has dots in it, then it can be exploded to become an array of args
			if strings.Contains(arg.(string), ".") {
				new_args := SimpleDottedStringToArray(arg.(string), ".")

				for _, new_arg := range new_args {
					out_args = AppendArray(out_args, new_arg)
				}
			} else {
				out_args = AppendArray(out_args, arg)
			}
		default:
			out_args = AppendArray(out_args, arg)
		}
	}

	//fmt.Printf("\n\nGetArgsFromArgsOrStrings: %v   ===>>>  %v\n\n", args, out_args)

	return out_args
}

func _MapGet(args []interface{}, udn_data interface{}) interface{} {
	// This is what we will use to Set the data into the last map[string]
	last_argument := GetResult(args[len(args)-1], type_string).(string)

	// Start at the top of udn_data, and work down
	var cur_udn_data interface{}
	cur_udn_data = udn_data

	// Go to the last element, so that we can set it with the last arg
	for count := 0; count < len(args)-1; count++ {
		arg := GetResult(args[count], type_string).(string)

		if count != 0 {
			UdnLogLevel(nil, log_trace, "Get: Cur UDN Data: Before move: %s: %v\n\n", arg, JsonDump(cur_udn_data))
		} else {
			UdnLogLevel(nil, log_trace, "Get: First UDN Data: Before move: %s\n\n", arg)
		}

		child_result := GetChildResult(cur_udn_data, arg)

		if child_result.Result != nil {
			if child_result.Type == type_array {
				cur_udn_data = child_result.Result
			} else {
				cur_udn_data = child_result.Result
			}
		} else {
			// Make a new map, simulating something being here.  __set will create this, so this make its bi-directinally the same...
			cur_udn_data = make(map[string]interface{})
		}
	}

	UdnLogLevel(nil, log_trace, "Get: Last Arg data: %s: %s\n\n", last_argument, SnippetData(cur_udn_data, 800))

	// Our result will be a list, of the result of each of our iterations, with a UdnResult per element, so that we can Transform data, as a pipeline
	final_result := GetChildResult(cur_udn_data, last_argument)

	return final_result.Result
}

func Direct_MapSet(args []interface{}, input interface{}, udn_data interface{}) {

	// This is what we will use to Set the data into the last map[string]
	last_argument := GetResult(args[len(args)-1], type_string).(string)

	// Start at the top of udn_data, and work down
	var cur_udn_data interface{}
	cur_udn_data = udn_data

	// Go to the last element, so that we can set it with the last arg
	for count := 0; count < len(args)-1; count++ {
		child_result := GetChildResult(cur_udn_data, args[count])

		UdnLogLevel(nil, log_trace, "Direct_MapSet: %d: %v\n\n", count, child_result)

		// If we dont have this key, create a map[string]interface{} to allow it to be created easily
		if child_result.Result == nil {
			new_map := make(map[string]interface{})
			SetChildResult(cur_udn_data, args[count], new_map)
			child_result = GetChildResult(cur_udn_data, args[count])
		}

		// Go down the depth of maps
		if child_result.Type == type_array {
			cur_udn_data = child_result.Result
		} else {
			cur_udn_data = child_result.Result
		}
	}

	// Set the last element
	SetChildResult(cur_udn_data, last_argument, input)
}

func MapGet(args []interface{}, udn_data interface{}) interface{} {
	// If we were given a single dotted string, expand it into our arg array
	args = UseArgArrayOrFirstArgString(args)

	// Only try with our first argument converted initially (legacy behavior)
	result := _MapGet(args, udn_data)

	if result == nil {
		// Try converting all our arguments if we couldnt get it before.  It might be dotted.
		args = GetArgsFromArgsOrStrings(args)

		result = _MapGet(args, udn_data)
	}

	return result
}

func MapSet(args []interface{}, input interface{}, udn_data interface{}) interface{} {
	// Determine what our args should be, based on whether the data is available for getting already, allow explicit to override depth-search
	first_args := UseArgArrayOrFirstArgString(args)
	result := _MapGet(first_args, udn_data)
	if result == nil {
		// If we didn't find this value in it's explicit (dotted) string location, then expand the dots
		args = GetArgsFromArgsOrStrings(args)
	} else {
		args = first_args
	}

	Direct_MapSet(args, input, udn_data)

	// Input is a pass-through
	return input
}

func MapIndexSet(args []interface{}, input interface{}, udn_data interface{}) interface{} {
	// Same as func MapSet but udn_data is returned rather than input

	// Determine what our args should be, based on whether the data is available for getting already, allow explicit to override depth-search
	first_args := UseArgArrayOrFirstArgString(args)
	result := _MapGet(first_args, udn_data)
	if result == nil {
		// If we didn't find this value in it's explicit (dotted) string location, then expand the dots
		args = GetArgsFromArgsOrStrings(args)
	} else {
		args = first_args
	}

	Direct_MapSet(args, input, udn_data)

	// Return the updated udn_data
	return udn_data
}


func TemplateShortFromMap(template_string string, template_map map[string]interface{}) string {
	//UdnLogLevel(nil, log_trace, "Short Template From Value: Template Input: %s\n\n", JsonDump(template_map))
	//UdnLogLevel(nil, log_trace, "Short Template From Value: Incoming Template String: %s\n\n", template_string)

	for key, value := range template_map {
		//UdnLogLevel(nil, log_trace, "Key: %v   Value: %v\n", key, value)
		key_replace := fmt.Sprintf("{{{%s}}}", key)
		value_str := GetResult(value, type_string).(string)

		//UdnLogLevel(nil, log_trace, "Short Template From Value: Value String: %s == '%s'\n\n", key, value_str)
		template_string = strings.Replace(template_string, key_replace, value_str, -1)
	}

	return template_string
}


func TemplateFromMap(template_string string, template_map map[string]interface{}) string {

	template_string = strings.Replace(template_string, "\\", "", -1)

	UdnLogLevel(nil, log_trace, "String Template From Value: Template Input: Post Conversion Input: %v\n\n", SnippetData(template_map, 600))


	UdnLogLevel(nil, log_trace, "String Template From Value: Template Input: %s Template String: %v\n\n", SnippetData(template_map, 60), SnippetData(template_string, 600))

	UdnLogLevel(nil, log_trace, "String Template From Value: Template Input: %s\n\n", JsonDump(template_map))

	// Use the actual_input, which may be input or arg_1
	input_template := NewTextTemplateMap()
	input_template.Map = GetResult(template_map, type_map).(map[string]interface{})

	item_template := template.Must(template.New("text").Parse(template_string))

	item := StringFile{}
	err := item_template.Execute(&item, input_template)
	if err != nil {
		log.Panic(err)
	}

	return item.String
}

func ArrayStringJoin(input_val []interface{}, separator string) string {
	result_string := ""
	for _, item := range input_val {
		if result_string != "" {
			result_string += separator
		}

		item_str := GetResult(item, type_string).(string)

		result_string += item_str
	}

	return result_string
}

func Base64Encode(text string) string {
	encoded := base64.URLEncoding.EncodeToString([]byte(text))

	return encoded
}

func Base64Decode(text string) (string, error) {
	decoded := ""

	decoded_bytes, err := base64.URLEncoding.DecodeString(text)
	if err == nil {
		decoded = string(decoded_bytes)
	}

	return decoded, err
}

// Convert a JSON map from all-maps to arrays where the keys are all ints, and if the final values are a CSV string into an array of strings
func JsonConvertRecordMap(field_map map[string]interface{}) map[string]interface{} {
	for key, value := range field_map {
		switch value.(type) {
		case map[string]interface{}:
			UdnLogLevel(nil, log_trace, "JsonConvertRecordMap: %s: %v\n", key, value)
		
			// Test all the keys to see if they are all ints, assume they are and falsify
			keys_all_ints := true
			value_map := value.(map[string]interface{})
			
			for field_key, _ := range value_map {
				_, err := strconv.ParseInt(field_key, 10, 64)
				if err != nil {
					keys_all_ints = false
				}
			} 

			if keys_all_ints {
				UdnLogLevel(nil, log_trace, "JsonConvertRecordMap: %s: Keys all ints -- Convert!\n", key)

				// Make our new array
				new_value := make([]interface{}, 0)

				map_keys := MapGetKeys(value_map)
				for _, map_key := range map_keys {
					new_value = append(new_value, value_map[map_key])
				}

				field_map[key] = new_value
			}
		}
	}

	return field_map
}

func MapGetKeys(data map[string]interface{}) []string {
	string_array := make([]string, 0)

	for key, _ := range data {
		string_array = append(string_array, key)
	}

	sort.Strings(string_array)

	return string_array
}

func MapUpdate(source_map map[string]interface{}, update_map map[string]interface{}) map[string]interface{} {
	new_map := MapCopy(source_map)

	for k, v := range update_map {
		new_map[k] = v
	}

	return new_map
}

