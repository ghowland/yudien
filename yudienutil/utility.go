package yudieutil

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
	type_string_force = iota // This forces it to a string, even if it will be ugly, will print the type of the non-string data too.  Testing this to see if splitting these into 2 yields better results.
	type_array        = iota // []interface{} - takes: lists, arrays, maps (key/value tuple array, strings (single element array), ints (single), floats (single)
	type_map          = iota // map[string]interface{}
)

var Debug_Udn bool
var Debug_Udn_Api bool

func GetResult(input interface{}, type_value int) interface{} {
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
				return nil
			}
			return result
		case int:
			return input
		case int8:
			return input
		case int16:
			return input
		case int32:
			return input
		case int64:
			return input
		case uint:
			return input
		case uint8:
			return input
		case uint16:
			return input
		case uint32:
			return input
		case uint64:
			return input
		case float64:
			return int(input.(float64))
		case float32:
			return int(input.(float32))
		default:
			fmt.Printf("\nGetResult: int: default: %v (%T)\n\n", input, input)
			return nil
		}
	case type_float:
		switch input.(type) {
		case string:
			result, err := strconv.ParseFloat(input.(string), 64)
			if err != nil {
				return nil
			}
			return result
		case float64:
			return input
		case float32:
			return input
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
			return nil
		}
	case type_string:
		switch input.(type) {
		case string:
			return input
		default:
			if input == nil {
				return ""
			}

			// If this is already an array, return it as-is
			if strings.HasPrefix(type_str, "[]string") {
				concat := ""
				for _, arr_value := range input.([]string) {
					concat += arr_value
				}
				return concat

			} else if strings.HasPrefix(type_str, "[]") {
				fmt.Printf("GetResult: Will attempt to coerce to string from []: %s\n", SnippetData(input, 60))
				concat := ""
				all_strings := true
				not_strings := make([]interface{}, 0)

				// If all these are strings, just concatenate them
				for _, arr_value := range input.([]interface{}) {
					switch arr_value.(type) {
					case string:
						concat += arr_value.(string)
					default:
						all_strings = false
						fmt.Printf("GetResult: Coerce failure.  Array element is not a string: %s\n", SnippetData(arr_value, 60))
						not_strings = AppendArray(not_strings, SnippetData(arr_value, 30))
					}
				}

				if all_strings {
					fmt.Printf("GetResult: Concatenated array to string\n")
					return concat
				} else {
					fmt.Printf("GetResult: Array cannot be coerced into a string:  Not all elements are strings: %v\n", not_strings)

					json_output, _ := json.MarshalIndent(input, "", "  ")
					return string(json_output)
				}
			} else {
				return fmt.Sprintf("%v", input)
			}

			//NOTE(g): Use type_string_force if you want to coerce this into a string, because this destroys too much data I think.  Testing this as 2 things anyways, easy to fold back into 1 if it doesnt work out.
			return input
		}
	case type_string_force:
		switch input.(type) {
		case string:
			return input
		default:
			if input == nil {
				return ""
			}

			return fmt.Sprintf("%v", input)
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
			result["value"] = input
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

func HtmlClean(html string) string {
	html = strings.Replace(html, "<", "&lt;", -1)
	html = strings.Replace(html, ">", "&gt;", -1)
	html = strings.Replace(html, "&", "&amp;", -1)
	html = strings.Replace(html, " ", "&nbsp;", -1)

	return html
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
func SimpleDottedStringToArray(arg_str string) []interface{} {
	args := make([]interface{}, 0)

	arg_array := strings.Split(arg_str, ".")

	for _, arg := range arg_array {
		arg_trimmed := strings.Trim(arg, ".")

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
		panic(err)
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

func MapCopy(input map[string]interface{}) map[string]interface{} {
	new_map := make(map[string]interface{})

	for k, v := range input {
		new_map[k] = v
	}

	return new_map
}

func JsonDump(value interface{}) string {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(value)
	if err != nil {
		panic(err)
	}

	return buffer.String()
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
