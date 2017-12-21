package yudien

import (
	"container/list"
	"database/sql"
	"fmt"
	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudienutil"
	"strings"
)

type DynamicResult struct {
	// This is the result
	Result interface{}

	Type int

	// Error messages
	Error string
}

func SprintUdnResultList(items list.List) string {
	output := ""

	for item := items.Front(); item != nil; item = item.Next() {
		item_str := GetResult(item.Value.(*UdnResult), type_string).(string)

		if output != "" {
			output += " -> "
		}

		output += item_str
	}

	return output
}

func GetUdnResultValue(udn_result *UdnResult) interface{} {
	result := udn_result.Result

	// Recurse if this is a UdnResult as well, since they can be packed inside each other, this function opens the box and gets the real answer
	if fmt.Sprintf("%T", result) == "*main.UdnResult" {
		result = GetUdnResultValue(result.(*UdnResult))
	}

	return result
}

func GetUdnResultString(udn_result *UdnResult) string {
	result := GetUdnResultValue(udn_result)

	result_str := fmt.Sprintf("%v", result)

	return result_str
}

func UseArgArrayOrFirstArgString(args []interface{}) []interface{} {
	// If we were given a single dotted string, expand it into our arg array
	if len(args) == 1 {
		switch args[0].(type) {
		case string:
			// If this has dots in it, then it can be exploded to become an array of args
			if strings.Contains(args[0].(string), ".") {
				new_args := SimpleDottedStringToArray(args[0].(string))

				return new_args
			}
		}
	}

	return args
}

func GetArgsFromArgsOrStrings(args []interface{}) []interface{} {
	out_args := make([]interface{}, 0)

	for _, arg := range args {
		switch arg.(type) {
		case string:
			// If this has dots in it, then it can be exploded to become an array of args
			if strings.Contains(arg.(string), ".") {
				new_args := SimpleDottedStringToArray(arg.(string))

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
			//fmt.Printf("Get: Cur UDN Data: Before change: %s: %v\n\n", arg, JsonDump(cur_udn_data))
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

	//fmt.Printf("Get: Last Arg data: %s: %s\n\n", last_argument, SnippetData(cur_udn_data, 800))

	// Our result will be a list, of the result of each of our iterations, with a UdnResult per element, so that we can Transform data, as a pipeline
	final_result := GetChildResult(cur_udn_data, last_argument)

	return final_result.Result
}

func SetChildResult(parent interface{}, child interface{}, value interface{}) {
	type_str := fmt.Sprintf("%T", parent)
	//fmt.Printf("\n\nSetChildResult: %s: %v: %v\n\n", type_str, child, SnippetData(parent, 300))

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
		parent_map[child_str] = value
	}
}

func _MapSet(args []interface{}, input interface{}, udn_data interface{}) {

	// This is what we will use to Set the data into the last map[string]
	last_argument := GetResult(args[len(args)-1], type_string).(string)

	// Start at the top of udn_data, and work down
	var cur_udn_data interface{}
	cur_udn_data = udn_data

	// Go to the last element, so that we can set it with the last arg
	for count := 0; count < len(args)-1; count++ {
		child_result := GetChildResult(cur_udn_data, args[count])

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

	_MapSet(args, input, udn_data)

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

	_MapSet(args, input, udn_data)

	// Return the updated udn_data
	return udn_data
}

// Parse a UDN string and return a hierarchy under UdnPart
func ParseUdnString(db *sql.DB, udn_schema map[string]interface{}, udn_value_source string) *UdnPart {

	// First Stage
	next_split := _SplitQuotes(db, udn_schema, udn_value_source)

	//UdnLog(udn_schema, "\nSplit: Quotes: AFTER: %v\n\n", next_split)

	next_split = _SplitCompoundStatements(db, udn_schema, next_split)

	//UdnLog(udn_schema, "\nSplit: Compound: AFTER: %v\n\n", next_split)

	next_split = _SplitStatementLists(db, udn_schema, next_split)

	//UdnLog(udn_schema, "\nSplit: List: AFTER: %v\n\n", next_split)

	// Forth Stage
	next_split = _SplitStatementMaps(db, udn_schema, next_split)

	//UdnLog(udn_schema, "\nSplit: Compound: Map: %v\n\n", next_split)

	// Fifth Stage
	next_split = _SplitStatementMapKeyValues(db, udn_schema, next_split)

	//UdnLog(udn_schema, "\nSplit: Compound: Map Key Values: %v\n\n", next_split)

	// Put it into a structure now -- UdnPart
	//
	udn_start := CreateUdnPartsFromSplit_Initial(db, udn_schema, next_split)

	////output := DescribeUdnPart(&udn_start)
	////UdnLog(udn_schema, "\n===== 0 - Description of UDN Part:\n\n%s\n===== 0 - END\n", output)

	// Put it into a structure now -- UdnPart
	//
	FinalParseProcessUdnParts(db, udn_schema, &udn_start)

	//output := DescribeUdnPart(&udn_start)
	//UdnLog(udn_schema, "\n===== 1 - Description of UDN Part:\n\n%s\n===== 1 - END\n", output)

	return &udn_start
}

// Take the partially created UdnParts, and finalize the parsing, now that it has a hierarchical structure.  Recusive function
func FinalParseProcessUdnParts(db *sql.DB, udn_schema map[string]interface{}, part *UdnPart) {

	//UdnLog(udn_schema, "\n** Final Parse **:  Type: %d   Value: %s   Children: %d  Next: %v\n", part.PartType, part.Value, part.Children.Len(), part.NextUdnPart)

	// If this is a map component, make a new Children list with our Map Keys
	if part.PartType == part_map {
		new_children := list.New()

		//fmt.Printf("\n\nMap Part:\n%s\n\n", DescribeUdnPart(part))

		next_child_is_value := false
		next_child_is_assignment := false

		for child := part.Children.Front(); child != nil; child = child.Next() {
			cur_child := *child.Value.(*UdnPart)

			// If this child isn't the value of the last Map Key, then we are expecting a new Map Key, possibly a value
			if next_child_is_assignment == true {
				// We found the assignment child, so the next child is the value
				next_child_is_assignment = false
				next_child_is_value = true
			} else if next_child_is_value == false {
				map_key_split := strings.Split(cur_child.Value, "=")

				// Create the map key part
				map_key_part := NewUdnPart()
				map_key_part.Value = map_key_split[0]
				map_key_part.PartType = part_map_key
				map_key_part.Depth = part.Depth + 1
				map_key_part.ParentUdnPart = part

				// Add to the new Children
				new_children.PushBack(&map_key_part)

				if len(map_key_split) == 1 {
					// We only had the key, so the next child is the assignment
					next_child_is_assignment = true
				} else if map_key_split[1] == "" {
					// We split on the =, but the next section is empty, so the value is in the next child
					next_child_is_value = true
				} else {
					// Else, we make a new UdnPart from the second half of this split, and add it as a child to a new Map Key.  The key and the value were in a single string...
					key_value_part := NewUdnPart()
					key_value_part.PartType = part_item
					key_value_part.Depth = map_key_part.Depth + 1
					key_value_part.ParentUdnPart = &map_key_part
					key_value_part.Value = map_key_split[1]
					map_key_part.Children.PushBack(&key_value_part)
				}
			} else {
				// Get the last Map Key in new_children
				last_map_key := new_children.Back().Value.(*UdnPart)

				// Add this UdnPart to the Map Key's children
				last_map_key.Children.PushBack(&cur_child)

				// Set this back to false, as we processed this already
				next_child_is_value = false
			}

			//new_children.PushBack(&cur_child)
		}

		// Assign the new children list to be our Map's children
		part.Children = new_children
	}

	// If this is a function, remove any children that are for other functions (once other functions start)
	if part.PartType == part_compound {
		//UdnLog(udn_schema, "  Compound type!\n\n")
	}

	// If this is a function, remove any children that are for other functions (once other functions start)
	if part.PartType == part_function {
		if part.ParentUdnPart != nil && part.ParentUdnPart.PartType == part_compound {
			// This is a function inside a compound, so dont do what we normally do, as we are already OK!
			//UdnLog(udn_schema, "\nSkipping: Parent is compound: %s\n\n", part.Value)
		} else {
			// Else, this is not a Compound function (Function Argument)
			if part.ParentUdnPart != nil {
				//UdnLog(udn_schema, "\nMap Function: %s  Parent:  %s (%d)\n\n", part.Value, part.ParentUdnPart.Value, part.ParentUdnPart.PartType)
			} else {
				//UdnLog(udn_schema, "\nMap Function: %s  Parent:  NONE\n\n", part.Value)
			}

			// Once this is true, start adding new functions and arguments into the NextUdnPart list
			found_new_function := false

			// New functions we will add after removing elements, into the NextUdnPart chain
			new_function_list := list.New()
			remove_children := list.New()

			// Current new function (this one will always be replaced before being used, but nil wouldnt type cast properly)
			cur_udn_function := UdnPart{}

			for child := part.Children.Front(); child != nil; child = child.Next() {
				if strings.HasPrefix(child.Value.(*UdnPart).Value, "__") {
					// All children from now on will be either a new NextUdnPart, or will be args to those functions
					found_new_function = true

					// Create our new function UdnPart here
					new_udn_function := NewUdnPart()
					new_udn_function.Value = child.Value.(*UdnPart).Value
					new_udn_function.Depth = part.Depth
					new_udn_function.PartType = part_function
					new_udn_function.Children = child.Value.(*UdnPart).Children

					new_function_list.PushBack(&new_udn_function)
					remove_children.PushBack(child)

					cur_udn_function = new_udn_function

					//UdnLog(udn_schema, "Adding to new_function_list: %s\n", new_udn_function.Value)

				} else if child.Value.(*UdnPart).PartType == part_compound {
					//SKIP: If this is a compount function, we dont need to do anything...
					//UdnLog(udn_schema, "-=-=-= Found Compound!\n -=-=-=-\n")
				} else if found_new_function == true {
					new_udn := NewUdnPart()
					new_udn.Value = child.Value.(*UdnPart).Value
					new_udn.ValueFinal = child.Value.(*UdnPart).ValueFinal
					new_udn.Depth = cur_udn_function.Depth + 1
					new_udn.PartType = child.Value.(*UdnPart).PartType
					new_udn.ParentUdnPart = &cur_udn_function
					new_udn.Children = child.Value.(*UdnPart).Children

					// Else, if we are taking
					cur_udn_function.Children.PushBack(&new_udn)
					remove_children.PushBack(child)

					//UdnLog(udn_schema, "  Adding new function Argument/Child: %s\n", new_udn.Value)
				}
			}

			// Remove these children from the current part.Children
			for child := remove_children.Front(); child != nil; child = child.Next() {

				//UdnLog(udn_schema, "Removing: %v\n", child.Value.(*list.Element).Value)

				_ = part.Children.Remove(child.Value.(*list.Element))
				//removed := part.Children.Remove(child.Value.(*list.Element))
				//UdnLog(udn_schema, "  Removed: %v\n", removed)
			}

			// Find the last UdnPart, that doesnt have a NextUdnPart, so we can add all the functions onto this
			last_udn_part := part
			for last_udn_part.NextUdnPart != nil {
				last_udn_part = last_udn_part.NextUdnPart
				//
				//
				//TODO(g): This is probably where this goes wrong for Compound, because it is assuming it will find this, but this is put of the primary function chain?
				//
				//...
				//
				//UdnLog(udn_schema, "Moving forward: %s   Next: %v\n", last_udn_part.Value, last_udn_part.NextUdnPart)
			}

			//UdnLog(udn_schema, "Elements in new_function_list: %d\n", new_function_list.Len())

			// Add all the functions to the NextUdnPart, starting from last_udn_part
			for new_function := new_function_list.Front(); new_function != nil; new_function = new_function.Next() {
				// Get the UdnPart for the next function
				add_udn_function := *new_function.Value.(*UdnPart)

				// Set at the next item, and connect parrent
				last_udn_part.NextUdnPart = &add_udn_function
				add_udn_function.ParentUdnPart = last_udn_part

				//UdnLog(udn_schema, "Added NextUdnFunction: %s\n", add_udn_function.Value)

				// Update our new last UdnPart, which continues the Next trail
				last_udn_part = &add_udn_function
			}
		}

	}

	// Process all this part's children
	for child := part.Children.Front(); child != nil; child = child.Next() {
		FinalParseProcessUdnParts(db, udn_schema, child.Value.(*UdnPart))
	}

	// Process any next parts (more functions)
	if part.NextUdnPart != nil {
		FinalParseProcessUdnParts(db, udn_schema, part.NextUdnPart)
	}
}

// Take partially split text, and start putting it into the structure we need
func CreateUdnPartsFromSplit_Initial(db *sql.DB, udn_schema map[string]interface{}, source_array []string) UdnPart {
	udn_start := NewUdnPart()
	udn_current := &udn_start

	// We start at depth zero, and descend with sub-statements, lists, maps, etc
	udn_current.Depth = 0

	is_open_quote := false

	//UdnLog(udn_schema, "Create UDN Parts: Initial: %v\n\n", source_array)

	// Traverse into the data, and start storing everything
	for _, cur_item := range source_array {
		//UdnLog(udn_schema, "  Create UDN Parts: UDN Current: %-20s    Cur Item: %v\n", udn_current.Value, cur_item)

		// If we are in a string, and we are not about to end it, keep appending to the previous element
		if is_open_quote && cur_item != "'" {
			udn_current.Value += cur_item
		} else {
			// We are not in a currently open string, or are about to end it, so do normal processing

			// If this is a Underscore, make a new piece, unless this is the first one
			if strings.HasPrefix(cur_item, "__") {

				// Split any dots that may be connected to this still (we dont split on them before this), so we do it here and the part_item test, to complete that
				dot_split_array := strings.Split(cur_item, ".")

				// In the beginning, the udn_start (first part) is part_unknown, but we can use that for the first function, so we just set it here, instead of AddFunction()
				if udn_current.PartType == part_unknown {
					// Set the first function value and part
					udn_current.Value = dot_split_array[0]
					udn_current.PartType = part_function
					// Manually set this first one, as it isnt done through AddFunction()
					udn_current.Id = fmt.Sprintf("%p", &udn_current)
					//UdnLog(udn_schema, "Create UDN: Function Start: %s\n", cur_item)
				} else {
					// Else, this is not the first function, so add it to the current function
					udn_current = udn_current.AddFunction(dot_split_array[0])
				}

				// Add any of the remaining dot_split_array as children
				for dot_count, doc_split_child := range dot_split_array {
					// Skip the 1st element, which is the function name we stored above
					if dot_count >= 1 {
						if doc_split_child != "" {
							if strings.HasPrefix(doc_split_child, "__") {
								udn_current = udn_current.AddFunction(doc_split_child)
							} else {
								udn_current.AddChild(part_item, doc_split_child)
							}
						}
					}
				}

			} else if cur_item == "'" {
				// Enable and disable our quoting, it is simple enough we can just start/stop it.  Lists, maps, and subs cant be done this way.
				if !is_open_quote {
					is_open_quote = true
					udn_current = udn_current.AddChild(part_string, "")
					//UdnLog(udn_schema, "Create UDN: Starting Quoted String\n")
				} else if is_open_quote {
					is_open_quote = false

					// Add single quotes using the HTML Double Quote mechanism, so we can still have single quotes
					udn_current.Value = strings.Replace(udn_current.Value, "&QUOTE;", "'", -1)

					// Reset to before we were a in string
					udn_current = udn_current.ParentUdnPart
					//UdnLog(udn_schema, "Create UDN: Closing Quoted String\n")
				}
			} else if cur_item == "(" {
				//UdnLog(udn_schema, "Create UDN: Starting Compound\n")

				////TODO(g): Is this the correct way to do this?  Im not sure it is...  Why is it different than other children?  Add as a child, then become the current...
				//// Get the last child, which we will become a child of (because we are on argument) -- Else, we are already in our udn_current...
				//if udn_current.Children.Len() > 0 {
				//	last_udn_current := udn_current.Children.Back().Value.(*UdnPart)
				//	// Set the last child to be the current item, and we are good!
				//	udn_current = last_udn_current
				//}

				// Make this compound current, so we continue to add into it, until it closes
				udn_current = udn_current.AddChild(part_compound, cur_item)

			} else if cur_item == ")" {
				//UdnLog(udn_schema, "Create UDN: Closing Compound\n")

				// Walk backwards until we are done
				done := false
				for done == false {
					if udn_current.ParentUdnPart == nil {
						// If we have no more parents, we are done because there is nothing left to come back from
						//TODO(g): This could be invalid grammar, need to test for that (extra closing sigils)
						done = true
						//UdnLog(udn_schema, "COMPOUND: No more parents, finished\n")
					} else if udn_current.PartType == part_compound {
						// Else, if we are already currently on the map, just move off once
						udn_current = udn_current.ParentUdnPart

						done = true
						//UdnLog(udn_schema, "COMPOUND: Moved out of the Compound\n")
					} else {
						//UdnLog(udn_schema, "COMPOUND: Updating UdnPart to part: %v --> %v\n", udn_current, *udn_current.ParentUdnPart)
						udn_current = udn_current.ParentUdnPart
						//UdnLog(udn_schema, "  Walking Up the Compound:  Depth: %d\n", udn_current.Depth)
					}

				}
			} else if cur_item == "[" {
				// Make this list current, so we continue to add into it, until it closes
				udn_current = udn_current.AddChild(part_list, cur_item)

			} else if cur_item == "]" {
				//UdnLog(udn_schema, "Create UDN: Closing List\n")

				// Walk backwards until we are done
				done := false
				for done == false {
					if udn_current.ParentUdnPart == nil {
						// If we have no more parents, we are done because there is nothing left to come back from
						//TODO(g): This could be invalid grammar, need to test for that (extra closing sigils)
						done = true
						//UdnLog(udn_schema, "LIST: No more parents, finished\n")
					} else if udn_current.PartType == part_list {
						// Else, if we are already currently on the map, just move off once
						udn_current = udn_current.ParentUdnPart

						done = true
						//UdnLog(udn_schema, "LIST: Moved out of the List\n")
					} else {
						//UdnLog(udn_schema, "LIST: Updating UdnPart to part: %v --> %v\n", udn_current, *udn_current.ParentUdnPart)
						udn_current = udn_current.ParentUdnPart
						//UdnLog(udn_schema, "  Walking Up the List:  Depth: %d\n", udn_current.Depth)
					}

				}
			} else if cur_item == "{" {
				// Make this list current, so we continue to add into it, until it closes
				udn_current = udn_current.AddChild(part_map, cur_item)

			} else if cur_item == "}" {
				//UdnLog(udn_schema, "Create UDN: Closing Map\n")

				// Walk backwards until we are done
				done := false
				for done == false {
					if udn_current.ParentUdnPart == nil {
						// If we have no more parents, we are done because there is nothing left to come back from
						//TODO(g): This could be invalid grammar, need to test for that (extra closing sigils)
						done = true
						UdnLog(udn_schema, "MAP: No more parents, finished\n")
					} else if udn_current.PartType == part_map {
						// Else, if we are already currently on the map, just move off once
						udn_current = udn_current.ParentUdnPart

						done = true
						//UdnLog(udn_schema, "MAP: Moved out of the Map\n")
					} else {
						//UdnLog(udn_schema, "MAP: Updating UdnPart to part: %v --> %v\n", udn_current, *udn_current.ParentUdnPart)
						udn_current = udn_current.ParentUdnPart
						//UdnLog(udn_schema, "  Walking Up the Map:  Depth: %d\n", udn_current.Depth)
					}
				}
			} else {
				// If this is not a separator we are going to ignore, add it as Children (splitting on commas)
				if cur_item != "" && cur_item != "." {
					children_array := strings.Split(cur_item, ",")

					// Add basic elements as children
					for _, comma_child_item := range children_array {
						dot_children_array := strings.Split(comma_child_item, ".")

						for _, new_child_item := range dot_children_array {
							if strings.TrimSpace(new_child_item) != "" {
								//udn_current.AddChild(part_item, new_child_item)

								if strings.HasPrefix(new_child_item, "__") {
									udn_current = udn_current.AddFunction(new_child_item)
								} else {
									udn_current.AddChild(part_item, new_child_item)
								}

							}
						}
					}
				}
			}
		}

	}

	//UdnLog(udn_schema, "Finished Create UDN Parts: Initial\n\n")

	return udn_start
}

func _SplitStringAndKeepSeparator(value string, separator string) []string {
	split_array := strings.Split(value, separator)

	final_array := make([]string, (len(split_array)*2)-1)

	for pos, item := range split_array {
		cur_pos := pos * 2

		final_array[cur_pos] = item

		if cur_pos+1 < len(final_array) {
			final_array[cur_pos+1] = separator
		}
	}

	// Fix the stupid trailing empty item, if it exists.  Will just increase with splits.
	if final_array[len(final_array)-1] == "" {
		final_array = final_array[0 : len(final_array)-1]
	}

	//UdnLog(udn_schema, "Split: %s  Sep: %s  Result: %s\n", value, separator, final_array)

	return final_array
}

func _SplitStringArray(value_array []string, separator string) []string {
	total_count := 0

	work_list := list.New()

	// Split all the string arrays, keep track of the new total, and put them into the work_list
	for _, item := range value_array {
		split_array := _SplitStringAndKeepSeparator(item, separator)

		total_count += len(split_array)

		work_list.PushBack(split_array)
	}

	// Our final array
	final_array := make([]string, total_count)

	// Iterate over the work list, and add them to our final array by index
	append_count := 0

	for item := work_list.Front(); item != nil; item = item.Next() {
		cur_string_array := item.Value.([]string)

		for _, item_string := range cur_string_array {
			final_array[append_count] = item_string

			append_count++
		}
	}

	return final_array
}

// FIRST STAGE: Recursive function, tracked by depth int.  Inserts sequentially into next_processing_udn_list (list[string]), each of the compound nested items, starting with the inner-most first, and then working out, so that all compound statements can be sequentially processed, with the inner-most getting processed before their immediate next-outer layer, which is the proper order
func _SplitQuotes(db *sql.DB, udn_schema map[string]interface{}, udn_value string) []string {
	//UdnLog(udn_schema, "\nSplit: Quotes: %v\n\n", udn_value)

	split_result := _SplitStringAndKeepSeparator(udn_value, "'")

	return split_result
}

// SECOND STAGE: Recursive function, tracked by depth int.  Inserts sequentially into next_processing_udn_list (list[string]), each of the compound nested items, starting with the inner-most first, and then working out, so that all compound statements can be sequentially processed, with the inner-most getting processed before their immediate next-outer layer, which is the proper order
func _SplitCompoundStatements(db *sql.DB, udn_schema map[string]interface{}, source_array []string) []string {
	//UdnLog(udn_schema, "\nSplit: Compound: %v\n\n", source_array)

	// Split Open Compound
	split_result := _SplitStringArray(source_array, "(")

	// Split Close Compound
	split_result = _SplitStringArray(split_result, ")")

	return split_result
}

// THIRD STAGE: Linear function, iterating over the THIRD STAGE's list[string], list values are collected as argument variables for their respective UDN processing sections
func _SplitStatementLists(db *sql.DB, udn_schema map[string]interface{}, source_array []string) []string {
	//UdnLog(udn_schema, "\nSplit: Lists: %v\n\n", source_array)

	// Split Open Compound
	split_result := _SplitStringArray(source_array, "[")

	// Split Close Compound
	split_result = _SplitStringArray(split_result, "]")

	return split_result
}

// FOURTH STAGE: Linear function, iterating over the SECOND STAGE's list[string], map values are collected as argument variables for their respective UDN processing sections
func _SplitStatementMaps(db *sql.DB, udn_schema map[string]interface{}, source_array []string) []string {
	//UdnLog(udn_schema, "\nSplit: Maps: %v\n\n", source_array)

	// Split Open Compound
	split_result := _SplitStringArray(source_array, "{")

	// Split Close Compound
	split_result = _SplitStringArray(split_result, "}")

	return split_result
}

// FIFTH STAGE: Linear function, iterating over the THIRD STAGE's list[string], list values are collected as argument variables for their respective UDN processing sections
func _SplitStatementMapKeyValues(db *sql.DB, udn_schema map[string]interface{}, source_array []string) []string {
	//UdnLog(udn_schema, "\nSplit: Map Key Values: %v\n\n", source_array)

	return source_array
}

// SIXTH STAGE: Linear function, iterating over the FIRST STAGE's list[string] sequence of compound-nested-items.  This populates a new list[string] which now includes the split items at each compound-layer, which means we have a full set of UDN statements that will be processed at the end of this function
func _SplitStatementItems(db *sql.DB, udn_schema map[string]interface{}, source_array []string) []string {
	//UdnLog(udn_schema, "\nSplit: Items: %v\n\n", source_array)

	// Split Open Compound
	split_result := _SplitStringArray(source_array, ".")

	return split_result
}

// SEVENTH STAGE: Linear function, iterating over the THIRD STAGE's list[string], list values are collected as argument variables for their respective UDN processing sections
func _DepthTagList(db *sql.DB, udn_schema map[string]interface{}, source_array []string) []string {
	//UdnLog(udn_schema, "\nSplit: Lists: %v\n\n", source_array)

	return source_array
}

// Need to pass in all the Widget data as well, so we have it as a pool of data to be accessed from UDN

// Cookies, Headers, URI Params, JSON Body Payload, etc, must be passed in also, so we have access to all of it

// All of this data should be passed in through 'udn_data', which will be the storage system for all of these

/*

Concurrency:

[
	[
		[SourceA1, TargetA1]
		[SourceA2, TargetA2]
	],
	[
		[SourceB1, TargetB1]
	]
]


__query.1.{username=(__get.header.user.username)}  -->  __set_.userstuff.{id=__hash.(__get.header.user.username), other=...}



*/
