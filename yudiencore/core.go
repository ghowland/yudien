package yudiencore

import (
	"container/list"
	"fmt"
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
	type_int				= iota
	type_float				= iota
	type_string				= iota
	type_string_force		= iota	// This forces it to a string, even if it will be ugly, will print the type of the non-string data too.  Testing this to see if splitting these into 2 yields better results.
	type_array				= iota	// []interface{} - takes: lists, arrays, maps (key/value tuple array, strings (single element array), ints (single), floats (single)
	type_map				= iota	// map[string]interface{}
)


var PartTypeName map[int]string



type UdnPart struct {
	Depth          int
	PartType       int

	Value          string

	// List of UdnPart structs, list is easier to use dynamically
	//TODO(g): Switch this to an array.  Lists suck...
	Children       *list.List

	Id             string

	// Puts the data here after it's been evaluated
	ValueFinal     interface{}
	ValueFinalType int

	// Allows casting the type, not sure about this, but seems useful to cast ints from strings for indexing.  We'll see
	CastValue      string

	ParentUdnPart *UdnPart
	NextUdnPart   *UdnPart

	// For block functions (ex: Begin: __iterate, End: __end_iterate).  For each block begin/end, save them during parsing, so we know which __end_ function ends which block, if there are multiple per UDN statement
	BlockBegin	  *UdnPart
	BlockEnd	  *UdnPart
}



func NewUdnPart() UdnPart {
	return UdnPart{
		Children: list.New(),
	}
}

func (part *UdnPart) String() string {
	output := ""

	if part.PartType == part_function {
		output += fmt.Sprintf("%s: %s [%s]\n", PartTypeName[part.PartType], part.Value, part.Id)
	} else {
		output += fmt.Sprintf("%s: %s\n", PartTypeName[part.PartType], part.Value)
	}

	return output
}


type UdnResult struct {
	// This is the result
	Result interface{}

	Type int

	// This is the next UdnPart to process.  If nil, the executor will just continue from current UdnPart.NextUdnPart
	NextUdnPart *UdnPart

	// Error messages, we will stop processing if not nil
	Error string
}


// Returns a function that starts with the value string, which doesnt have a BlockBegin/BlockEnd set yet
func (start_udn_part *UdnPart) FindBeginBlock(value string) *UdnPart {
	cur_udn := start_udn_part

	// Go up parents of parts, until we find a matching value, with no BlockBegin set, return in-place
	done := false
	for done == false {
		// If this is a matching function value, and it isnt already assigned to a Block
		if cur_udn.PartType == part_function && cur_udn.Value == value && cur_udn.BlockBegin == nil {
			return cur_udn
		}

		// If we are out of parents to go up to, we are done
		if cur_udn.ParentUdnPart == nil {
			done = true
		} else {
			// Else, go up to our parent
			cur_udn = cur_udn.ParentUdnPart
		}
	}

	// Failed to find the correct part, returning the first part we were given (which is ignored, because its not the right part)
	return start_udn_part
}

// Returns the new Function, added to the previous function chain
func (udn_parent *UdnPart) AddFunction(value string) *UdnPart {
	//UdnLog(udn_schema, "UdnPart: Add Function: Parent: %s   Function: %s\n", udn_parent.Value, value)

	new_part := NewUdnPart()
	new_part.ParentUdnPart = udn_parent

	new_part.Depth = udn_parent.Depth

	new_part.PartType = part_function
	new_part.Value = value

	new_part.Id = fmt.Sprintf("%p", &new_part)


	// Because this is a function, it is the NextUdnPart, which is how flow control is performed
	udn_parent.NextUdnPart = &new_part

	// If this is an End Block "__end_" function, mark it and find it's matching Being and mark that
	if strings.HasPrefix(value, "__end_") {
		// We are the end of ourselves
		new_part.BlockEnd = &new_part

		// Walk backwards and find the Begin Block which doesnt have an End Block yet
		start_function_arr := strings.Split(value, "__end_")
		start_function := "__" + start_function_arr[1]
		//UdnLog(udn_schema, "  Starting function: %v\n", start_function)

		// Find the begin block, if this is the block we were looking for, tag it
		begin_block_part := udn_parent.FindBeginBlock(start_function)
		if begin_block_part.Value == start_function && begin_block_part.BlockBegin == nil {
			// Set the begin block to this new function's BlockBegin
			new_part.BlockBegin = begin_block_part

			// Set the Begin and End on the being block as well, so both parts are tagged
			begin_block_part.BlockBegin = begin_block_part
			begin_block_part.BlockEnd = &new_part
		} else {
			panic(fmt.Sprintf("ERROR: Incorrect grammar.  Missing open function for: %s\n", value))
		}
	}

	return &new_part
}

// Returns the new Child, added to the udn_parent
func (udn_parent *UdnPart) AddChild(part_type int, value string) *UdnPart {
	//UdnLog(udn_schema, "UdnPart: Add Child: Parent: %s   Child: %s (%d)\n", udn_parent.Value, value, part_type)

	new_part := NewUdnPart()
	new_part.ParentUdnPart = udn_parent

	new_part.Depth = udn_parent.Depth + 1

	new_part.PartType = part_type
	new_part.Value = value

	// Add to current chilidren
	udn_parent.Children.PushBack(&new_part)

	return &new_part
}