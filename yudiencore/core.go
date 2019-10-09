package yudiencore

import (
	"container/list"
	"fmt"
	"strings"
	"io/ioutil"
	"encoding/json"
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

type DynamicResult struct {
	// This is the result
	Result interface{}

	Type int

	// Error messages
	Error string
}


var Debug_Udn bool
var Debug_Udn_Api bool
var Debug_Udn_Log_Level int

var PartTypeName map[int]string

type UdnPart struct {
	Depth    int
	PartType int

	Value string

	// List of UdnPart structs, list is easier to use dynamically
	//TODO(g): Switch this to an array.  Lists suck...
	Children *list.List

	Id string

	// Puts the data here after it's been evaluated
	ValueFinal     interface{}
	ValueFinalType int

	// Allows casting the type, not sure about this, but seems useful to cast ints from strings for indexing.  We'll see
	CastValue string

	ParentUdnPart *UdnPart
	NextUdnPart   *UdnPart

	// For block functions (ex: Begin: __iterate, End: __end_iterate).  For each block begin/end, save them during parsing, so we know which __end_ function ends which block, if there are multiple per UDN statement
	BlockBegin *UdnPart
	BlockEnd   *UdnPart
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
	Result interface{} `json:"result"`

	Type int `json:"type"`

	// This is the next UdnPart to process.  If nil, the executor will just continue from current UdnPart.NextUdnPart
	NextUdnPart *UdnPart `json:"next_udp_part,omitempty"`

	// Error messages, we will stop processing if not nil
	Error string `json:"error,omitempty"`
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
			UdnError(nil, "ERROR: Incorrect grammar.  Missing open function for: %s\n", value)
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


func UdnError(udn_schema map[string]interface{}, format string, args ...interface{}) {
	// Format the incoming Printf args, and print them
	output := fmt.Sprintf("ERROR: " + format, args...)

	fmt.Print(output)

	// Append the output into our udn_schema["debug_log"], where we keep raw logs, before wrapping them up for debugging visibility purposes
	if udn_schema != nil {
		udn_schema["error_log"] = udn_schema["error_log"].(string) + output
	}
}

func UdnLogHtml(udn_schema map[string]interface{}, log_level int, format string, args ...interface{}) {
	UdnLogLevel(udn_schema, log_level, format, args)

	if (Debug_Udn || udn_schema["udn_debug"].(bool)) && udn_schema["allow_logging"].(bool) {
		// Format the incoming Printf args, and print them
		output := fmt.Sprintf(format, args...)
		fmt.Print(output)

		// Append the output into our udn_schema["debug_log"], where we keep raw logs, before wrapping them up for debugging visibility purposes
		udn_schema["debug_log"] = udn_schema["debug_log"].(string) + output
		// Append to HTML as well, so it shows up.  This is a convenience function for this reason.  Headers and stuff.
		udn_schema["debug_output_html"] = udn_schema["debug_output_html"].(string) + "<pre>" + HtmlClean(output) + "</pre>"
	}
}


func HtmlClean(html string) string {
	html = strings.Replace(html, "<", "&lt;", -1)
	html = strings.Replace(html, ">", "&gt;", -1)
	html = strings.Replace(html, "&", "&amp;", -1)
	html = strings.Replace(html, " ", "&nbsp;", -1)

	return html
}





func UdnDebugWriteHtml(udn_schema map[string]interface{}) string {
	if Debug_Udn || udn_schema["udn_debug"] == true {
		fmt.Printf("\n\n\n\n-=-=-=-=-=- UDN Debug Write HTML -=-=-=-=-=-\n\n\n\n")
	}

	//TODO(g): Make this unique, time in milliseconds should be OK (and sequential), so we can have more than one.  Then deal with cleanup.  And make a sub directory...
	output_path := "/tmp/udn_debug_log.html"

	// Process any remaining HTML chunk as well
	UdnDebugIncrementChunk(udn_schema)

	err := ioutil.WriteFile(output_path, []byte(udn_schema["debug_output_html"].(string)), 0644)
	if err != nil {
		UdnError(nil, err.Error())
	}

	// Clear the schema info
	//TODO(g): This only works for concurrency at the moment because I get the udn_schema every request, which is wasteful.  So work that out...
	UdnDebugReset(udn_schema)

	return output_path

}

func UdnDebugReset(udn_schema map[string]interface{}) {
	if Debug_Udn || udn_schema["udn_debug"] == true {
		fmt.Printf("\n\n\n\n-=-=-=-=-=- UDN Debug Reset -=-=-=-=-=-\n\n\n\n")
	}

	udn_schema["error_log"] = ""

	udn_schema["debug_log"] = ""
	udn_schema["debug_log_count"] = 0
	udn_schema["debug_html_chunk_count"] = 0
	udn_schema["debug_html_chunk"] = ""
	udn_schema["debug_output"] = ""
	udn_schema["debug_output_html"] = `
		<head>
			<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js">
			</script>
			<script>
			function ToggleDisplay(element_id) {
				var current_display = $('#'+element_id).css('display');
				if (current_display == 'none') {
					$('#'+element_id).css('display', 'block');
					//alert('Setting ' + element_id + ' to BLOCK == Current: ' + current_display)
				}
				else {
					$('#'+element_id).css('display', 'none');
					//alert('Setting ' + element_id + ' to NONE == Current: ' + current_display)
				}
			}
			</script>
		</head>
		`

}

func UdnDebugIncrementChunk(udn_schema map[string]interface{}) {
	current := udn_schema["debug_html_chunk_count"].(int)
	current++
	udn_schema["debug_html_chunk_count"] = current

	// Update the output with the current Debug Log (and clear it, as it's temporary).  This ensures anything previously undated, gets updated.
	UdnDebugUpdate(udn_schema)

	// Wrap anything we have put into our current HTML chunk, and write it to the HTML Output
	if udn_schema["debug_html_chunk"] != "" {
		// Render our HTML chunk in a hidden DIV, with a button to toggle visibility
		html_output := fmt.Sprintf("<button onclick=\"ToggleDisplay('debug_chunk_%d')\">Statement %d</button><br><br><div id=\"debug_chunk_%d\" style=\"display: none\">%s</div>\n", current, current, current, udn_schema["debug_html_chunk"])

		udn_schema["debug_output_html"] = udn_schema["debug_output_html"].(string) + html_output

		// Clear the chunk
		udn_schema["debug_html_chunk"] = ""
	}
}

func UdnDebug(udn_schema map[string]interface{}, input interface{}, button_label string, message string) {
	if Debug_Udn || udn_schema["udn_debug"] == true {
		// Increment the number of times we have done this, so we have unique debug log sections
		debug_log_count := udn_schema["debug_log_count"].(int)
		debug_log_count++
		udn_schema["debug_log_count"] = debug_log_count

		// Update the output with the current Debug Log (and clear it, as it's temporary)
		UdnDebugUpdate(udn_schema)
		// Render our input, and current UDN Data as well
		html_output := fmt.Sprintf("<pre>%s</pre><button onclick=\"ToggleDisplay('debug_state_%d')\">%s</button><br><br><div id=\"debug_state_%d\" style=\"display: none\">\n", HtmlClean(message), debug_log_count, button_label, debug_log_count)
		udn_schema["debug_html_chunk"] = udn_schema["debug_html_chunk"].(string) + html_output

		// Input
		switch input.(type) {
		case string:
			udn_schema["debug_html_chunk"] = udn_schema["debug_html_chunk"].(string) + "<pre>" + HtmlClean(input.(string)) + "</pre>"
		default:
			input_output, _ := json.MarshalIndent(input, "", "  ")
			//input_output := fmt.Sprintf("%v", input)	// Tried this to increase performance, this is not the bottleneck...
			udn_schema["debug_html_chunk"] = udn_schema["debug_html_chunk"].(string) + "<pre>" + HtmlClean(string(input_output)) + "</pre>"
		}

		// Close the DIV tag
		udn_schema["debug_html_chunk"] = udn_schema["debug_html_chunk"].(string) + "</div>"

	}
}

func UdnDebugUpdate(udn_schema map[string]interface{}) {
	debug_log_count := udn_schema["debug_log_count"].(int)

	// If we have anything in our UDN Debug Log, lets put it into a DIV we can hide, and clear it out, so we collect them in pieces
	if udn_schema["debug_log"] != "" {
		// Append to our raw output
		udn_schema["debug_output"] = udn_schema["debug_output"].(string) + udn_schema["debug_log"].(string)

		// Append to our HTML output
		html_output := fmt.Sprintf("<button onclick=\"ToggleDisplay('debug_log_%d')\">Debug</button><br><pre id=\"debug_log_%d\" style=\"display: none\">%s</pre>\n", debug_log_count, debug_log_count, HtmlClean(udn_schema["debug_log"].(string)))
		udn_schema["debug_html_chunk"] = udn_schema["debug_html_chunk"].(string) + html_output

		// Clear the debug log, as we have put it into the debug_output and debug_output_html
		udn_schema["debug_log"] = ""
	}
}

func UdnLog(udn_schema map[string]interface{}, format string, args ...interface{}) {
	if (Debug_Udn || udn_schema["udn_debug"].(bool)) && udn_schema["allow_logging"].(bool) {
		// Format the incoming Printf args, and print them
		output := fmt.Sprintf(format, args...)

		fmt.Print(output)

		// Append the output into our udn_schema["debug_log"], where we keep raw logs, before wrapping them up for debugging visibility purposes
		udn_schema["debug_log"] = udn_schema["debug_log"].(string) + output
	}
}

func UdnLogLevel(udn_schema map[string]interface{}, log_level int, format string, args ...interface{}) {
	// Function works the same as UdnLog/UdnError but allows level logging
	//TODO(z): Migrate UdnDebug functionality here later
	//TODO(z): Combine all log functions to put under UdnLogLevel
	if log_level <= Debug_Udn_Log_Level {
		output := fmt.Sprintf(format, args...)

		if log_level == log_error && udn_schema != nil{
			output = fmt.Sprintf("ERROR: " + format, args...)
			udn_schema["error_log"] = udn_schema["error_log"].(string) + output
		}
		if log_level >= log_debug && udn_schema != nil{
			// Append the output into our udn_schema["debug_log"], where we keep raw logs, before wrapping them up for debugging visibility purposes
			udn_schema["debug_log"] = udn_schema["debug_log"].(string) + output
		}

		fmt.Print(output)
	}
}

func ParseUdnLogLevel(level string) int {
	level = strings.ToLower(level)

	switch level {
	case "error":
		return log_error
	case "warn":
		return log_warn
	case "info":
		return log_info
	case "debug":
		return log_debug
	case "trace":
		return log_trace
	default:
		return log_off
	}
}
