package yudien

import (
	"bytes"
	"container/list"
	"database/sql"
	"encoding/json"
	"fmt"
	. "github.com/ghowland/ddd/ddd"
	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudiendata"
	. "github.com/ghowland/yudien/yudienutil"
	"github.com/junhsieh/goexamples/fieldbinding/fieldbinding"
	"github.com/segmentio/ksuid"
	"log"
	"strconv"
	"strings"
	"text/template"
	"time"
	"os/exec"
	"net/http"
	"io/ioutil"
	"encoding/base64"
	"github.com/google/go-cmp/cmp"
	"math"
)

func UDN_Comment(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_Login(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}

	username := GetResult(args[0], type_string).(string)
	password := GetResult(args[1], type_string).(string)

	ldap_user := LdapLogin(username, password)

	user_map := make(map[string]interface{})

	// This is for using this without LDAP.
	//TODO(z): Allow multiple static users from config file rather than just admin for now
	ldap_override_admin := true

	// Get the user data, if they authed
	if ldap_user.IsAuthenticated == true {
		user_map["first_name"] = ldap_user.FirstName
		user_map["full_name"] = ldap_user.FullName
		user_map["email"] = ldap_user.Email
		user_map["home_dir"] = ldap_user.HomeDir
		user_map["uid"] = ldap_user.Uid
		user_map["username"] = ldap_user.Username
		user_map["groups"] = ldap_user.Groups
		user_map["error"] = ""

		// Store it in UDN global as well
		//TODO(g): Save into the DB as our User Session...
		udn_data["ldap_user"] = user_map

		UdnLogLevel(udn_schema, log_info,"LDAP Authenticated: %s\n\n", user_map["username"])

	} else if user, user_found := DevelopmentUsers[username]; user_found && ldap_override_admin && username == user.Username && password == user.Password {
		user_map["first_name"] = user.Data.FirstName
		user_map["full_name"] = user.Data.FirstName + " " + user.Data.LastName
		user_map["email"] = user.Data.Email
		user_map["home_dir"] = user.Data.HomeDir
		user_map["uid"] = user.Data.Uid
		user_map["username"] = user.Data.Username
		user_map["groups"] = user.Data.Groups
		user_map["error"] = ""

		ldap_user.Username = user_map["username"].(string)

		UdnLogLevel(udn_schema, log_info,"LDAP Override: Admin User\n\n")

	} else {
		user_map["error"] = ldap_user.Error

		result.Result = user_map
		result.Error = ldap_user.Error

		UdnLogLevel(udn_schema, log_error, "LDAP ERROR: %s\n\n", result.Error)

		return result
	}

	// Get the user (if it exists)
	filter := map[string]interface{}{}
	filter["name"] = []interface{}{"=", ldap_user.Username}

	filter_options := make(map[string]interface{})
	user_data_result := DatamanFilter("user", filter, filter_options)

	UdnLogLevel(udn_schema, log_debug, "DatamanFilter: RESULT: %v\n", user_data_result)

	var user_data map[string]interface{}

	if len(user_data_result) == 0 {
		// Need to create this user
		user_data = make(map[string]interface{})
		user_data["name"] = ldap_user.Username
		user_data["email"] = ldap_user.Email
		user_data["name_full"] = ldap_user.FullName
		user_data["user_domain_id"] = 1 //TODO(g): Make dynamic

		// Save the LDAP data
		user_map_json, err := json.Marshal(user_map)
		if err != nil {
			UdnLogLevel(udn_schema, log_error,  "Cannot marshal User Map data: %s\n", err)
		}
		user_data["ldap_data_json"] = string(user_map_json)

		// Save the new user into the DB
		options_map := make(map[string]interface{})
		user_data = DatamanSet("user", user_data, options_map)

	} else {
		//TODO(g): Remove once I can use filters...
		for _, user_data_item := range user_data_result {
			if user_data_item["name"] == ldap_user.Username {
				// Save this user
				user_data = user_data_item

			}
		}
	}

	// Get the web_user_session
	web_user_session := map[string]interface{}{}
	filter = make(map[string]interface{})
	filter["user_id"] = []interface{}{"=", user_data["_id"]}
	filter["web_site_id"] = []interface{}{"=", 1} //TODO(g): Make dynamic
	filter_options = make(map[string]interface{})
	web_user_session_filter := DatamanFilter("web_user_session", filter, filter_options)

	if len(web_user_session_filter) == 0 {
		// If we dont have a session, create one
		web_user_session["user_id"] = user_data["_id"]
		web_user_session["web_site_id"] = 1 //TODO(g): Make dynamic

		//TODO(g): Something better than a UUID here?  I think its the best option actually.  Could salt it with a digest...
		id := ksuid.New()
		web_user_session["name"] = id.String()

		// Save the new user session
		options_map := make(map[string]interface{})
		web_user_session = DatamanSet("web_user_session", web_user_session, options_map)

	} else {
		// Save the session information
		web_user_session = web_user_session_filter[0]
	}

	//TODO(g): Ensure they have a user account in our DB, save the ldap_user data, update UDN with their session data...


	//TODO(g): Login returns the SESSION_ID

	result.Result = web_user_session["name"]

	return result
}

func UDN_DddRender(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "DDD Render: %v\n\nInput: %s\n\n", args, JsonDump(input))

	position_location := GetResult(args[0], type_string).(string)
	move_x := GetResult(args[1], type_int).(int64)
	move_y := GetResult(args[2], type_int).(int64)
	is_delete := GetResult(args[3], type_int).(int64)
	ddd_id := GetResult(args[4], type_int).(int64)
	data_location := GetResult(args[5], type_string).(string)          // The data (record) we are operating on should be at this location
	save_data := GetResult(args[6], type_map).(map[string]interface{}) // This is incoming data, and will be only for the position_location's data, not the complete record
	temp_id := GetResult(args[7], type_int).(int64)                    // Initial value is passed in as 0, not empty string or nil

	UdnLogLevel(udn_schema, log_trace, "\nDDD Render: Position: %s  Move X: %d  Y: %d  Is Delete: %d  DDD: %d  Data Location: %s\nSave Data:\n%s\n\n", position_location, move_x, move_y, is_delete, ddd_id, data_location, JsonDump(save_data))

	//TEST: Add some static rows...
	input_map := input.(map[string]interface{})
	input_map_rows := input_map["form"].([]interface{})

	//TODO(g): Process the move_x/y with position location.  Get a new position location.  Do this same thing with the buttons, and test each one for validity to see if we should add that button
	//		Just update the string with the move, then do the get.  Makes it simple, no working 2 things at once.  String is manipulated, and get.  That's it.

	// -- Do work here to change stuff

	// Move, if we need to
	position_location = DddMove(position_location, move_x, move_y)
	UdnLogLevel(udn_schema, log_trace, "DDD Render: After move: %s\n", position_location)

	// Get the DDD Data (record data) from our stored location (first time) or from the temp table subsequent times
	ddd_data := make(map[string]interface{})

	// Get our DDD data, so we can cache it and use it without having to query it many times
	ddd_options := make(map[string]interface{})
	ddd_data_record := DatamanGet("ddd", int(ddd_id), ddd_options)
	ddd_data = ddd_data_record["data_json"].(map[string]interface{})

	var data_record interface{}

	// If we dont have a temp_id, then we will get the data from data_location and store it into the temp table
	if temp_id == 0 {
		// Get the data we are working on
		data_record_args := make([]interface{}, 0)
		data_record_args = append(data_record_args, data_location)
		data_record = MapGet(data_record_args, udn_data)
		UdnLogLevel(udn_schema, log_trace, "DddRender: Data Record: %s: %s\n\n", data_location, JsonDump(data_record))

		// Put this data into the temp table, and get our temp_id
		temp_data := make(map[string]interface{})
		temp_data["data_json"] = JsonDump(data_record)
		options_map := make(map[string]interface{})
		temp_data_result := DatamanSet("temp", temp_data, options_map)
		UdnLogLevel(udn_schema, log_trace, "Temp data result: %v\n\n", temp_data_result)
		temp_id = temp_data_result["_id"].(int64)
	} else {
		// Get the ddd_data from the temp table
		temp_options := make(map[string]interface{})
		temp_record := DatamanGet("temp", int(temp_id), temp_options)

		err := json.Unmarshal([]byte(temp_record["data_json"].(string)), &data_record)
		if err != nil {
			UdnLogLevel(udn_schema, log_error, "UDN_DddRender: Failed to parse JSON: %s", err)
		}
	}
	//fmt.Printf("DDD Data Record: (%d): %s\n\n", temp_id, JsonDump(data_record))

	// Get the DDD node, which has our
	ddd_label, ddd_node, ddd_cursor_data := DddGetNode(position_location, ddd_data, data_record, udn_data)

	UdnLogLevel(udn_schema, log_trace, "DDD Node: %s\n\n", JsonDump(ddd_node))
	UdnLogLevel(udn_schema, log_trace, "DDD Cursor Data: %s\n\n", JsonDump(ddd_cursor_data))

	// -- Done changing stuff, time to RENDER!

	// Render this DDD Spec Node
	ddd_spec_render_nodes := DddRenderNode(position_location, ddd_id, temp_id, ddd_label, ddd_node, ddd_cursor_data)
	if ddd_spec_render_nodes != nil {
		input_map_rows = append(input_map_rows, ddd_spec_render_nodes)
	}

	// New Row
	new_row_html := make([]interface{}, 0)

	// HTML Descriptive content  -- Showing the position so I can test it...
	new_html_field := map[string]interface{}{
		"color":       "primary",
		"icon":        "icon-make-group",
		"info":        "",
		"label":       "Position Location",
		"name":        "position_location",
		"placeholder": "",
		"size":        "12",
		"type":        "html",
		"value":       fmt.Sprintf("<b>Cursor:</b> %s<br>%s", position_location, SnippetData(JsonDump(ddd_node), 80)),
	}
	new_row_html = AppendArray(new_row_html, new_html_field)

	// Add buttons
	input_map_rows = AppendArray(input_map_rows, new_row_html)

	// New Row
	new_row_buttons := make([]interface{}, 0)

	// Add buttons
	new_button := map[string]interface{}{
		"color":       "primary",
		"icon":        "icon-arrow-up8",
		"info":        "",
		"label":       "Move Up",
		"name":        "move_up",
		"placeholder": "",
		"size":        "2",
		"type":        "button",
		"onclick":     fmt.Sprintf("$(this).closest('.ui-dialog-content').dialog('close'); RPC('/api/dwi_render_ddd', {'move_x': 0, 'move_y': -1, 'position_location': '%s', 'ddd_id': %d, 'is_delete': 0, 'web_data_widget_instance_id': '{{{_id}}}', 'web_widget_instance_id': '{{{web_widget_instance_id}}}', '_web_data_widget_instance_id': 34, 'dom_target_id':'dialog_target', 'temp_id': %d})", position_location, ddd_id, temp_id),
		"value":       "",
	}
	// Check if the button is valid, by getting an item from this
	if _, test_node, _ := DddGetNode(DddMove(position_location, 0, -1), ddd_data, data_record, udn_data); test_node == nil {
		new_button["color"] = ""
		new_button["onclick"] = ""
	}
	new_row_buttons = AppendArray(new_row_buttons, new_button)

	new_button = map[string]interface{}{
		"color":       "primary",
		"icon":        "icon-arrow-down8",
		"info":        "",
		"label":       "Move Down",
		"name":        "move_down",
		"placeholder": "",
		"size":        "2",
		"type":        "button",
		"onclick":     fmt.Sprintf("$(this).closest('.ui-dialog-content').dialog('close'); RPC('/api/dwi_render_ddd', {'move_x': 0, 'move_y': 1, 'position_location': '%s', 'ddd_id': %d, 'is_delete': 0, 'web_data_widget_instance_id': '{{{_id}}}', 'web_widget_instance_id': '{{{web_widget_instance_id}}}', '_web_data_widget_instance_id': 34, 'dom_target_id':'dialog_target', 'temp_id': %d})", position_location, ddd_id, temp_id),
		"value":       "",
	}
	// Check if the button is valid, by getting an item from this
	if _, test_node, _ := DddGetNode(DddMove(position_location, 0, 1), ddd_data, data_record, udn_data); test_node == nil {
		new_button["color"] = ""
		new_button["onclick"] = ""
	}
	new_row_buttons = AppendArray(new_row_buttons, new_button)

	new_button = map[string]interface{}{
		"color":       "primary",
		"icon":        "icon-arrow-left8",
		"info":        "",
		"label":       "Move Left",
		"name":        "move_left",
		"placeholder": "",
		"size":        "2",
		"type":        "button",
		"onclick":     fmt.Sprintf("$(this).closest('.ui-dialog-content').dialog('close'); RPC('/api/dwi_render_ddd', {'move_x': -1, 'move_y': 0, 'position_location': '%s', 'ddd_id': %d, 'is_delete': 0, 'web_data_widget_instance_id': '{{{_id}}}', 'web_widget_instance_id': '{{{web_widget_instance_id}}}', '_web_data_widget_instance_id': 34, 'dom_target_id':'dialog_target', 'temp_id': %d})", position_location, ddd_id, temp_id),
		"value":       "",
	}
	// Check if the button is valid, by getting an item from this
	if len(position_location) == 1 {
		new_button["color"] = ""
		new_button["onclick"] = ""
	}
	new_row_buttons = AppendArray(new_row_buttons, new_button)

	new_button = map[string]interface{}{
		"color":       "primary",
		"icon":        "icon-arrow-right8",
		"info":        "",
		"label":       "Move Right",
		"name":        "move_right",
		"placeholder": "",
		"size":        "2",
		"type":        "button",
		"onclick":     fmt.Sprintf("$(this).closest('.ui-dialog-content').dialog('close'); RPC('/api/dwi_render_ddd', {'move_x': 1, 'move_y': 0, 'position_location': '%s', 'ddd_id': %d, 'is_delete': 0, 'web_data_widget_instance_id': '{{{_id}}}', 'web_widget_instance_id': '{{{web_widget_instance_id}}}', '_web_data_widget_instance_id': 34, 'dom_target_id':'dialog_target', 'temp_id': %d})", position_location, ddd_id, temp_id),
		"value":       "",
	}
	// Check if the button is valid, by getting an item from this
	if _, test_node, _ := DddGetNode(DddMove(position_location, 1, 0), ddd_data, data_record, udn_data); test_node == nil {
		new_button["color"] = ""
		new_button["onclick"] = ""
	}
	new_row_buttons = AppendArray(new_row_buttons, new_button)

	// Add buttons
	input_map_rows = AppendArray(input_map_rows, new_row_buttons)

	// Add static JSON field
	new_item := make(map[string]interface{})
	new_item["color"] = ""
	new_item["icon"] = "icon-file-text"
	new_item["info"] = ""
	new_item["label"] = "Static JSON"
	new_item["name"] = "static_data_json"
	new_item["placeholder"] = ""
	new_item["size"] = "12"
	new_item["type"] = "ace"
	new_item["format"] = "json"
	new_item["udn_process"] = "__json_encode"
	new_item["value"] = ""

	new_row := make([]interface{}, 0)
	new_row = AppendArray(new_row, new_item)

	input_map_rows = AppendArray(input_map_rows, new_row)

	input_map["form"] = input_map_rows

	/*

		if is_delete == 1 {
			// If we are deleting this element
			DddDelete(position_location, data_location, ddd_id, udn_data)

		} else if len(save_data) > 0 {
			// Else, If we are saving this data
			DddSet(position_location, data_location, save_data, ddd_id, udn_data)
		}

		// Is this valid data?  Returns array of validation error locations
		validation_errors := DddValidate(data_location, ddd_id, udn_data)

		// If we have validation errors, move there
		if len(validation_errors) > 0 {
			error := validation_errors[0]

			// Update the location information to the specified first location
			MapSet(MakeArray(position_location), error["location"], udn_data)
		}
	*/

	//// Get the data at our current location
	//data := DddGet(position_location, data_location, ddd_id, udn_data)
	//
	//// Get DDD node, which explains our data
	//ddd_node := DddGetNode(position_location, ddd_id, udn_data)

	result := UdnResult{}
	result.Result = input_map //TODO(g): Need to modify this, which is the point of this function...

	UdnLogLevel(udn_schema, log_debug, "\nDDD Render: Result:\n%s\n\n", JsonDump(input_map))

	return result
}

func UDN_Library_Query(db *sql.DB, sql string) []interface{} {
	// Query
	rs, err := db.Query(sql)
	if err != nil {
		log.Panic(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}
	defer rs.Close()

	// create a fieldbinding object.
	var fArr []string
	fb := fieldbinding.NewFieldBinding()

	if fArr, err = rs.Columns(); err != nil {
		log.Panic(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}

	fb.PutFields(fArr)

	// Final output, array of maps
	//result_list := list.New()
	result_list := make([]interface{}, 0)

	for rs.Next() {
		if err := rs.Scan(fb.GetFieldPtrArr()...); err != nil {
			log.Panic(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
		}

		template_map := make(map[string]interface{})

		for key, value := range fb.GetFieldArr() {
			//UdnLogLevel(udn_schema, log_trace, "Found value: %s = %s\n", key, value)

			switch value.(type) {
			case []byte:
				template_map[key] = fmt.Sprintf("%s", value)
			default:
				template_map[key] = value
			}
		}

		//result_list.PushBack(template_map)
		result_list = AppendArray(result_list, template_map)
	}

	if err := rs.Err(); err != nil {
		log.Panic(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}

	return result_list
}

func UDN_QueryById(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}

	UdnLogLevel(udn_schema, log_trace, "Query: %v\n", args)

	//arg_0 := args.Front().Value.(*UdnResult)
	arg_0 := args[0]

	// The 2nd arg will be a map[string]interface{}, so ensure it exists, and get it from our args if it was passed in
	arg_1 := make(map[string]interface{})
	if len(args) > 1 {
		//UdnLogLevel(udn_schema, log_trace, "Query: %s  Stored Query: %s  Data Args: %v\n", udn_start.Value, arg_0, args[1])

		//TODO(g):VALIDATE: Validation and error handling
		arg_1 = GetResult(args[1], type_map).(map[string]interface{})
	}

	UdnLogLevel(udn_schema, log_trace, "Query: %s  Stored Query: %s  Data Args: %v\n", udn_start.Value, arg_0, arg_1)

	query_sql := fmt.Sprintf("SELECT * FROM datasource_query WHERE _id = %s", arg_0)

	//TODO(g): Make a new function that returns a list of UdnResult with map.string

	// This returns an array of TextTemplateMap, original method, for templating data
	query_result := Query(db, query_sql)

	sql_parameters := make(map[string]string)
	has_params := false
	if query_result[0]["parameter_json_data"] != nil {
		//UdnLogLevel(udn_schema, log_trace, "-- Has params: %v\n", query_result[0]["parameter_data_json"])
		err := json.Unmarshal([]byte(query_result[0]["parameter_json_data"].(string)), &sql_parameters)
		if err != nil {
			log.Panic(err)
		}
		has_params = true
	} else {
		UdnLogLevel(udn_schema, log_trace, "-- No params\n")
	}

	result_sql := fmt.Sprintf(query_result[0]["sql"].(string))

	UdnLogLevel(udn_schema, log_trace, "Query: SQL: %s   Params: %v\n", result_sql, sql_parameters)

	// If we have params, then format the string for each of them, from our arg map data
	if has_params {
		for param_key, _ := range sql_parameters {
			replace_str := fmt.Sprintf("{{%s}}", param_key)
			//value_str := fmt.Sprintf("%s", param_value)

			// Get the value from the arg_1
			value_str := fmt.Sprintf("%v", arg_1[param_key])

			//UdnLogLevel(udn_schema, log_trace, "REPLACE PARAM:  Query: SQL: %s   Replace: %s   Value: %s\n", result_sql, replace_str, value_str)

			result_sql = strings.Replace(result_sql, replace_str, value_str, -1)

			//UdnLogLevel(udn_schema, log_trace, "POST-REPLACE PARAM:  Query: SQL: %s   Replace: %s   Value: %s\n", result_sql, replace_str, value_str)
		}

		UdnLogLevel(udn_schema, log_trace, "Query: Final SQL: %s\n", result_sql)
	}

	// This query returns a list.List of map[string]interface{}, new method for more-raw data
	result.Result = UDN_Library_Query(db, result_sql)

	UdnLogLevel(udn_schema, log_trace, "Query: Result [Items: %d]: %s\n", len(result.Result.([]interface{})), SnippetData(GetResult(result, type_string), 60))

	//// DEBUG
	//result_list := result.Result.(*list.List)
	//for item := result_list.Front(); item != nil; item = item.Next() {
	//	real_item := item.Value.(map[string]interface{})
	//	UdnLogLevel(udn_schema, log_trace, "Query Result Value: %v\n", real_item)
	//}

	return result
}

func UDN_DebugOutput(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}
	result.Result = input

	type_str := fmt.Sprintf("%T", input)

	if type_str == "*list.List" {
		UdnLogLevel(udn_schema, log_debug, "Debug Output: List: %s: %v\n", type_str, SprintList(*input.(*list.List)))

	} else {
		UdnLogLevel(udn_schema, log_debug, "Debug Output: %s: %s\n", type_str, JsonDump(input))
	}

	return result
}

func UDN_True(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Return True\n")

	result := UdnResult{}
	result.Result = true

	return result
}

func UDN_False(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Return False\n")

	result := UdnResult{}
	result.Result = false

	return result
}

func UDN_TestReturn(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Test Return data: %s\n", args[0])

	result := UdnResult{}
	result.Result = args[0]

	return result
}

func UDN_Widget(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Widget: %v\n", args[0])

	udn_data_page := udn_data["page"].(map[string]interface{})

	result := UdnResult{}
	//result.Result = udn_data["widget"].Map[arg_0.Result.(string)]
	result.Result = udn_data_page[args[0].(string)] //TODO(g): We get this from the page map.  Is this is the best naming?  Check it...

	return result
}

func UDN_StringTemplateFromValueShort(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	//UdnLogLevel(udn_schema, log_trace, "\n\nShort Template: %v  Input: %v\n\n", SnippetData(args, 60), SnippetData(input, 60))
	//UdnLogLevel(udn_schema, log_trace, "\n\n--- Short Template ---: %v  Input:\n%v\n\n", SnippetData(args, 60), input)

	// If arg_1 is present, use this as the input instead of input
	actual_input := input
	if len(args) >= 2 {
		actual_input = args[1]
	}

	actual_input = GetResult(actual_input, type_map)

	/*
		// If this is an array, convert it to a string, so it is a concatenated string, and then can be properly turned into a map.
		if actual_input != nil {
			if strings.HasPrefix(fmt.Sprintf("%T", actual_input), "[]") {
				UdnLogLevel(udn_schema, log_trace, "Short Template: Converting from array to string: %s\n", SnippetData(actual_input, 60))
				actual_input = GetResult(actual_input, type_string)
			} else {
				UdnLogLevel(udn_schema, log_trace, "Short Template: Input is not an array: %s\n", SnippetData(actual_input, 60))
				//UdnLogLevel(udn_schema, log_trace, "String Template: Input is not an array: %s\n", actual_input)
			}
		} else {
			UdnLogLevel(udn_schema, log_trace, "Short Template: Input is nil\n")
		}*/

	template_str := GetResult(args[0], type_string).(string)

	//UdnLogLevel(udn_schema, log_trace, "Short Template From Value: Template String: %s Template Input: %v\n\n", SnippetData(actual_input, 60), SnippetData(template_str, 60))
	UdnLogLevel(udn_schema, log_trace, "Short Template From Value: Template Input: %s\n\n", JsonDump(actual_input))
	UdnLogLevel(udn_schema, log_trace, "Short Template From Value: Incoming Template String: %s\n\n", template_str)

	// Use the actual_input, which may be input or arg_1
	input_template_map := GetResult(actual_input, type_map).(map[string]interface{})

	for key, value := range input_template_map {
		//fmt.Printf("Key: %v   Value: %v\n", key, value)
		key_replace := fmt.Sprintf("{{{%s}}}", key)
		value_str := GetResult(value, type_string).(string)
		UdnLogLevel(udn_schema, log_trace, "Short Template From Value: Value String: %s == '%s'\n\n", key, value_str)
		template_str = strings.Replace(template_str, key_replace, value_str, -1)
	}

	result := UdnResult{}
	result.Result = template_str

	UdnLogLevel(udn_schema, log_trace, "Short Template From Value:  Result:  %v\n\n", template_str)

	return result
}

func UDN_StringTemplateFromValue(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	//UdnLogLevel(udn_schema, log_trace, "\n\nString Template: \n%v\n\n", args)

	// If arg_1 is present, use this as the input instead of input
	actual_input := input
	if len(args) >= 2 {
		actual_input = args[1]
		UdnLogLevel(udn_schema, log_trace, "String Template From Value: Template Input: Input from Arg 1: %v\n\n", SnippetData(actual_input, 60))
	}

	actual_input = GetResult(actual_input, type_map)
	UdnLogLevel(udn_schema, log_trace, "String Template From Value: Template Input: Post Conversion Input: %v\n\n", SnippetData(actual_input, 600))

	template_str := GetResult(args[0], type_string).(string)


	//TODO(g):REMOVE: Debugging problem, remove when fixed, as it should be a no-op (and problem if left in)
	//template_str = strings.Replace(template_str, "{{", "<<<", -1)
	//template_str = strings.Replace(template_str, "}}", ">>>", -1)
	//template_str = strings.Replace(template_str, "\\", "\\\\", -1)
	template_str = strings.Replace(template_str, "\\", "", -1)

	UdnLogLevel(udn_schema, log_trace, "String Template From Value: Template Input: %s Template String: %v\n\n", SnippetData(actual_input, 60), SnippetData(template_str, 600))

	UdnLogLevel(udn_schema, log_trace, "String Template From Value: Template Input: %s\n\n", JsonDump(actual_input))

	// Use the actual_input, which may be input or arg_1
	input_template := NewTextTemplateMap()
	input_template.Map = GetResult(actual_input, type_map).(map[string]interface{})

	//item_template := template.Must(template.New("text").Delims("<<<", ">>>").Parse(template_str))
	item_template := template.Must(template.New("text").Parse(template_str))

	item := StringFile{}
	err := item_template.Execute(&item, input_template)
	if err != nil {
		log.Panic(err)
	}

	result := UdnResult{}
	result.Result = item.String

	return result
}

func UDN_StringTemplateMultiWrap(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	//UdnLogLevel(udn_schema, log_trace, "\n\nString Template: \n%v\n\n", args)

	wrap_key := GetResult(args[0], type_string).(string)

	// Ensure our arg count is correct
	if len(args) < 2 {
		UdnLogLevel(udn_schema, log_error, "Wrong number of arguments.  Map Template takes N 2-tuples: set_key, map_data.  The first map_data may be skipped if there is only one set_key, input will be used.")
	} else if len(args) > 3 || len(args)%2 != 1 {
		UdnLogLevel(udn_schema, log_error, "Wrong number of arguments.  Map Template takes N 2-tuples: set_key, map_data")
	}

	items := (len(args) - 1) / 2

	current_output := ""

	// If arg_1 is present, use this as the input instead of input
	current_input := input
	if len(args) >= 3 {
		current_input = GetResult(args[2], type_map).(map[string]interface{})
	}

	for count := 0; count < items; count++ {
		offset := count * 2

		// Use the input we already had set up before this for loop for the actual_input, initially, every other iteration use our arg map data
		if count > 0 {
			current_input = GetResult(args[offset+2], type_map).(map[string]interface{})

			// Every iteration, after the first, we set the previous current_output to the "value" key, which is the primary content (by convention) in our templates
			current_input.(map[string]interface{})[wrap_key] = current_output
		}

		// Prepare to template
		template_str := GetResult(args[offset+1], type_string).(string)

		UdnLogLevel(udn_schema, log_trace, "String Template From Value: Template String: %s Template Input: %v\n\n", SnippetData(current_input, 60), SnippetData(template_str, 60))

		// Use the actual_input, which may be input or arg_1
		input_template := NewTextTemplateMap()
		input_template.Map = GetResult(current_input, type_map).(map[string]interface{})

		item_template := template.Must(template.New("text").Parse(template_str))

		item := StringFile{}
		err := item_template.Execute(&item, input_template)
		if err != nil {
			log.Panic(err)
		}

		// Set the current_output for return, and put it in our udn_data, so we can access it again
		current_output = item.String
	}

	result := UdnResult{}
	result.Result = current_output

	return result
}

func UDN_MapStringFormat(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Map String Format: %v\n", args)

	// Ensure our arg count is correct
	if len(args) < 2 || len(args)%2 != 0 {
		UdnLogLevel(udn_schema, log_error, "Wrong number of arguments.  Map Template takes N 2-tuples: set_key, format")
	}

	items := len(args) / 2

	for count := 0; count < items; count++ {
		offset := count * 2

		set_key := GetResult(args[offset+0], type_string).(string)
		format_str := GetResult(args[offset+1], type_string).(string)

		UdnLogLevel(udn_schema, log_trace, "Format: %s  Format String: %s  Input: %v\n", set_key, SnippetData(format_str, 60), SnippetData(input, 60))

		if input != nil {
			input_template := NewTextTemplateMap()
			input_template.Map = input.(map[string]interface{})

			item_template := template.Must(template.New("text").Parse(format_str))

			item := StringFile{}
			err := item_template.Execute(&item, input_template)
			if err != nil {
				log.Panic(err)
			}

			// Save the templated string to the set_key in our input, so we are modifying our input
			input.(map[string]interface{})[set_key] = item.String

			UdnLogLevel(udn_schema, log_trace, "Format: %s  Result: %s\n\n", set_key, item.String)
		} else {
			input.(map[string]interface{})[set_key] = format_str

			UdnLogLevel(udn_schema, log_trace, "Format: %s  Result (No Templating): %s\n\n", set_key, format_str)
		}

	}

	result := UdnResult{}
	result.Result = input

	UdnLogLevel(udn_schema, log_trace, "Map String Format: Result: %s\n\n", JsonDump(input))

	return result
}

func UDN_MapTemplate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Map Template: %v\n", args)

	// Ensure our arg count is correct
	if len(args) < 3 || len(args)%3 != 0 {
		UdnLogLevel(udn_schema, log_error, "Wrong number of arguments.  Map Template takes N 3-tuples: set_key, text, map")
	}

	items := len(args) / 3

	for count := 0; count < items; count++ {
		offset := count * 3

		set_key := args[offset].(string)
		template_str := GetResult(args[offset+1], type_string).(string)
		template_data := GetResult(args[offset+2], type_map).(map[string]interface{})

		UdnLogLevel(udn_schema, log_trace, "Map Template: %s Template String: %s Template Data: %v Template Input: %v\n\n", set_key, SnippetData(template_str, 60), SnippetData(template_data, 60), SnippetData(input, 60))

		input_template := NewTextTemplateMap()
		input_template.Map = template_data

		item_template := template.Must(template.New("text").Parse(template_str))

		item := StringFile{}
		err := item_template.Execute(&item, input_template)
		if err != nil {
			log.Panic(err)
		}

		// Save the templated string to the set_key in our input, so we are modifying our input
		input.(map[string]interface{})[set_key] = item.String
	}

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_MapUpdate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	update_map := GetResult(args[0], type_map).(map[string]interface{})

	// Update the input map's fields with the arg0 map
	UdnLogLevel(udn_schema, log_trace, "Map Update: %s  Over Input: %s\n", SnippetData(update_map, 60), SnippetData(input, 60))

	for k, v := range update_map {
		input.(map[string]interface{})[k] = v
	}

	result := UdnResult{}
	result.Result = input

	UdnLogLevel(udn_schema, log_debug, "Map Update: Result: %v", input)

	return result
}

func UDN_MapTemplateKey(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	input_map := GetResult(input, type_map).(map[string]interface{})


	template_str := GetResult(args[0], type_string).(string)
	template_map := GetResult(args[1], type_map).(map[string]interface{})

	if len(args) > 2 {
		input_map = GetResult(args[2], type_map).(map[string]interface{})
	}

	// Update the input map's fields with the arg0 map
	UdnLogLevel(udn_schema, log_trace, "Map Template Key: %s  Template Map: %s  Input Map: %s\n", SnippetData(template_str, 60), SnippetData(template_map, 60), SnippetData(input_map, 60))

	output_map := make(map[string]interface{})

	// Loop over our
	for k, v := range input_map {
		// Add this key to "key" value, so we can use it in our template as well
		template_map["key"] = k

		UdnLogLevel(udn_schema, log_trace, "Map Template Key: Template Map: %s\n", SnippetData(template_map, 60))



		// Use the actual_input, which may be input or arg_1
		input_template := NewTextTemplateMap()
		input_template.Map = template_map

		//item_template := template.Must(template.New("text").Delims("<<<", ">>>").Parse(template_str))
		item_template := template.Must(template.New("text").Parse(template_str))

		item := StringFile{}
		err := item_template.Execute(&item, input_template)
		if err != nil {
			log.Panic(err)
		}

		template_key := item.String

		output_map[template_key] = v
	}

	result := UdnResult{}
	result.Result = output_map

	UdnLogLevel(udn_schema, log_debug, "Map Update: Result: %v", input)

	return result
}

func UDN_HtmlEncode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "HTML Encode: %v\n", SnippetData(input, 80))

	input_str := GetResult(input, type_string).(string)

	// Replace all the characters with their fixed HTML alternatives
	input_str = strings.Replace(input_str, "&", "&amp;", -1)
	input_str = strings.Replace(input_str, "<", "&lt;", -1)
	input_str = strings.Replace(input_str, ">", "&gt;", -1)

	result := UdnResult{}
	result.Result = input_str

	//UdnLogLevel(udn_schema, log_trace, "HTML Encode: Result: %v\n", SnippetData(input_str, 80))
	UdnLogLevel(udn_schema, log_trace, "HTML Encode: Result: %v\n", input_str)

	return result
}

func UDN_StringAppend(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Append: %v\n", args)

	// If we only have 1 argument, and it contains dots, we need to break this into a set of args
	if len(args) == 1 && strings.Contains(args[0].(string), ".") {
		args = SimpleDottedStringToArray(args[0].(string))
	}

	// Get the string we are going to append to
	access_str := ""
	access_result := UDN_Get(db, udn_schema, udn_start, args, input, udn_data)
	if access_result.Result != nil {
		access_str = GetResult(access_result.Result, type_string).(string)
	} else {
		access_str = ""
	}

	UdnLogLevel(udn_schema, log_trace, "String Append: %v  Current: %s  Append (%T): %s\n\n", args, SnippetData(access_str, 60), input, SnippetData(input, 60))

	// Append
	access_str = fmt.Sprintf("%s%s", access_str, GetResult(input, type_string).(string))

	//UdnLogLevel(udn_schema, log_trace, "String Append: %v  Appended:\n%s\n\n", args, access_str)		//DEBUG

	// Save the appended string
	UDN_Set(db, udn_schema, udn_start, args, access_str, udn_data)

	result := UdnResult{}
	result.Result = access_str

	return result
}

func UDN_StringClear(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Clear: %v\n", args)

	// arg_0 is always a string that needs to be broken up into a list, so that we can pass it as args to Set
	//arg_0 := args.Front().Value.(*UdnResult).Result.(string)
	arg_0 := GetResult(args[0], type_string).(string)

	// Create a list of UdnResults, so we can pass them as args to the Set command
	udn_result_args := SimpleDottedStringToArray(arg_0)

	// Clear
	result := UdnResult{}
	result.Result = ""

	// Save the appended string
	UDN_Set(db, udn_schema, udn_start, udn_result_args, "", udn_data)

	return result
}

func UDN_StringReplace(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Replace: %v   Input: %s\n", args, SnippetData(input, 60))

	input_string := GetResult(input, type_string).(string)

	// arg_0 is always a string that needs to be broken up into a list, so that we can pass it as args to Set
	//arg_0 := args.Front().Value.(*UdnResult).Result.(string)
	arg_0 := GetResult(args[0], type_string).(string)
	arg_1 := GetResult(args[1], type_string).(string)

	// Create a list of UdnResults, so we can pass them as args to the Set command
	result_string := strings.Replace(input_string, arg_0, arg_1, -1)

	// Clear
	result := UdnResult{}
	result.Result = result_string

	return result
}

func UDN_StringConcat(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Concat: %v\n", args)

	output := ""

	// Loop over the items in the input
	//for item := input.Result.(*list.List).Front(); item != nil; item = item.Next() {
	for _, item := range input.([]interface{}) {
		output += fmt.Sprintf("%v", item)
	}

	// Input is a pass-through
	result := UdnResult{}
	result.Result = output

	return result
}

func UDN_StringSplit(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Concat: %v\n", args)

	input_str := GetResult(input, type_string).(string)

	separator := GetResult(args[0], type_string).(string)

	//max_split := int64(0)
	//if len(args) > 1 {
	//	max_split = GetResult(args[1], type_int).(int64)
	//}


	// Input is a pass-through
	result := UdnResult{}
	result.Result = strings.Split(input_str, separator)

	return result
}

func UDN_StringJoin(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Join: %v\n", args)

	separator := GetResult(args[0], type_string).(string)

	result := UdnResult{}
	if input_str_array, ok := input.([] string); ok {
		result.Result = strings.Join(input_str_array, separator)
	} else {
		result.Error = fmt.Sprintf("Expected []string but got: %v", input)
	}

	return result
}

func UDN_StringLower(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Lower: %v\n", args)

	arg_0 := GetResult(args[0], type_string).(string)

	result := UdnResult{}
	result.Result = strings.ToLower(arg_0)

	return result
}

func UDN_StringUpper(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String Upper: %v\n", args)

	arg_0 := GetResult(args[0], type_string).(string)

	result := UdnResult{}
	result.Result = strings.ToUpper(arg_0)

	return result
}

func UDN_Input(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// If we have no arguments, return our input as the result.  This is used for passing our input into a function argument
	if len(args) == 0 {
		result := UdnResult{}
		result.Result = input
		UdnLogLevel(udn_schema, log_trace, "Input: No args, returning input: %v\n", input)
		return result
	}

	UdnLogLevel(udn_schema, log_trace, "Input: %v\n", args[0])

	result := UdnResult{}
	result.Result = args[0]

	return result
}

func UDN_InputGet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	cur_result := input

	UdnLogLevel(udn_schema, log_trace, "Input Get: %v   Input: %v\n", args, SnippetData(input, 60))

	for _, arg := range args {
		switch arg.(type) {
		case string:
			cur_result = cur_result.(map[string]interface{})[arg.(string)]
		default:
			//TODO(g): Support ints?  Make this a stand alone function, and just call it from the UDN function
			cur_result = nil
		}
	}

	result := UdnResult{}
	result.Result = cur_result

	return result
}

func UDN_StoredFunction(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Stored Function: %s\n", SnippetData(args, 80))

	function_name := GetResult(args[0], type_string).(string)

	function_domain_id := udn_data["web_site"].(map[string]interface{})["udn_stored_function_domain_id"]

	sql := fmt.Sprintf("SELECT * FROM udn_stored_function WHERE name = '%s' AND udn_stored_function_domain_id = %d", function_name, function_domain_id)

	function_rows := Query(db, sql)

	// Get all our args, after the first one (which is our function_name)
	udn_data["function_arg"] = GetResult(args[1:], type_map)

	//UdnLogLevel(udn_schema, log_trace, "Stored Function: Args: %d: %s\n", len(udn_data["function_arg"].(map[string]interface{})), SprintMap(udn_data["function_arg"].(map[string]interface{})))

	// Our result, whether we populate it or not
	result := UdnResult{}

	if len(function_rows) > 0 {
		result.Result = ProcessSchemaUDNSet(db, udn_schema, function_rows[0]["udn_data_json"].(string), udn_data)
	}

	return result
}

func UDN_Execute(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	// Try from input
	udn_target := GetResult(input, type_string).(string)

	// If we have an argument, override input
	if len(args) > 0 {
		udn_target = GetResult(args[0], type_string).(string)
	}

	UdnLogLevel(udn_schema, log_trace, "Execute: UDN String As Target: %v\n", udn_target)

	// Execute the Target against the input
	result := UdnResult{}
	//result.Result = ProcessUDN(db, udn_schema, udn_source, udn_target, udn_data)

	// Extract the JSON into a list of list of lists (2), which gives our execution blocks, and UDN pairs (Source/Target)
	udn_execution_group := UdnExecutionGroup{}

	// Decode the JSON data for the widget data
	err := json.Unmarshal([]byte(udn_target), &udn_execution_group.Blocks)
	if err != nil {
		// Process the UDN as a single string, as it wasnt in the UDN array format
		result.Result = ProcessSingleUDNTarget(db, udn_schema, udn_target, input, udn_data)
	} else {
		result.Result = ProcessSchemaUDNSet(db, udn_schema, udn_target, udn_data)
	}

	return result
}

func UDN_Uuid(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	uuid := ksuid.New()

	UdnLogLevel(udn_schema, log_trace, "UUID: %s\n", uuid.String())

	// Execute the Target against the input
	result := UdnResult{}
	result.Result = uuid.String()

	return result

}

func UDN_ArraySlice(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// UdnLogLevel(udn_schema, log_trace, "Slice: %v\n", SnippetData(args, 80))

	result := UdnResult{}

	start_index := 0
	end_index := 0
	args_len := len(args)
	input_len := 0

	// Find len of input array
	switch input.(type){
	case []string:
		input_len = len(input.([]string))
	case []interface{}:
		input_len = len(input.([]interface{}))
	case []map[string]interface{}:
		input_len = len(input.([]map[string]interface{}))
	default: // Cannot recognize input array type. Return input
		result.Result = input
		return result
	}

	// Check that start and end indices are present & given in int format
	if args_len == 0 { // No index given, return input
		result.Result = input
		return result
	} else if args_len >= 1 { // Set start index
		switch args[0].(type){
		case string:
			start_int, err := strconv.Atoi(args[0].(string))

			if err == nil{
				start_index = start_int
				end_index = input_len // If only start index given. Assume end_index is at end of array.
			} else {
				result.Result = input
				return result
			}
		case int:
			start_index = args[0].(int)
			end_index = input_len
		case float64:
			start_index = int(args[0].(float64))
			end_index = input_len
		default:
			result.Result = input
			return result
		}

	}

	if args_len > 1 { // Both start and end indices are given - start_index is already set
		switch args[1].(type){
		case string:
			end_int, err := strconv.Atoi(args[1].(string))

			if err == nil{
				end_index = end_int
			} else {
				result.Result = input
				return result
			}
		case int:
			end_index = args[1].(int)
		case float64:

			end_index = int(args[1].(float64))
		default:
			result.Result = input
			return result
		}
	}

	// Make sure that the start & end index are correct - Order of these error checks matter
	// Implement negative indexing (not default in Go)
	if start_index < 0 {
		start_index = input_len + start_index + 1 // -1 is the last element (start at -1 and not -0)
	}
	if end_index < 0 {
		end_index = input_len + end_index + 1
	}

	// Check for out of bounds - force the index to be in range
	if start_index > input_len {
		start_index = input_len
	}
	if start_index < 0 { // If start_index is still < 0 after negative adjustment, then it is out of bounds
		start_index = 0
	}
	if end_index > input_len {
		end_index = input_len
	}
	if end_index < 0 {
		end_index = 0
	}
	// Check that end index is not before start index (index error)
	if end_index < start_index { // Return empty array
		start_index = 0
		end_index = 0
	}

	// Finally, return the slice of array
	switch input.(type){
	case []string:
		result.Result = input.([]string)[start_index:end_index]
	case []interface{}:
		result.Result = input.([]interface{})[start_index:end_index]
	case []map[string]interface{}:
		result.Result = input.([]map[string]interface{})[start_index:end_index]
	default: // Cannot recognize input array type. Return input
		result.Result = input
	}

	return result
}

func UDN_Increment(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Increment: %v\n", args)

	result := UdnResult{}

	value := GetResult(input, type_int).(int64)

	increment := int64(1)

	// First value represents the diff if it exists
	if len(args) >= 1 {
		increment = GetResult(args[0], type_int).(int64)
	}

	value += increment

	result.Result = value

	return result
}

func UDN_Decrement(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Decrement: %v\n", args)

	result := UdnResult{}

	value := GetResult(input, type_int).(int64)

	decrement := int64(1)

	// First value represents the diff if it exists
	if len(args) >= 1 {
		decrement = GetResult(args[0], type_int).(int64)
	}

	value -= decrement

	result.Result = value

	return result
}

func UDN_ArrayAppend(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLogLevel(udn_schema, log_trace, "Array Append: %v\n", args)

	// Get whatever we have stored at that location
	array_value_potential := MapGet(args, udn_data)

	// Force it into an array
	array_value := GetResult(array_value_potential, type_array).([]interface{})

	// Append the input into our array
	array_value = AppendArray(array_value, input)

	// Save the result back into udn_data
	MapSet(args, array_value, udn_data)

	// Return the array
	result := UdnResult{}
	result.Result = array_value

	UdnLogLevel(nil, log_trace, "Array Append: Final: %v: %s\n", args, JsonDump(array_value))

	return result
}

func UDN_ArrayDivide(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	divisor, err := strconv.Atoi(args[0].(string))

	// Dont process this, if it isnt valid...  Just pass through
	if err != nil || divisor <= 0 {
		UdnLogLevel(udn_schema, log_trace, "ERROR: Divisor is invalid: %d\n", divisor)
		result := UdnResult{}
		result.Result = input
		return result
	}

	UdnLogLevel(udn_schema, log_trace, "Array Divide: %v\n", divisor)

	// Make the new array.  This will be a 2D array, from our 1D input array
	result_array := make([]interface{}, 0)
	current_array := make([]interface{}, 0)

	// Loop until we have taken account of all the elements in the array
	for count, element := range input.([]interface{}) {
		if count%divisor == 0 && count > 0 {
			result_array = AppendArray(result_array, current_array)
			current_array = make([]interface{}, 0)

			UdnLogLevel(udn_schema, log_trace, "Adding new current array: %d\n", len(result_array))
		}

		current_array = AppendArray(current_array, element)
		UdnLogLevel(udn_schema, log_trace, "Adding new current array: Element: %d\n", len(current_array))
	}

	if len(current_array) != 0 {
		result_array = AppendArray(result_array, current_array)
	}

	result := UdnResult{}
	result.Result = result_array

	return result
}

func UDN_ArrayMapRemap(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	arg_0 := args[0]
	remap := GetResult(arg_0, type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_trace, "Array Map Remap: %v\n", remap)

	new_array := make([]interface{}, 0)

	for _, old_map := range input.([]map[string]interface{}) {
		new_map := make(map[string]interface{})

		// Remap all the old map keys to new map keys in the new map
		for new_key, old_key := range remap {
			new_map[new_key] = old_map[old_key.(string)]
		}

		// Add the new map to the new array
		new_array = AppendArray(new_array, new_map)
	}

	result := UdnResult{}
	result.Result = new_array

	return result
}

func UDN_ArrayMapFind(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	arg_0 := args[0]
	find_map := GetResult(arg_0, type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_trace, "Array Map Find: %v in %d Record(s)\n", find_map, len(input.([]map[string]interface{})))

	result := UdnResult{}
	found_value := false

	// Find and return the first item that matches
	for _, item := range input.([]map[string]interface{}) {
		all_keys_matched := true

		// Remap all the old map keys to new map keys in the new map
		for key, value := range find_map {
			UdnLogLevel(udn_schema, log_trace, "Array Map Find: Key %s: %s == %s\n", key, SnippetData(item[key], 20), SnippetData(value, 20))
			if CompareUdnData(item[key], value) == 0 {
				all_keys_matched = false
				break
			}
		}

		if all_keys_matched {
			UdnLogLevel(udn_schema, log_trace, "Array Map Find: Found: %s\n", SnippetData(item, 200))
			found_value = true
			result.Result = item
			break
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Array Map Find: No Matches Found\n")
		result.Result = nil
	}

	return result
}


func UDN_ArrayMapFindUpdate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "TODO(g): UDN_ArrayMapFindUpdate called UDN_ArrayMapFilterUpdate.  Depricated.\n")

	result := UDN_ArrayMapFilterUpdate(db, udn_schema, udn_start, args, input, udn_data)

	return result
}

func UDN_ArrayMapFilterUpdate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	filter_map := GetResult(args[0], type_map).(map[string]interface{})
	update_map := GetResult(args[1], type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_trace, "Array Map Filter Update: %v in %d Record(s): Update: %v\n", filter_map, len(input.([]map[string]interface{})), SnippetData(update_map, 40))

	found_value := false

	// Find and return the first item that matches
	for _, item := range input.([]map[string]interface{}) {
		all_keys_matched := true

		// Remap all the old map keys to new map keys in the new map
		for key, value := range filter_map {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Update: Key %s: %s == %s\n", key, SnippetData(item[key], 20), SnippetData(value, 20))
			if CompareUdnData(item[key], value) == 0 {
				all_keys_matched = false
				break
			}
		}

		if all_keys_matched {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Update: Found: %s\n", SnippetData(item, 200))

			// Update the map
			for k, v := range update_map {
				item[k] = v
			}

			// We have found at least 1 item
			found_value = true

			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Update: Found: After: %s\n", SnippetData(item, 200))
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Array Map Filter Update: No Matches Found\n")
	}

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_ArrayMapFilterIn(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	filter_map := GetResult(args[0], type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_trace, "Array Map Filter In: %v in %d Record(s)\n", filter_map, len(input.([]map[string]interface{})))

	found_value := false

	result_array := make([]map[string]interface{}, 0)

	// Find and return the first item that matches
	for _, item := range input.([]map[string]interface{}) {
		any_keys_matched := true

		// Remap all the old map keys to new map keys in the new map
		for key, value := range filter_map {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter In: Key %s: %s == %s\n", key, SnippetData(item[key], 20), SnippetData(value, 20))
			if CompareUdnData(item[key], value) == 0 {
				any_keys_matched = false
				break
			}
		}

		if any_keys_matched {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter In: Matched: %s\n", SnippetData(item, 200))

			// Add the item to the result array
			result_array = append(result_array, item)

			// We have found at least 1 item
			found_value = true

			UdnLogLevel(udn_schema, log_trace, "Array Map Filter In: Found: After: %s\n", SnippetData(item, 200))
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Array Map Filter In: No Matches Found\n")
	}

	result := UdnResult{}
	result.Result = result_array

	return result
}

func UDN_ArrayMapFilterContains(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	filter_map := GetResult(args[0], type_map).(map[string]interface{})
	options_map := make(map[string]interface{})
	if len(args) > 1 {
		options_map = GetResult(args[1], type_map).(map[string]interface{})
	}

	UdnLogLevel(udn_schema, log_trace, "Array Map Filter Contains: %v in %d Record(s)\n", filter_map, len(input.([]map[string]interface{})))

	found_value := false

	result_array := make([]map[string]interface{}, 0)

	// Find and return the first item that matches
	for _, item := range input.([]map[string]interface{}) {
		// If we have more than one args, and arent explicitly turning "all" off, then assume we want to match them all (any of them)
		if len(filter_map) > 1 && options_map["all"] != nil && options_map["all"] != false {
			options_map["all"] = true
		}

		// Each record matches at least 1 key
		any_keys_matched := false

		// We need to match
		any_keys_matched_from_all := true

		// Remap all the old map keys to new map keys in the new map
		for key, value := range filter_map {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Contains: Key %s: %s == %s\n", key, SnippetData(item[key], 20), SnippetData(value, 20))

			value_list := value.([]interface{})

			switch item[key].(type) {
			case []interface{}:
				found_item_match := false

				for _, item_value := range item {

					found_value_match := false
					for _, value_value := range value_list {
						if CompareUdnData(value_value, item_value) == 0 {
							found_value_match = true
							any_keys_matched = true
							break
						}

						if found_value_match {
							found_item_match = true
							break
						} else {
							// We didn't match any of the values, so not all records are met
							any_keys_matched_from_all = false

							// Dont continue if we are
							if options_map["all"] != nil && options_map["all"] == true {
								break
							}
						}
					}
				}

				if found_item_match {

				}
			}

			if CompareUdnData(item[key], value) == 0 {
				any_keys_matched = false
				break
			}
		}

		if any_keys_matched {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Contains: Matched: %s\n", SnippetData(item, 200))

			if options_map["all"] != nil && options_map["all"] == true {
				UdnLogLevel(udn_schema, log_trace, "Array Map Filter Contains: Matched: All Matched: %v\n", any_keys_matched_from_all)
				// If we got an "any" match, from every one of our filter keys
				if any_keys_matched_from_all {
					// Add the item to the result array
					result_array = append(result_array, item)
				}
			} else {
				// Add the item to the result array
				result_array = append(result_array, item)
			}

			// We have found at least 1 item
			found_value = true

			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Contains: Found: After: %s\n", SnippetData(item, 200))
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Array Map Filter Contains: No Matches Found\n")
	}

	result := UdnResult{}
	result.Result = result_array

	return result
}

func UDN_ArrayMapFilterArrayContains(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	map_key := GetResult(args[0], type_string).(string)
	filter_map := GetResult(args[1], type_map).(map[string]interface{})
	options_map := make(map[string]interface{})
	if len(args) > 2 {
		options_map = GetResult(args[2], type_map).(map[string]interface{})
	}

	UdnLogLevel(udn_schema, log_trace, "Array Map Filter Array Contains: Key: %s: %v in %d Record(s)\n", map_key, filter_map, len(input.([]map[string]interface{})))

	found_value := false

	result_array := make([]map[string]interface{}, 0)

	// Find and return the first item that matches
	for _, item := range input.([]map[string]interface{}) {
		// If we have more than one args, and arent explicitly turning "all" off, then assume we want to match them all (any of them)
		if len(filter_map) > 1 && options_map["all"] != nil && options_map["all"] != false {
			options_map["all"] = true
		}

		// Each record matches at least 1 key
		any_keys_matched := false

		// We need to match
		//any_keys_matched_from_all := true

		// Remap all the old map keys to new map keys in the new map
		for key, value := range filter_map {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Array Contains: Key %s: %s == %s\n", key, SnippetData(item[key], 20), SnippetData(value, 20))

			value_list := value.([]interface{})

			switch item[key].(type) {
			case []interface{}:
				found_item_match := false

				map_array_item := item[map_key].([]interface{})

				// Look in the map's key for an array
				for _, item_value := range map_array_item {

					found_value_match := false
					for _, value_value := range value_list {
						if CompareUdnData(value_value, item_value) == 0 {
							found_value_match = true
							any_keys_matched = true
							break
						}

						if found_value_match {
							found_item_match = true
							break
						} else {
							//// We didn't match any of the values, so not all records are met
							//any_keys_matched_from_all = false

							//// Dont continue if we are
							//if options_map["all"] != nil && options_map["all"] == true {
							//	break
							//}
						}
					}
				}

				if found_item_match {

				}
			}

			if CompareUdnData(item[key], value) == 0 {
				any_keys_matched = false
				break
			}
		}

		if any_keys_matched {
			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Array Contains: Matched: %s\n", SnippetData(item, 200))

			/*
			if options_map["all"] != nil && options_map["all"] == true {
				UdnLogLevel(udn_schema, log_trace, "Array Map Find Match: Matched: All Matched: %v\n", any_keys_matched_from_all)
				// If we got an "any" match, from every one of our filter keys
				if any_keys_matched_from_all {
					// Add the item to the result array
					result_array = append(result_array, item)
				}
			} else {
				// Add the item to the result array
				result_array = append(result_array, item)
			}
			*/

			// Add the item to the result array
			result_array = append(result_array, item)

			// We have found at least 1 item
			found_value = true

			UdnLogLevel(udn_schema, log_trace, "Array Map Filter Array Contains: Found: After: %s\n", SnippetData(item, 200))
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Array Map Filter Array Contains: No Matches Found\n")
	}

	result := UdnResult{}
	result.Result = result_array

	return result
}


func UDN_MapFilterArrayContains(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	input_map := GetResult(input, type_map).(map[string]interface{})

	// Get the remapping information
	filter_map := GetResult(args[0], type_map).(map[string]interface{})
	//options_map := make(map[string]interface{})
	//if len(args) > 1 {
	//	options_map = GetResult(args[1], type_map).(map[string]interface{})
	//}

	UdnLogLevel(udn_schema, log_trace, "Map Filter Array Contains:: %v in %d Record(s)\n", filter_map, len(input_map))

	found_value := false

	result_map := make(map[string]interface{})

	// Remap all the old map keys to new map keys in the new map
	for input_test_map_key, input_test_map := range input_map {

		record_matched := false

		for filter_key, filter_key_value := range filter_map {

			UdnLogLevel(udn_schema, log_trace, "Map Filter Array Contains: Key %s: %s == %s\n", input_test_map_key, SnippetData(input_map[input_test_map_key], 20), SnippetData(filter_key_value, 20))

			// If this input_map test element has the filter key
			if input_test_map.(map[string]interface{})[filter_key] != nil {
				test_list := GetResult(input_test_map.(map[string]interface{})[filter_key], type_array).([]interface{})
				filter_list := GetResult(filter_key_value, type_array).([]interface{})

				any_keys_matched := true

				for _, test_list_value := range test_list {
					for _, filter_list_value := range filter_list {
						UdnLogLevel(udn_schema, log_trace, "Map Filter Array Contains: Key %s: Value: %s == %s\n", input_test_map_key, SnippetData(test_list_value, 20), SnippetData(filter_list_value, 20))

						if CompareUdnData(test_list_value, filter_list_value) == 0 {
							any_keys_matched = true
							break
						} else {
							//any_keys_not_matched = true
						}

					}
				}

				//TODO(g): Do options for more than "any key in any filter list" in the future.  For now, thats all I need
				if any_keys_matched {
					record_matched = true
				}
			}



			// If any of the keys match, add this input_map[key] to the result_map
			if record_matched {
				// Add the item to the result array
				result_map[input_test_map_key] = input_map[input_test_map_key]

				// We found at least one match with our filter
				found_value = true
			}
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Map Filter Array Contains: No Matches Found\n")
	}

	result := UdnResult{}
	result.Result = result_map

	return result
}

func UDN_MapFilterKey(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	input_map := GetResult(input, type_map).(map[string]interface{})

	// Get the remapping information
	filter_key_list := GetResult(args[0], type_array).([]interface{})
	//options_map := make(map[string]interface{})
	//if len(args) > 1 {
	//	options_map = GetResult(args[1], type_map).(map[string]interface{})
	//}

	UdnLogLevel(udn_schema, log_trace, "Map Filter Key: %v in %d Record(s)\n", filter_key_list, len(input_map))

	found_value := false

	result_map := make(map[string]interface{})


	for _, filter_key := range filter_key_list {

		filter_key_str := filter_key.(string)

		UdnLogLevel(udn_schema, log_trace, "Map Filter Key: Key %s: %s\n", filter_key, SnippetData(input_map, 20))

		record_matched := false

		// If this input_map test element has the filter key
		if input_map[filter_key_str] != nil {
			record_matched = true
		}

		// If any of the keys match, add this input_map[key] to the result_map
		if record_matched {
			// Add the item to the result array
			result_map[filter_key_str] = input_map[filter_key_str]

			// We found at least one match with our filter
			found_value = true
		}
	}

	// If we didn't find the record, we return nil
	if !found_value {
		UdnLogLevel(udn_schema, log_trace, "Map Filter Key: No Matches Found\n")
	}

	result := UdnResult{}
	result.Result = result_map

	return result
}


// Update all map's key's values with a template statement from each map's key/values
func UDN_ArrayMapTemplate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	result := UdnResult{}

	if len(args) % 2 != 0 {
		UdnLogLevel(udn_schema, log_trace, "ERROR: ArrayMapTemplate: Args should be in pairs of 2: Length: %d\n", len(args))
		result.Result = input
		return result
	}

	for _, item := range input.([]map[string]interface{}) {
		for count := 0 ; count < len(args) / 2 ; count ++ {
			key_str := GetResult(args[count * 2], type_string).(string)
			template_str := GetResult(args[count * 2 + 1], type_string).(string)

			// Use the actual_input, which may be input or arg_1
			input_template := NewTextTemplateMap()
			input_template.Map = item

			//item_template := template.Must(template.New("text").Delims("<<<", ">>>").Parse(template_str))
			item_template := template.Must(template.New("text").Parse(template_str))

			result_str := StringFile{}
			err := item_template.Execute(&result_str, input_template)
			if err != nil {
				log.Panic(err)
			}

			// Save the resulting templated string back into the input array of maps
			item[key_str] = result_str.String
		}
	}

	result.Result = input

	return result
}

func UDN_ArrayMapKeySet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	result := UdnResult{}

	if len(args) % 2 != 0 {
		UdnLogLevel(udn_schema, log_trace, "ERROR: ArrayMapKeySet: Args should be in pairs of 2: Length: %d\n", len(args))
		result.Result = input
		return result
	}

	for _, item := range input.([]map[string]interface{}) {
		for count := 0 ; count < len(args) / 2 ; count ++ {
			key_str := GetResult(args[count * 2], type_string).(string)
			value := args[count * 2 + 1]

			// Save the value into the item's keys
			item[key_str] = value
		}
	}

	result.Result = input

	return result
}




func UDN_ArrayMapToMap(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	input_value := input.([]map[string]interface{})

	// Get the remapping information
	map_key := GetResult(args[0], type_string).(string)

	if len(args) > 1 {
		input_value = GetResult(args[1], type_array).([]map[string]interface{})
	}

	UdnLogLevel(udn_schema, log_trace, "Array Map To Map: %s in %d Record(s)\n", map_key, len(input_value))

	result := UdnResult{}
	result.Result = MapArrayToMap(input_value, map_key)


	return result
}

func UDN_ArrayRemove(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	array_value_potential := MapGet(args, udn_data)

	// Force it into an array
	array_value := GetResult(array_value_potential, type_array).([]interface{})

	UdnLogLevel(udn_schema, log_trace, "Array Remove: %v  FROM  %v\n", input, array_value)
	UdnLogLevel(udn_schema, log_trace, "Array Remove: Array Value Length: %d\n", len(array_value))

	new_array := make([]interface{}, 0)

	found_index := -1
	for index, value := range array_value {
		//TODO(g): Is this a good enough comparison?  What about map to map?  Content of the map?
		if cmp.Equal(value, input) {
			UdnLogLevel(udn_schema, log_trace, "Array Remove: Found: Value: %s\n", JsonDump(value))
			UdnLogLevel(udn_schema, log_trace, "Array Remove: Found: Input: %s\n", JsonDump(input))
			found_index = index
			break
		}
	}

	if found_index != -1 {
		UdnLogLevel(udn_schema, log_trace, "Array Remove: %v  Found Index: %d\n", input, found_index)
		UdnLogLevel(udn_schema, log_trace, "Array Remove: Array Value: %s\n", JsonDump(array_value))

		for item_index, item := range array_value {
			if item_index != found_index {
				new_array = append(new_array, item)
			}
		}

		/*
		if len(array_value) == 1 {
			UdnLogLevel(udn_schema, log_trace, "Array Remove: Array Value: 000: Clearing the array, last element\n")

		} else if found_index+1 != len(array_value) {
			new_array = append(array_value[:found_index], array_value[found_index+1:]...)
			UdnLogLevel(udn_schema, log_trace, "Array Remove: Array Value: 111: %s\n", JsonDump(array_value))
		} else {
			new_array = array_value[:found_index]
			UdnLogLevel(udn_schema, log_trace, "Array Remove: Array Value: 222: %s\n", JsonDump(array_value))
		}*/
	}

	result := UdnResult{}
	result.Result = new_array

	MapSet(args, new_array, udn_data)

	UdnLogLevel(nil, log_trace, "Array Remove: Final: %v: %s\n", args, JsonDump(new_array))

	return result
}

func UDN_ArrayContains(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Get the remapping information
	array_value_potential := MapGet(args, udn_data)

	// Force we want to check into an array
	array_value := GetResult(array_value_potential, type_array).([]interface{})

	// Force the input into an array
	array_input := GetResult(input, type_array).([]interface{})

	UdnLogLevel(udn_schema, log_trace, "Array Contains: %v  IN  %v\n", array_input, array_value)
	UdnLogLevel(udn_schema, log_trace, "Array Contains: Array Value Length: %d\n", len(array_value))

	found_all := true
	for _, input_item := range array_input {
		found_item := false
		for _, value := range array_value {
			//TODO(g): Is this a good enough comparison?  What about map to map?  Content of the map?
			if cmp.Equal(value, input_item) {
				UdnLogLevel(udn_schema, log_trace, "Array Contains: Value: %s\n", JsonDump(value))
				UdnLogLevel(udn_schema, log_trace, "Array Contains: Input: %s\n", JsonDump(input))
				found_item = true
				break
			}
		}

		// If we didnt find this item, we didnt find them all, fail and return
		if !found_item {
			found_all = false
			break
		}
	}

	result := UdnResult{}
	result.Result = found_all

	return result
}

func UDN_ArrayIndex(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	arg_0 := args[0]

	UdnLogLevel(udn_schema, log_trace, "Array Index: %v\n", arg_0)

	found_index := -1		// -1 is not found

	array_value_potential := MapGet(args, udn_data)

	// Force it into an array
	array_value := GetResult(array_value_potential, type_array).([]interface{})

	for index, value := range array_value {
		//TODO(g): Is this a good enough comparison?  What about map to map?  Content of the map?
		if cmp.Equal(value, input) {
			found_index = index
			break
		}
	}

	result := UdnResult{}
	result.Result = found_index

	return result
}

func UDN_RenderDataWidgetInstance(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//TODO(g): Take arg3 as optional argument, which is a map of control values.  Allow "dialog=true" to wrap any result in a dialog window.  This will allow non-dialog items to be rendered in a dialog.
	//

	//TODO(g): Make Dialog Form use this and change it to Form.  Then it is ready to be used in a normal page, and I can just wrap it with a Dialog...  Pass in the dialog title and any options (width).
	//

	UdnLogLevel(udn_schema, log_trace, "Render Data Widget Instance: %v\n", args)

	dom_target_id_str := GetResult(args[0], type_string).(string)
	web_data_widget_instance_id := GetResult(args[1], type_int).(int64)
	widget_instance_update_map := GetResult(args[2], type_map).(map[string]interface{})
	udn_update_map := make(map[string]interface{})
	if len(args) > 3 {
		udn_update_map = GetResult(args[3], type_map).(map[string]interface{})
	}

	// Make this work, we can just fake the data format so it works the same as the page rendering...
	fake_site_page_widget := make(map[string]interface{})
	fake_site_page_widget["name"] = dom_target_id_str
	fake_site_page_widget["web_data_widget_instance_id"] = web_data_widget_instance_id
	fake_site_page_widget["web_widget_instance_output"] = "output." + dom_target_id_str

	// Get the web_data_widget_instance data
	sql := fmt.Sprintf("SELECT * FROM web_data_widget_instance WHERE _id = %d", web_data_widget_instance_id)
	web_data_widget_instance := Query(db, sql)[0]

	// Decode JSON static
	decoded_instance_json := make(map[string]interface{})
	if web_data_widget_instance["static_data_json"] != nil {
		err := json.Unmarshal([]byte(web_data_widget_instance["static_data_json"].(string)), &decoded_instance_json)
		if err != nil {
			log.Panic(err)
		}
	}
	udn_data["data_instance_static"] = decoded_instance_json

	// Get the web_data_widget data
	sql = fmt.Sprintf("SELECT * FROM web_data_widget WHERE _id = %d", web_data_widget_instance["web_data_widget_id"])
	web_data_widget := Query(db, sql)[0]

	// Decode JSON static
	decoded_json := make(map[string]interface{})
	if web_data_widget["static_data_json"] != nil {
		err := json.Unmarshal([]byte(web_data_widget["static_data_json"].(string)), &decoded_json)
		if err != nil {
			log.Panic(err)
		}
	}
	udn_data["data_static"] = decoded_json

	// If we dont have this bucket yet, make it
	if udn_data["widget_instance"] == nil {
		udn_data["widget_instance"] = make(map[string]interface{})
	}

	// Loop over all the keys in the widget_instance_update_map, and update them into the widget_instance
	for key, value := range widget_instance_update_map {
		udn_data["widget_instance"].(map[string]interface{})[key] = value
	}

	// Loop over all the keys in the udn_update_map, and update them directly into the udn_data.  This is for overriding things like "widget_static", which is initialized earlier
	for key, value := range udn_update_map {
		UdnLogLevel(udn_schema, log_trace, "Render Data Widget Instance: Update udn_data: %s: %v\n", key, value)
		udn_data[key] = value
	}

	// Render the Widget Instance, from the web_data_widget_instance
	RenderWidgetInstance(db, udn_schema, udn_data, fake_site_page_widget, udn_update_map)

	// Prepare the result from the well-known target output location (dom_target_id_str)
	result := UdnResult{}
	result.Result = udn_data["output"].(map[string]interface{})[dom_target_id_str].(string)

	// Store this result in a well-known location which can be returned as JSON output as well
	api_result := make(map[string]interface{})
	api_result[dom_target_id_str] = result.Result
	udn_data["set_api_result"] = api_result

	return result
}

func UDN_JsonDecode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "JSON Decode: %v   Input: %v\n", args, SnippetData(input, 300))

	// Use the argument instead of input, if it exists
	if len(args) != 0 {
		input = args[0]
	}

	//decoded_map := make(map[string]interface{})
	var decoded_interface interface{}

	if input != nil {
		//err := json.Unmarshal([]byte(input.(string)), &decoded_map)
		err := json.Unmarshal([]byte(input.(string)), &decoded_interface)
		if err != nil {
			log.Panic(err)
		}
	}

	result := UdnResult{}
	//result.Result = decoded_map
	result.Result = decoded_interface

	UdnLogLevel(udn_schema, log_trace, "JSON Decode: Result: %v\n", decoded_interface)
	UdnLogLevel(udn_schema, log_trace, "JSON Decode: Result: %s\n", SnippetData(decoded_interface, 120))

	return result
}

func UDN_JsonEncode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "JSON Encode: %v\n", args)

	// Use the argument instead of input, if it exists
	if len(args) != 0 {
		input = args[0]
	}

	/*	var buffer bytes.Buffer
		body, _ := json.MarshalIndent(input, "", "  ")
		buffer.Write(body)
	*/
	result := UdnResult{}
	result.Result = JsonDump(input)

	UdnLogLevel(udn_schema, log_trace, "JSON Encode: Result: %v\n", result.Result)

	return result
}

func UDN_JsonEncodeData(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "JSON Encode: %v\n", args)

	// Use the argument instead of input, if it exists
	if len(args) != 0 {
		input = args[0]
	}

	var buffer bytes.Buffer
	body, _ := json.Marshal(input)
	buffer.Write(body)

	result := UdnResult{}
	result.Result = buffer.String()

	UdnLogLevel(udn_schema, log_trace, "JSON Encode: Result: %v\n", result.Result)

	return result
}

func UDN_Base64Decode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Base64 Decode: %v   Input: %v\n", args, SnippetData(input, 300))

	// Use the argument instead of input, if it exists
	if len(args) != 0 {
		input = args[0]
	}

	decoded_bytes, _ := base64.URLEncoding.DecodeString(input.(string))
	decoded := string(decoded_bytes)

	result := UdnResult{}
	result.Result = JsonDump(decoded)

	UdnLogLevel(udn_schema, log_trace, "Base64 Decode: Result: %v\n", decoded)
	UdnLogLevel(udn_schema, log_trace, "Base64 Decode: Result: %s\n", SnippetData(decoded, 120))

	return result
}

func UDN_Base64Encode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Base64 Encode: %v\n", args)

	// Use the argument instead of input, if it exists
	if len(args) != 0 {
		input = args[0]
	}

	encoded := base64.URLEncoding.EncodeToString([]byte(input.(string)))

	result := UdnResult{}
	result.Result = encoded

	UdnLogLevel(udn_schema, log_trace, "Base64 Encode: Result: %v\n", result.Result)

	return result
}

func UDN_GetIndex(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLogLevel(udn_schema, log_trace, "Get Index: %v\n", SnippetData(args, 80))

	result := UdnResult{}

	if len(args) > 0 {
		result.Result = MapGet(args, input)
	} else {
		result.Result = input
	}

	//UdnLogLevel(udn_schema, log_trace, "Get Index: %v   Result: %v\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_SetIndex(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLogLevel(udn_schema, log_trace, "Set: %v   Input: %s\n", SnippetData(args, 80), SnippetData(input, 40))

	result := UdnResult{}

	args_len := len(args)

	if args_len >= 2 {
		// args[args_len - 1] is the new value to update the input while args[0:args_len - 1] represent the target path
		result.Result = MapIndexSet(args[0:args_len - 1], args[args_len - 1], input)
	} else if args_len == 1 { // Return the only argument
		result.Result = args[0]
	} else { // Pass through input if no args passed
		result.Result = input
	}

	//UdnLogLevel(udn_schema, log_trace, "Set: %v  Result: %s\n\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_DataGet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Data Get: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)
	record_id := GetResult(args[1], type_int).(int64)

	options := make(map[string]interface{})
	if len(args) > 2 {
		options = GetResult(args[2], type_map).(map[string]interface{})
	}

	result_map := DatamanGet(collection_name, int(record_id), options)

	result := UdnResult{}
	result.Result = result_map

	return result
}

func UDN_DataSet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Data Set: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)
	record := GetResult(args[1], type_map).(map[string]interface{})
	options := GetResult(args[1], type_map).(map[string]interface{})

	result_map := DatamanSet(collection_name, record, options)

	result := UdnResult{}
	result.Result = result_map

	return result
}

func UDN_DataFilter(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Data Filter: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)
	filter := GetResult(args[1], type_map).(map[string]interface{})

	// Optionally, options
	options := make(map[string]interface{})
	if len(args) >= 3 {
		options = GetResult(args[2], type_map).(map[string]interface{})
	}

	result_list := DatamanFilter(collection_name, filter, options)

	result := UdnResult{}
	result.Result = result_list

	return result
}

func UDN_DataFilterFull(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Updated version of DataFilter. Filter is a JSON following the specs detailed in the docs of Dataman
	// DataFilter will work fine with one constraint but not multiple ones
	UdnLogLevel(udn_schema, log_trace, "Data Filter: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)

	filter := args[1] // filter could be either map[string]interface{} for single filters or []interface{} for multifilters

	// Optionally, options
	options := make(map[string]interface{})
	if len(args) >= 3 {
		options = GetResult(args[2], type_map).(map[string]interface{})
	}

	result_list := DatamanFilterFull(collection_name, filter, options)

	result := UdnResult{}
	result.Result = result_list

	return result
}

func UDN_DataDelete(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Data Delete: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)
	record_id := GetResult(args[1], type_int).(int64)

	options := make(map[string]interface{})
	if len(args) > 2 {
		options = GetResult(args[2], type_map).(map[string]interface{})
	}

	result_map := DatamanDelete(collection_name, record_id, options)

	result := UdnResult{}
	result.Result = result_map

	return result
}

func UDN_DataDeleteFilter(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// DataDelete using filter - can have multiple deletes (deletes are performed one-by-one)
	UdnLogLevel(udn_schema, log_trace, "Data Delete Filter: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)

	filter := args[1] // filter could be either map[string]interface{} for single filters or []interface{} for multifilters

	// Optionally, options
	options := make(map[string]interface{})
	if len(args) >= 3 {
		options = GetResult(args[2], type_map).(map[string]interface{})
	}

	// Find all entries to be delete
	delete_list := DatamanFilterFull(collection_name, filter, options)
	result_array := make([]map[string]interface{}, 0, 10)

	// call the singular DataDelete on each element
	for _, element := range delete_list {
		//TODO(z): For future speed improvements if needed, group deletes together if necessary
		result_map := DatamanDelete(collection_name, element["_id"].(int64), make(map[string]interface{}))
		result_array = AppendArrayMap(result_array, result_map)
	}

	result := UdnResult{}
	result.Result = result_array

	return result
}

func UDN_MapKeyDelete(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Map Key Delete: %v\n", args)

	for _, key := range args {
		delete(input.(map[string]interface{}), key.(string))
	}

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_MapKeySet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Map Key Set: %v\n", args)

	// Ensure our arg count is correct
	if len(args) < 2 || len(args)%2 != 0 {
		UdnLogLevel(udn_schema, log_error, "Wrong number of arguments.  Map Template takes N 2-tuples: set_key, format")
	}

	items := len(args) / 2

	for count := 0; count < items; count++ {
		offset := count * 2

		set_key := GetResult(args[offset+0], type_string).(string)
		value_str := GetResult(args[offset+1], type_string).(string)

		UdnLogLevel(udn_schema, log_trace, "Map Key Set: %s  Value String: %s  Input: %v\n", set_key, SnippetData(value_str, 60), SnippetData(input, 60))

		input.(map[string]interface{})[set_key] = value_str

	}

	result := UdnResult{}
	result.Result = input

	UdnLogLevel(udn_schema, log_trace, "Map Key Set: Result: %s\n\n", JsonDump(input))

	return result
}

func UDN_MapCopy(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Map Copy: %v\n", args)

	result := UdnResult{}

	if input == nil {
		return result
	}

	// Deep copy - json dump & load
	var new_map interface{}
	bytes, err := json.Marshal(input)

	if err != nil { // error in parsing source - return nil
		return result
	}

	err = json.Unmarshal(bytes, &new_map)

	if err != nil { // error in copying to new map - return nil
		return result
	}


	result.Result = new_map

	return result
}

// This is the generalized function for comparing UDN data.  Works like Python where empty map/array is equal to nil
func CompareUdnData(arg0 interface{}, arg1 interface{}) int64 {
	arg0_str := GetResult(arg0, type_string).(string)
	arg1_str := GetResult(arg1, type_string).(string)

	var value int64

	// Assume they are equal and find negation
	value = 1

	// Do basic comparison with forced strings.  Works for many cases
	if arg0_str != arg1_str {
		value = 0
	}

	UdnLogLevel(nil, log_trace, "CompareUdnData: String Compare: %v == %v :: %d\n", arg0, arg1, value)

	// arg0: Solve the unequal data type cases:  nil == [] == {}
	if arg0 == nil {
		switch arg1.(type) {
		case map[string]interface{}:
			arg1_map := arg1.(map[string]interface{})
			UdnLogLevel(nil, log_trace, "CompareUdnData: Nil Compare: %v == %v :: Arg1 Map Len: %d\n", arg0, arg1, len(arg1_map))
			if len(arg1_map) == 0 {
				value = 1
			}


		case []interface{}:
			arg1_array := arg1.([]interface{})
			UdnLogLevel(nil, log_trace, "CompareUdnData: Nil Compare: %v == %v :: Arg1 Array Len: %d\n", arg0, arg1, len(arg1_array))
			if len(arg1_array) == 0 {
				value = 1
			}
		}
	}

	// arg1: Solve the unequal data type cases:  nil == [] == {}
	if arg1 == nil {
		switch arg0.(type) {
		case map[string]interface{}:
			arg0_map := arg0.(map[string]interface{})
			UdnLogLevel(nil, log_trace, "CompareUdnData: Nil Compare: %v == %v :: Arg0 Map Len: %d\n", arg0, arg1, len(arg0_map))
			if len(arg0_map) == 0 {
				value = 1
			}


		case []interface{}:
			arg0_array := arg0.([]interface{})
			UdnLogLevel(nil, log_trace, "CompareUdnData: Nil Compare: %v == %v :: Arg0 Array Len: %d\n", arg0, arg1, len(arg0_array))
			if len(arg0_array) == 0 {
				value = 1
			}
		}
	}

	return value
}

func UDN_CompareEqual(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Compare: Equal: %v\n", args)

	value := CompareUdnData(args[0], args[1])

	/*
	arg0 := GetResult(args[0], type_string).(string)
	arg1 := GetResult(args[1], type_string).(string)

	value := 1
	if arg0 != arg1 {
		value = 0
	}*/


	UdnLogLevel(udn_schema, log_debug, "Compare: Equal: '%v' == '%v' : %d\n", args[0], args[1], value)


	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_CompareNotEqual(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Compare: Not Equal: %v\n", args)

	value := CompareUdnData(args[0], args[1])

	// Reverse the comparison result, because we are NOT EQUAL
	if value == 0 {
		value = 1
	} else {
		value = 0
	}

	/*
	arg0 := GetResult(args[0], type_string).(string)
	arg1 := GetResult(args[1], type_string).(string)

	value := 1
	if arg0 == arg1 {
		value = 0
	}
	*/

	UdnLogLevel(udn_schema, log_debug, "Compare: Not Equal: '%v' != '%v' : %d\n", args[0], args[1], value)

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_Test(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Test Function\n")

	result := UdnResult{}
	result.Result = "Testing.  123."

	return result
}

func UDN_TestDifferent(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Different Test Function!!!\n")

	result := UdnResult{}
	result.Result = "Testing.  Differently."

	return result
}

func UDN_GetFirst(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Get First: %s\n", SnippetData(args, 300))

	result := UdnResult{}

	// Process each of our args, until one of them isnt nil
	for _, arg := range args {
		type_str := fmt.Sprintf("%T", arg)

		if strings.HasPrefix(type_str, "[]") {
			for _, item := range arg.([]interface{}) {
				arg_str := GetResult(item, type_string).(string)
				arg_array := make([]interface{}, 0)
				arg_array = AppendArray(arg_array, arg_str)

				result.Result = MapGet(arg_array, udn_data)

				// If this wasnt nil, quit
				if result.Result != nil {
					UdnLogLevel(udn_schema, log_trace, "Get First: %v   Found: %v   Value: %v\n", SnippetData(args, 300), arg_str, result.Result)
					break
				}
			}
		} else {
			arg_str := GetResult(arg, type_string).(string)
			arg_array := make([]interface{}, 0)
			arg_array = AppendArray(arg_array, arg_str)

			result.Result = MapGet(arg_array, udn_data)

			// If this wasnt nil, quit
			if result.Result != nil {
				UdnLogLevel(udn_schema, log_trace, "Get First: %s   Found: %s\n", SnippetData(args, 300), arg_str)
			}
		}

		// Always stop if we have a result here
		if result.Result != nil {
			break
		}
	}

	//UdnLogLevel(udn_schema, log_trace, "Get: %v   Result: %v\n", SnippetData(args, 80), SnippetData(result.Result, 80))
	UdnLogLevel(udn_schema, log_trace, "Get First: %s   Result: %v\n", SnippetData(args, 300), result.Result)

	return result
}

func UDN_Get(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Get: %v\n", SnippetData(args, 80))

	result := UdnResult{}
	result.Result = MapGet(args, udn_data)

	UdnLogLevel(udn_schema, log_trace, "Get: %v   Result: %v\n", SnippetData(args, 80), SnippetData(result.Result, 80))
	//UdnLogLevel(udn_schema, log_trace, "Get: %v   Result: %v\n", SnippetData(args, 80), result.Result)

	return result
}

func UDN_Set(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLogLevel(udn_schema, log_trace, "Set: %v   Input: %s\n", SnippetData(args, 80), SnippetData(input, 40))

	result := UdnResult{}
	result.Result = MapSet(args, input, udn_data)

	//UdnLogLevel(udn_schema, log_trace, "Set: %v  Result: %s\n\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_GetTemp(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	function_stack := udn_data["__function_stack"].([]map[string]interface{})
	function_stack_item := function_stack[len(function_stack)-1]
	function_uuid := function_stack_item["uuid"].(string)
	UdnLogLevel(udn_schema, log_trace, "Get Temp: %s: %v\n", function_uuid, SnippetData(args, 80))

	// Ensure temp exists
	if udn_data["__temp"] == nil {
		udn_data["__temp"] = make(map[string]interface{})
	}

	// Ensure this Function Temp exists
	if udn_data["__temp"].(map[string]interface{})[function_uuid] == nil {
		udn_data["__temp"].(map[string]interface{})[function_uuid] = make(map[string]interface{})
	}

	// Set the temp_udn_data starting at this new value
	temp_udn_data := udn_data["__temp"].(map[string]interface{})[function_uuid].(map[string]interface{})

	// Call the normal Get function, with this temp_udn_data data
	result := UDN_Get(db, udn_schema, udn_start, args, input, temp_udn_data)

	return result
}

func UDN_GetTempKey(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	function_stack := udn_data["__function_stack"].([]map[string]interface{})
	function_stack_item := function_stack[len(function_stack)-1]
	function_uuid := function_stack_item["uuid"].(string)
	UdnLogLevel(udn_schema, log_trace, "Get Temp Key: %s: %v\n", function_uuid, SnippetData(args, 80))

	// Ensure temp exists
	if udn_data["__temp"] == nil {
		udn_data["__temp"] = make(map[string]interface{})
	}

	// Ensure this Function Temp exists
	if udn_data["__temp"].(map[string]interface{})[function_uuid] == nil {
		udn_data["__temp"].(map[string]interface{})[function_uuid] = make(map[string]interface{})
	}

	// concatenate all the arguments to return the final temp variable string
	var buffer bytes.Buffer

	buffer.WriteString("__temp.")
	buffer.WriteString(function_uuid)

	for _, arg := range args{
		buffer.WriteString(".")
		buffer.WriteString(arg.(string))
	}

	temp_string := buffer.String()

	// return the string that will allow direct access to the temp variable (including the function uuid)
	result := UdnResult{}
	result.Result = temp_string

	return result
}

func UDN_SetTemp(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	function_stack := udn_data["__function_stack"].([]map[string]interface{})
	function_stack_item := function_stack[len(function_stack)-1]
	function_uuid := function_stack_item["uuid"].(string)
	UdnLogLevel(udn_schema, log_trace, "Set Temp: %s: %v   Input: %s\n", function_uuid, SnippetData(args, 80), SnippetData(input, 40))

	// Ensure temp exists
	if udn_data["__temp"] == nil {
		udn_data["__temp"] = make(map[string]interface{})
	}

	// Ensure this Function Temp exists
	if udn_data["__temp"].(map[string]interface{})[function_uuid] == nil {
		udn_data["__temp"].(map[string]interface{})[function_uuid] = make(map[string]interface{})
	}

	// Set the temp_udn_data starting at this new value
	temp_udn_data := udn_data["__temp"].(map[string]interface{})[function_uuid].(map[string]interface{})

	// Call the normal Get function, with this temp_udn_data data
	result := UDN_Set(db, udn_schema, udn_start, args, input, temp_udn_data)

	return result
}

func UDN_SetHttpResponseCode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Set HTTP code: %v\n", SnippetData(args, 80))

	// args[0] - desired http code to be set
	if len(args) > 0 {
		http_response_code, err := strconv.Atoi(args[0].(string))
		if err == nil {
			udn_data["http_response_code"] = http_response_code
		}
	}

	// result is passed through
	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_Iterate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Will loop over all UdnParts until it finds __end_iterate.  It expects input to hold a list.List, which use to iterate and execute the UdnPart blocks
	// It will set a variable that will be accessable by the "__get.current.ARG0"
	// Will return a list.List of each of the loops, which allows for filtering the iteration

	// This is our final input list, as an array, it always works and gets input to pass into the first function
	input_array := GetResult(input, type_array).([]interface{})

	UdnLogLevel(udn_schema, log_trace, "Iterate: [%s]  Input: %s\n\n", udn_start.Id, SnippetData(input_array, 240))

	// Our result will be a list, of the result of each of our iterations, with a UdnResult per element, so that we can Transform data, as a pipeline
	result := UdnResult{}
	result_list := make([]interface{}, 0)

	// If we have something to iterate over
	if len(input_array) > 0 {
		// Loop over the items in the input
		for _, item := range input_array {
			UdnLogLevel(udn_schema, log_trace, "\n====== Iterate Loop Start: [%s]  Input: %v\n\n", udn_start.Id, SnippetData(item, 300))

			// Get the input
			current_input := item

			// Variables for looping over functions (flow control)
			udn_current := udn_start

			// Loop over the UdnParts, executing them against the input, allowing it to transform each time
			for udn_current != nil && udn_current.Id != udn_start.BlockEnd.Id && udn_current.NextUdnPart != nil {
				udn_current = udn_current.NextUdnPart

				//UdnLogLevel(udn_schema, log_trace, "  Walking ITERATE block [%s]: Current: %s   Current Input: %v\n", udn_start.Id, udn_current.Value, SnippetData(current_input, 60))

				// Execute this, because it's part of the __if block, and set it back into the input for the next function to take
				current_input_result := ExecuteUdnPart(db, udn_schema, udn_current, current_input, udn_data)
				current_input = current_input_result.Result

				// If we are being told to skip to another NextUdnPart, we need to do this, to respect the Flow Control
				if current_input_result.NextUdnPart != nil {
					// Move the current to the specified NextUdnPart
					//NOTE(g): This works because this NextUdnPart will be "__end_iterate", or something like that, so the next for loop test works
					udn_current = current_input_result.NextUdnPart
				}
			}

			// Take the final input (the result of all the execution), and put it into the list.List we return, which is now a transformation of the input list
			result_list = AppendArray(result_list, current_input)

			// Fix the execution stack by setting the udn_current to the udn_current, which is __end_iterate, which means this block will not be executed when UDN_Iterate completes
			result.NextUdnPart = udn_current
		}

		// Send them passed the __end_iterate, to the next one, or nil
		if result.NextUdnPart == nil {
			UdnLogLevel(udn_schema, log_trace, "\n====== Iterate Finished: [%s]  NextUdnPart: %v\n\n", udn_start.Id, result.NextUdnPart)
		} else if result.NextUdnPart.NextUdnPart != nil {
			UdnLogLevel(udn_schema, log_trace, "\n====== Iterate Finished: [%s]  NextUdnPart: %v\n\n", udn_start.Id, result.NextUdnPart)
		} else {
			UdnLogLevel(udn_schema, log_trace, "\n====== Iterate Finished: [%s]  NextUdnPart: End of UDN Parts\n\n", udn_start.Id)
		}
	} else {
		// Else, there is nothing to iterate over, but we still need to get our NextUdnPart to skip iterate's execution block
		udn_current := udn_start

		// Loop over the UdnParts, executing them against the input, allowing it to transform each time
		for udn_current != nil && udn_current.Id != udn_start.BlockEnd.Id && udn_current.NextUdnPart != nil {
			udn_current = udn_current.NextUdnPart
			result.NextUdnPart = udn_current
		}
	}

	// Store the result list
	result.Result = result_list

	// Return the
	return result
}


func UDN_While(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Will loop over all UdnParts until it finds __end_iterate.  It expects input to hold a list.List, which use to iterate and execute the UdnPart blocks
	// It will set a variable that will be accessable by the "__get.current.ARG0"
	// Will return a list.List of each of the loops, which allows for filtering the iteration

	// This is our final input list, as an array, it always works and gets input to pass into the first function
	condition_udn_string := GetResult(args[0], type_string).(string)

	max_loops := GetResult(args[1], type_int).(int64)
	current_loops := int64(0)

	UdnLogLevel(udn_schema, log_trace, "While: [MAX=%d]  Condition: %s\n\n", max_loops, condition_udn_string)

	// Our result will be a list, of the result of each of our iterations, with a UdnResult per element, so that we can Transform data, as a pipeline
	result := UdnResult{}
	result_list := make([]interface{}, 0)

	var current_input interface{}

	// If we have something to iterate over
	for current_loops < max_loops {
		condition_value := ProcessSingleUDNTarget(db, udn_schema, condition_udn_string, nil, udn_data)

		if !IfResult(condition_value) {
			UdnLogLevel(udn_schema, log_trace, "\n====== While Finished: [%s]  Condition False (%v): %v\n\n", udn_start.Id, current_loops, condition_value)
			// Break out of the while loop
			break
		}

		UdnLogLevel(udn_schema, log_trace, "\n====== While Loop Start: [%d]  Current Loop: %d  Condition: %v\n\n", udn_start.Id, current_loops, SnippetData(condition_value, 300))

		// Get the input
		current_input = nil

		// Variables for looping over functions (flow control)
		udn_current := udn_start

		// Loop over the UdnParts, executing them against the input, allowing it to transform each time
		for udn_current != nil && udn_current.Id != udn_start.BlockEnd.Id && udn_current.NextUdnPart != nil {
			udn_current = udn_current.NextUdnPart

			//UdnLogLevel(udn_schema, log_trace, "  Walking ITERATE block [%s]: Current: %s   Current Input: %v\n", udn_start.Id, udn_current.Value, SnippetData(current_input, 60))

			// Execute this, because it's part of the __if block, and set it back into the input for the next function to take
			current_input_result := ExecuteUdnPart(db, udn_schema, udn_current, current_input, udn_data)
			current_input = current_input_result.Result

			// If we are being told to skip to another NextUdnPart, we need to do this, to respect the Flow Control
			if current_input_result.NextUdnPart != nil {
				// Move the current to the specified NextUdnPart
				//NOTE(g): This works because this NextUdnPart will be "__end_iterate", or something like that, so the next for loop test works
				udn_current = current_input_result.NextUdnPart
			}
		}

		// Take the final input (the result of all the execution), and put it into the list.List we return, which is now a transformation of the input list
		result_list = AppendArray(result_list, current_input)

		// Fix the execution stack by setting the udn_current to the udn_current, which is __end_iterate, which means this block will not be executed when UDN_Iterate completes
		result.NextUdnPart = udn_current

		// Send them passed the __end_iterate, to the next one, or nil
		if result.NextUdnPart == nil {
			UdnLogLevel(udn_schema, log_trace, "\n====== While Finished: [%s]  NextUdnPart: %v\n\n", udn_start.Id, result.NextUdnPart)
		} else if result.NextUdnPart.NextUdnPart != nil {
			UdnLogLevel(udn_schema, log_trace, "\n====== While Finished: [%s]  NextUdnPart: %v\n\n", udn_start.Id, result.NextUdnPart)
		} else {
			UdnLogLevel(udn_schema, log_trace, "\n====== While Finished: [%s]  NextUdnPart: End of UDN Parts\n\n", udn_start.Id)
		}

		current_loops++
		if current_loops >= max_loops {
			UdnLogLevel(udn_schema, log_trace, "\n====== While Finished: [%s]  Maximum Loops Reached (%d): %v\n\n", udn_start.Id, max_loops, result.NextUdnPart)
			// Break out of the while loop
			break
		}
	}

	// Store the result list
	result.Result = result_list

	// Return the
	return result
}

// This is a common function to test for UDN true/false.  Similar to Python's concept of true false.
//TODO(g): Include empty array and empty map in this, as returning "false", non-empty is "true"
func IfResult(value interface{}) bool {
	// Empty arrays and maps are false
	switch value.(type) {
	case []interface{}:
		if len(value.([]interface{})) == 0 {
			return false
		}
	case map[string]interface{}:
		if len(value.(map[string]interface{})) == 0 {
			return false
		}
	}

	// Match various "false" equavalent values
	if value == "0" || value == nil || value == 0 || value == false || value == "" {
		return false
	} else {
		// Catch all: Match various "false" equavalent values, as a string representation
		value_str := fmt.Sprintf("%v", value)
		if value_str == "0" || value_str == "<nil>" || value_str == "0" || value_str == "false" || value_str == "" {
			return false
		} else {
			return true
		}
	}

}

func UDN_IfCondition(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	arg_0 := args[0]

	UdnLogLevel(udn_schema, log_trace, "If Condition: %s\n", arg_0)

	// If this is true, all other blocks (else-if, else) will be skipped.  It doesnt matter which block this is, an IF/ELSE-IF/ELSE chain only executes 1 block
	executed_a_block := false
	// Track when we leave the "then" (first) block
	outside_of_then_block := false
	// Used to control when we skip a block
	skip_this_block := false

	// Evaluate whether we will execute the IF-THEN (first) block.  (We dont use a THEN, but thats the saying)
	execute_then_block := true
	//if arg_0 == "0" || arg_0 == nil || arg_0 == 0 || arg_0 == false || arg_0 == "" {
	if !IfResult(arg_0) {
		execute_then_block = false

		UdnLogLevel(udn_schema, log_trace, "If Condition: Not Executing THEN: %s\n", arg_0)
	} else {
		// We will execute the "then" block, so we mark this now, so we skip any ELSE-IF/ELSE blocks
		// Execute A Block, means we should execute at least one
		executed_a_block = true

		UdnLogLevel(udn_schema, log_trace, "If Condition: Executing THEN: %s\n", arg_0)
	}

	// Variables for looping over functions (flow control)
	udn_current := udn_start

	current_input := input

	// Check the first argument, to see if we should execute the IF-THEN statements, if it is false, we will look for ELSE-IF or ELSE if no ELSE-IF blocks are true.

	// Keep track of any embedded IF statements, as we will need to process or not process them, depending on whether we are currently embedded in other IFs
	embedded_if_count := 0

	//TODO(g): Walk our NextUdnPart until we find our __end_if, then stop, so we can skip everything for now, initial flow control
	for udn_current != nil && (embedded_if_count == 0 && udn_current.Value != "__end_if") && udn_current.NextUdnPart != nil {
		udn_current = udn_current.NextUdnPart

		UdnLogLevel(udn_schema, log_trace, "Walking IF block: Current: %s   Current Input: %s\n", udn_current.Value, SnippetData(current_input, 80))

		// If we are not executing the THEN block, and we encounter an __if statement, keep track of depth
		if execute_then_block == false && outside_of_then_block == false && udn_current.Value == "__if" {
			embedded_if_count++
		} else if embedded_if_count > 0 {
			// Skip everything until our embedded if is done
			if udn_current.Value == "__end_if" {
				embedded_if_count--
			}
		} else if udn_current.Value == "__else" || udn_current.Value == "__else_if" {
			outside_of_then_block = true
			// Reset this every time we get a new control block start (__else/__else_if), because we havent tested it to be skipped yet
			skip_this_block = false

			if executed_a_block {
				// If we have already executed a block before, then it's time to skip the remaining blocks/parts
				UdnLogLevel(udn_schema, log_trace, "Found non-main-if block, skipping: %s\n", udn_current.Value)
				break
			} else {
				// Else, we havent executed a block, so we need to determine if we should start executing.  This is only variable for "__else_if", "else" will always execute if we get here
				if udn_current.Value == "__else_if" {
					udn_current_arg_0 := udn_current.Children.Front().Value.(*UdnPart)
					// If we dont have a "true" value, then skip this next block
					if udn_current_arg_0.Value == "0" {
						skip_this_block = true
					} else {
						UdnLogLevel(udn_schema, log_trace, "Executing Else-If Block: %s\n", udn_current_arg_0.Value)
						// Mark block execution, so we wont do any more
						executed_a_block = true
					}
				} else {
					// This is an "__else", and we made it here, so we are executing the else.  Leaving this here to demonstrate that
					UdnLogLevel(udn_schema, log_trace, "Executing Else Block\n")
					// Mark block execution, so we wont do any more.  This shouldnt be needed as there should only be one final ELSE, but in case there are more, we will skip them all further ELSE-IF/ELSE blocks
					executed_a_block = true
				}
			}
		} else {
			// Either we are outside the THEN block (because we would skip if not correct), or we want to execute the THEN block
			if outside_of_then_block || execute_then_block {
				if !skip_this_block {
					// Execute this, because it's part of the __if block
					current_result := ExecuteUdnPart(db, udn_schema, udn_current, current_input, udn_data)
					current_input = current_result.Result

					// If we were told what our NextUdnPart is, jump ahead
					if current_result.NextUdnPart != nil {
						UdnLogLevel(udn_schema, log_trace, "If: Flow Control: JUMPING to NextUdnPart: %s [%s]\n", current_result.NextUdnPart.Value, current_result.NextUdnPart.Id)
						udn_current = current_result.NextUdnPart
					}
				}
			}
		}
	}

	// Skip to the end of the __if block (__end_if)
	for udn_current != nil && udn_current.Value != "__end_if" && udn_current.NextUdnPart != nil {
		udn_current = udn_current.NextUdnPart
	}

	final_result := UdnResult{}
	final_result.Result = current_input
	final_result.NextUdnPart = udn_current

	return final_result
}

func UDN_ElseCondition(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Else Condition\n")

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_ElseIfCondition(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Else If Condition\n")

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_Not(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Not: %v\n", SnippetData(input, 60))

	value := "0"
	if input != nil && input != "0" {
		value = "1"
	}

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_NotNil(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Not Nil: %v\n", SnippetData(input, 60))

	value := "0"
	if input != nil {
		value = "1"
	}

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_IsNil(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Not Nil: %v\n", SnippetData(input, 60))

	value := "0"
	if input == nil {
		value = "1"
	}

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_StringToTime(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "String to Time: %v\n", SnippetData(input, 60))

	time_string := ""
	switch input.(type) {
	case string:
		time_string = input.(string)
	}

	layout := "2006-01-02 15:04:05"
	parsed_time, err := time.Parse(layout, time_string)
	result := UdnResult{}

	// return Time.time object is conversion is successful
	if err == nil {
		result.Result = parsed_time
	}

	if err != nil {
		// try another layout if previous one does not work
		layout2 := "2006-01-02T15:04:05"
		parsed_time, err := time.Parse(layout2, time_string)

		if err == nil {
			result.Result = parsed_time
		}
	}

	if err != nil {
		// try another layout if previous one does not work
		layout3 := "2006-01-02T15:04:05.000Z"
		parsed_time, err := time.Parse(layout3, time_string)

		if err == nil {
			result.Result = parsed_time
		}
	}

	return result
}

func UDN_TimeToEpoch(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "time.Time to unix time in seconds: %v\n", SnippetData(input, 60))

	result := UdnResult{}

	// input is a Time.time object
	switch input.(type) {
	case time.Time:
		result.Result = int64(input.(time.Time).Unix())
	default: // Do nothing if input is not a Time.time object
	}

	return result
}

func UDN_TimeToEpochMs(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "time.Time to unix time in milliseconds: %v\n", SnippetData(input, 60))

	result := UdnResult{}

	// input is a Time.time object
	switch input.(type) {
	case time.Time:
		result.Result = int64(input.(time.Time).UnixNano()) / int64(time.Millisecond)
	default: // Do nothing if input is not a Time.time object
	}

	return result
}

func UDN_NumberToString(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Number to String: %v\n", args)

	result := UdnResult{}
	args_len := len(args)

	var prec int

	var MAX_PRECISION int
	MAX_PRECISION = 12

	if args_len == 1 {
		// check given precision (int)
		switch args[0].(type) {
		case string:
			precision, err := strconv.Atoi(GetResult(args[0], type_string).(string))

			if err == nil {
				prec = precision
			} else {
				// args[0] cannot convert to (int)
				// return the input converted to string
				result.Result = fmt.Sprintf("%v", input)
				return result
			}

		case int, int64, float64:
			prec = GetResult(args[0], type_int).(int)
		default:
			// args[0] not (int/int64/float64/string), return the input converted to string
			result.Result = fmt.Sprintf("%v", input)
			return result
		}

		if prec > MAX_PRECISION {
			prec = MAX_PRECISION
		}

		// Convert given number to string with specified precision
		switch input.(type) {
		case int, int64, float64:
			result.Result = strconv.FormatFloat(GetResult(input, type_float).(float64), 'f', prec, 64)
		default:
			// Do nothing
		}
	} else {
		// No precision is specified, 0 or > 2 arguments
		// return input converted to string.
		switch input.(type) {
		case int, int64, float64:
			//check if the input is a whole number, if it is, convert it to int.
			if GetResult(input, type_float).(float64) == math.Trunc(GetResult(input, type_float).(float64)) {
				result.Result = fmt.Sprintf("%d", GetResult(input, type_int).(int64))
			} else {
				result.Result = strconv.FormatFloat(GetResult(input, type_float).(float64), 'f', -1, 64)
			}
		default:
			//Otherwise, just print it out
			result.Result = fmt.Sprintf("%v", input)
		}
	}
	return result
}


func UDN_GetCurrentTime(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Get Current Time: %v\n", SnippetData(input, 60))

	result := UdnResult{}
	args_len := len(args)

	var zero_ascii byte
	zero_ascii = 48

	var nine_ascii byte
	nine_ascii = 57

	layout := "2006-01-02 15:04:05"
	fmt_len := len(layout)
	current_time_string := time.Now().UTC().String()[:fmt_len] // formated string of current time
	time_obj := time.Now().UTC()

	current_time, err := time.Parse(layout, current_time_string)

	if err == nil {
		time_obj = current_time
	} else {
		// if current_time is invalid => return current time.Time obj
		time_obj = time.Now().UTC()
	}

	if args_len == 1 {

		time_string := ""
		specified_string := ""

		switch args[0].(type) {
		case string:
			specified_string = args[0].(string) // 'YYYY-MM-DD hh:mm:ss'
		default:
			// if arg[0] is invalid => return current time.Time obj
			result.Result = time_obj

		}

		if len(current_time_string) == len(specified_string) {

			for i, _ := range specified_string {

				if specified_string[i] != current_time_string[i] && specified_string[i] >= zero_ascii && specified_string[i] <= nine_ascii {
					// specified_string[i] (byte) within ascii range for 0 to 9
					time_string += string(specified_string[i])
				} else {
					time_string += string(current_time_string[i])
				}
			}
		}
		parsed_time, err := time.Parse(layout, time_string)
		// if conversion from string is successful => return time.time obj
		if err == nil {
			result.Result = parsed_time
		} else {
			// if parsed_time is invalid => return current time.Time obj
			result.Result = time_obj
			return result
		}

	} else {
		// invalid number of args => return current time.time obj
		result.Result = time_obj

	}
	return result
}


func UDN_GetLocalTime(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Get Local Time: %v\n", SnippetData(input, 60))

	result := UdnResult{}
	args_len := len(args)

	time_obj := time.Now()

	if args_len == 1 {

		specified_timezone := ""

		switch args[0].(type) {
		case string:
			specified_timezone = args[0].(string) // specified timezone (e.g "America/Chicago")
		default:
			// if arg[0] is invalid => return current local time.Time obj
			result.Result = time_obj
		}

		if specified_timezone == "" {
			// Given empty string => override it with local as timezone
			specified_timezone = "local"
		}

		location, err := time.LoadLocation(specified_timezone)
		// given current UTC time => return current local time using the IANA specified_timezone location
		if err == nil {
			result.Result = time.Now().UTC().In(location)
		} else {
			// if specified_timezone is invalid => return current local time.Time obj
			result.Result = time_obj
			return result
		}

	} else {
		// invalid number of args => return current local time.time obj
		result.Result = time_obj
	}
	return result
}


func UDN_GroupBy(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Group by: %v\n", SnippetData(input, 60))

	// arg[0] = method to group on
	// arg[1] = source of data
	// arg[2] = aggregated field (ex: cost, total, etc.)
	// arg[3] = field to group on
	// ex: __group_by.method.data_location.field1.field2.field3...

	// source of data should be a list of maps
	// ex: [{order_id: 101, category: monitor, cost: 80},
	//      {order_id: 102, category: monitor, cost: 82},
	//      {order_id: 103, category: laptop, cost: 100}]
	// __group_by.sum.data_above.category yields:
	// [{category: monitor, cost: 162},
	//  {category: laptop, cost: 100}]


	result := UdnResult{}

	if len(args) < 4 {
		return result // Nothing to group by
	}

	var source_data []map[string]interface{}

	switch args[1].(type){
	case []interface{}:
		source_data = make([]map[string]interface{}, len(args[1].([]interface{})))

		for index, element := range args[1].([]interface{}) {
			source_data[index] = element.(map[string]interface{})
		}
	case []map[string]interface{}:
		source_data = args[1].([]map[string]interface{})
	}

	method := strings.ToLower(args[0].(string))
	aggregate_field := args[2].(string)
	field := args[3].(string) //TODO(z): Make field variadic - Implement grouping on multiple fields - currently only supports grouping on one field  (when there is use case)

	result_list := make([]map[string]interface{}, 0) // stores result array
	result_map := make(map[string]interface{}) // stores all seen keys

	// Certain default methods will be implemented - rest found in an entry in opsdb udn_stored_functions table (TODO)
	//TODO(z): Need to add entry in udn_stored_functions table to handle such new functions (ex: group_by_bettersum)
	//TODO(z): Other default group by functions such as min, max, avg (when there is use case)
	switch method {
	case "count":
		for _, element := range source_data {
			// check for new keys based on the group by field
			if _, key_exists := result_map[element[field].(string)]; !key_exists {
				// create new key to group on
				new_key_map := make(map[string]interface{})
				new_key_map[field] = element[field].(string)
				new_key_map[aggregate_field] = int64(0)

				result_list = append(result_list, new_key_map)

				index := int64(len(result_list) - 1)
				result_map[element[field].(string)] = index // store index of the result in the seen key map

				// only aggregate if the target field exists
				if _, field_exists := element[aggregate_field]; field_exists {
					result_list[index][aggregate_field] = result_list[index][aggregate_field].(int64) + int64(1)
				}
			} else { // key exists - add count to existing value if aggregate_field exists

				index := result_map[element[field].(string)].(int64)

				// only aggregate if the target field exists
				if _, field_exists := element[aggregate_field]; field_exists {
					result_list[index][aggregate_field] = result_list[index][aggregate_field].(int64) + int64(1)
				}
			}
		}
	case "sum":
		for _, element := range source_data {
			// convert element[aggregate_field] to int64 if necessary
			//TODO(z): add float support if there is use-case for the sum function - default is int
			aggregate_value := int64(0)

			switch element[aggregate_field].(type){
			case string:
				aggregate_value, _ = strconv.ParseInt(element[aggregate_field].(string), 10, 64)
			case int64:
				aggregate_value = element[aggregate_field].(int64)
			case float64:
				aggregate_value = int64(element[aggregate_field].(float64))
			}


			// check for new keys based on the group by field
			if _, key_exists := result_map[element[field].(string)]; !key_exists {
				// create new key to group on
				new_key_map := make(map[string]interface{})
				new_key_map[field] = element[field].(string)
				new_key_map[aggregate_field] = aggregate_value

				result_list = append(result_list, new_key_map)
				result_map[element[field].(string)] = int64(len(result_list) - 1) // store index of the result in the seen key map
			} else { // key exists - add sum to existing value
				index := result_map[element[field].(string)].(int64)

				result_list[index][aggregate_field] = result_list[index][aggregate_field].(int64) + aggregate_value
			}
		}
	default: // Not found - look in udn_stored_functions table (TODO)
	}

	result.Result = result_list
	return result
}

func UDN_Math(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLogLevel(udn_schema, log_trace, "Group by: %v\n", SnippetData(input, 60))

	// This function will encompass all math related functions for UDN
	// arg[0] = function
	// arg[1...n] = operands
	// ex: __math.divide.operand1.operand2

	result := UdnResult{}

	if len(args) < 1 {
		return result // Function not specified
	}

	all_integer := true // Flag used to determine whether we should do an integer operation
	function := strings.ToLower(args[0].(string))
	operands := args[1:]
	num_of_operands := len(operands)

	// Make a first pass through each operand to check if all operands are integers (for integer arithmetic)
	for _, operand := range operands {
		switch operand.(type) {
		case int, int32, int64:
		case string:
			if _, err := strconv.Atoi(operand.(string)); err != nil {
				all_integer = false
			}
		default:
			all_integer = false
		}
	}

	// Second pass - go through each operand and check for type and do conversions when necessary
	for index, operand := range operands {
		switch operand.(type) {
		case int64: // int64 is default
			if !all_integer {
				operands[index] = float64(operand.(int64))
			}
		case int:
			if all_integer {
				operands[index] = int64(operand.(int))
			} else {
				operands[index] = float64(operand.(int))
			}
		case int32:
			if all_integer {
				operands[index] = int64(operand.(int32))
			} else {
				operands[index] = float64(operand.(int32))
			}
		case float64: // float64 is default
		case float32:
			operands[index] = float64(operand.(float32))
		case string: // try to convert from string to int64 first, then float64
			operand_int, err := strconv.ParseInt(operand.(string), 10, 64)

			if err == nil && all_integer {
				operands[index] = operand_int
			} else { // try to convert to float64
				operand_float, err := strconv.ParseFloat(operand.(string), 64)

				if err == nil {
					operands[index] = operand_float
				} else {
					return result // invalid operand - return nil
				}
			}
		default:
			// One of the operands is not a valid int/float, thus stop function and return nil
			return result
		}
	}

	//TODO(z): implement more arithmetic functions as needed when there is use case
	switch function {
	case "input": // __input function for integer or float
		if num_of_operands < 1 {
			return result
		}
		if all_integer {
			result.Result = operands[0].(int64)
		} else {
			result.Result = operands[0].(float64)
		}

	case "+", "add": // TODO(z): make operations variadic when applicable
		if num_of_operands < 2 {
			return result
		}
		if all_integer {
			result.Result = operands[0].(int64) + operands[1].(int64)
		} else {
			result.Result = operands[0].(float64) + operands[1].(float64)
		}
	case "-", "subtract":
		if num_of_operands < 2 {
			return result
		}
		if all_integer {
			result.Result = operands[0].(int64) - operands[1].(int64)
		} else {
			result.Result = operands[0].(float64) - operands[1].(float64)
		}
	case "*", "multiply":
		if num_of_operands < 2 {
			return result
		}
		if all_integer {
			result.Result = operands[0].(int64) * operands[1].(int64)
		} else {
			result.Result = operands[0].(float64) * operands[1].(float64)
		}
	case "/", "divide": // returns float - not integer division by default
		if num_of_operands < 2 {
			return result
		}
		if all_integer {
			result.Result = float64(operands[0].(int64)) / float64(operands[1].(int64))
		} else {
			result.Result = operands[0].(float64) / operands[1].(float64)
		}
	default:
		result.Result = 0
	}
	return result
}

func UDN_DebugGetAllUdnData(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//TODO(g):SECURITY: This should have a security check, because it is a DEBUG style function, and could give away information the end user should not see, but is needed during processing, and is not exposed without this type of DEBUG function

	debug_udn_data := make(map[string]interface{})

	// Remove keys that arent useful for debugging
	//TODO(g): Make ignoring these optional, as we may want some of them, or others.  Use a counting system, so higher number shows more, or something.  Lower shows more?  Something.
	ignore_keys := []string {"base_widget", "__temp", "__function_stack", "user", "cookie", "header", "param"}
	for k, v := range udn_data {
		if !IsStringInArray(k, ignore_keys) {
			debug_udn_data[k] = v
		}
	}

	result := UdnResult{}
	result.Result = debug_udn_data

	return result
}

func UDN_ExecCommand(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Execute a command line command from UDN
	// Ex: __exec_command.'ls'.'-l'.'-h'
	// Separating command and flags enables easy dynamic flag generations in UDN
	// Data output: the output of the command line execution
	// TODO: Require authentication for this command, o/w people can write commands that destroy a system. 

	UdnLogLevel(udn_schema, log_trace, "UDN Exec_Command: %v\n", args)

	var evaluated_args []string
	for _, arg := range args {
		evaluated_args = append(evaluated_args, GetResult(arg, type_string).(string))
	}

	cmd_output, err := exec.Command(evaluated_args[0], evaluated_args[1:]...).Output()

	if err != nil {
		log.Panic(err)
	}

	UdnLogLevel(udn_schema, log_debug, "Exec_Command: Command '%s'\n", evaluated_args[0])
	UdnLogLevel(udn_schema, log_debug, "Exec_Command: Options '%s'\n", evaluated_args[1:])
	UdnLogLevel(udn_schema, log_debug, "Exec_Command: Output '%s'\n", cmd_output)


	result := UdnResult{}
	result.Result = string(cmd_output)

	return result
}

func UDN_HttpRequest(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult{
	//Send a http request to url
	//E.g. __http_request.'GET'.'http://example.com', sends GET request to target url, returns an unmarshalled json if have any in the response.
	//E.g. __input.{name=Bob}.__http_request.'POST'.'http://example.com', sends POST request to target url with {"name":"bob"}, returns an unmarshalled json if have any in the response.
	//If error or failed or timeout (10 seconds by default), return nothing

	UdnLogLevel(udn_schema, log_debug,"Http Request: %v with input: %v\n", args, input)

	result := UdnResult{}

	if len(args) < 2 {
		UdnLogLevel(udn_schema, log_error,"Insufficient args: %v\n", args)
		return result
	}

	method := GetResult(args[0], type_string).(string)
	url := GetResult(args[1], type_string).(string)

	timeout_secs := 10.0
	if len(args) > 2 {
		timeout_secs = GetResult(args[2], type_float).(float64)
	}
	timeout := time.Duration(timeout_secs) * time.Second
	client := http.Client{
		Timeout: timeout,
	}

	var request *http.Request

	if method == "POST" || method == "PUT"{
		if input != nil {
			var inputMap interface{}
			var ok bool
			switch input.(type){
			case []interface{}:
				inputMap, ok = input.([]interface{})
				break
			case map[string]interface{}:
				inputMap, ok = input.(map[string]interface{})
				break
			default:
				UdnLogLevel(udn_schema, log_error,"Body format not supported, input: %v\n", input)
				return result
			}
			if !ok {
				UdnLogLevel(udn_schema, log_error,"Input format casting failed, input: %v\n", input)
				return result
			}
			jsonValue, err := json.Marshal(inputMap)
			if err != nil {
				UdnLogLevel(udn_schema, log_error,"Input json marshal error: %v\n", err)
				return result
			}
			request, err = http.NewRequest(method, url, bytes.NewBuffer(jsonValue))
			if err != nil {
				UdnLogLevel(udn_schema, log_error, "Cannot create a Http NewRequest: %v\n", err)
				return result
			}
		}
	} else if method == "GET" || method == "DELETE"{
		var err error
		request, err = http.NewRequest(method, url, nil)
		if err != nil {
			UdnLogLevel(udn_schema, log_error, "Cannot create a Http NewRequest: %v\n", err)
			return result
		}
	} else {
		UdnLogLevel(udn_schema, log_error, "Unsupported http request method: %v\n", method)
		return result
	}

	resp, err := client.Do(request)
	if err != nil {
		UdnLogLevel(udn_schema, log_error,"Http request failed or timed-out: %v\n", err)
		return result
	}

	defer resp.Body.Close()

	var res interface{}

	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		UdnLogLevel(udn_schema, log_error,"Response body read error: %v\n", readErr)
		return result
	}

	if resp.StatusCode < 200 || resp.StatusCode > 300{
		UdnLogLevel(udn_schema, log_error,"Http request failed with status: %v\n", resp.Status)
	} else {
		if method == "POST" || method == "PUT" || method == "DELETE"{
			result.Result = resp.StatusCode
		} else {
			contentType := resp.Header.Get("Content-Type")
			if strings.Contains(contentType, "application/json"){
				err = json.Unmarshal(body, &res)
				if err != nil {
					UdnLogLevel(udn_schema, log_error,"Response body unmarshal error: %v\n", err)
					return result
				}
				result.Result = res
			} else {
				result.Result = string(body)
			}
		}
	}
	return result

}

func RenderWidgetInstance(db_web *sql.DB, udn_schema map[string]interface{}, udn_data map[string]interface{}, site_page_widget map[string]interface{}, udn_update_map map[string]interface{}) {
	// Render a Widget Instance

	// data_static  --  data_instance_static --  Available for default data...

	// We are rendering a Web Widget Instance here instead, load the data necessary for the Processing UDN
	// Data for the widget instance goes here (Inputs: data, columns, rows, etc.  These are set from the Processing UDN
	//udn_data["widget_instance"] = make(map[string]interface{})
	// If we dont have this bucket yet, make it
	if udn_data["widget_instance"] == nil {
		udn_data["widget_instance"] = make(map[string]interface{})
	}

	// Get the UUID for this widget instance
	id := ksuid.New()
	udn_data["widget_instance"].(map[string]interface{})["uuid"] = id.String()

	// Widgets go here (ex: base, row, row_column, header).  We set this here, below.
	udn_data["widget"] = make(map[string]interface{})

	// Set web_widget_instance output location (where the Instance's UDN will string append the output)
	udn_data["widget_instance"].(map[string]interface{})["output_location"] = site_page_widget["web_widget_instance_output"]

	// Use this to abstract between site_page_widget and web_data_widget_instance
	widget_instance := site_page_widget

	if site_page_widget["web_data_widget_instance_id"] != nil {
		// Get the web_data_widget_instance data
		sql := fmt.Sprintf("SELECT * FROM web_data_widget_instance WHERE _id = %d", site_page_widget["web_data_widget_instance_id"])
		web_data_widget_instance := Query(db_web, sql)[0]

		// Set this as the new widget instance data, since it supercedes the site_page_widget
		widget_instance = web_data_widget_instance

		// Save the widget instance ID too, so we can put it in our hidden field for re-rendering
		udn_data["widget_instance"].(map[string]interface{})["_web_data_widget_instance_id"] = web_data_widget_instance["_id"]

		UdnLogLevel(udn_schema, log_debug, "Web Data Widget Instance: %s\n", web_data_widget_instance["name"])

		// If we havent overridden this already, then get it
		if udn_update_map["widget_static"] == nil {
			// Get any static content associated with this page widget.  Then we dont need to worry about quoting or other stuff
			widget_static := make(map[string]interface{})
			udn_data["widget_static"] = widget_static
			if web_data_widget_instance["static_data_json"] != nil {
				err := json.Unmarshal([]byte(web_data_widget_instance["static_data_json"].(string)), &widget_static)
				if err != nil {
					log.Panic(err)
				}
			}
		}
	}

	// Get the web_widget_instance data
	sql := fmt.Sprintf("SELECT * FROM web_widget_instance WHERE _id = %d", widget_instance["web_widget_instance_id"])
	web_widget_instance := Query(db_web, sql)[0]


	UdnLogLevel(udn_schema, log_debug, "Web Widget Instance: %s\n", web_widget_instance["name"])
	UdnLogLevel(udn_schema, log_debug, "Web Widget Instance Data: %s\n", JsonDump(udn_data["widget_instance"]))

	// Get any static content associated with this page widget.  Then we dont need to worry about quoting or other stuff
	widget_static := make(map[string]interface{})
	udn_data["static_instance"] = widget_static
	if web_widget_instance["static_data_json"] != nil {
		err := json.Unmarshal([]byte(web_widget_instance["static_data_json"].(string)), &widget_static)
		if err != nil {
			log.Panic(err)
		}
	}

	UdnLogLevel(udn_schema, log_debug, "Params Data: %s\n", JsonDump(udn_data["param"]))
	default_map := make(map[string]interface{})
	if udn_data["param"].(map[string]interface{})["defaults"] != nil {
		defaults_string := udn_data["param"].(map[string]interface{})["defaults"].(string)
		UdnLogLevel(udn_schema, log_trace, "WI: Param: Defaults: JSON Before Load: %s\n", defaults_string)
		default_map, _ = JsonLoadMapIfValid(&defaults_string)
		UdnLogLevel(udn_schema, log_trace, "WI: Param: Defaults: After JSON Load: %v\n", default_map)
	} else if udn_data["widget_instance"].(map[string]interface{})["control"] != nil && udn_data["widget_instance"].(map[string]interface{})["control"].(map[string]interface{})["_defaults"] != nil {
		defaults_string_base64 := udn_data["widget_instance"].(map[string]interface{})["control"].(map[string]interface{})["_defaults"].(string)
		UdnLogLevel(udn_schema, log_trace, "WI: Control: _Defaults: %s\n", defaults_string_base64)
		detaults_string_b, _ := base64.StdEncoding.DecodeString(defaults_string_base64)
		defaults_string := string(detaults_string_b)
		UdnLogLevel(udn_schema, log_trace, "WI: Control: _Defaults: JSON Before Load: %s\n", defaults_string)
		default_map, _ = JsonLoadMapIfValid(&defaults_string)
		UdnLogLevel(udn_schema, log_trace, "WI: Control: _Defaults: After JSON Load: %v\n", default_map)
	}
	//udn_data["widget_instance"].(map[string]interface{})["default"] = default_map
	UdnLogLevel(udn_schema, log_trace, "WI: Defaults: Before default var assignment: %v\n", default_map)
	for key, value := range default_map {
		UdnLogLevel(udn_schema, log_trace, "WI: Defaults: Update: %s = %v\n", key, value)

		udn_data["data_static"].(map[string]interface{})["defaults"].(map[string]interface{})[key] = value
	}
	// Add the defaults to the defaults, so we pass the originals through each time we re-render
	if udn_data["data_static"] != nil && udn_data["data_static"].(map[string]interface{})["defaults"] != nil {
		defaults_json := JsonDump(default_map)
		defaults_json_base64 := base64.StdEncoding.EncodeToString([]byte(defaults_json))
		udn_data["data_static"].(map[string]interface{})["defaults"].(map[string]interface{})["_defaults"] = defaults_json_base64
	}


	UdnLogLevel(udn_schema, log_debug, "Web Widget Instance Data Static: %s\n", JsonDump(udn_data["data_static"]))

	// Get all the web widgets, by their web_widget_instance_widget.name
	sql = fmt.Sprintf("SELECT * FROM web_widget_instance_widget WHERE web_widget_instance_id = %d", widget_instance["web_widget_instance_id"])
	web_instance_widgets := Query(db_web, sql)
	for _, widget := range web_instance_widgets {
		sql = fmt.Sprintf("SELECT * FROM web_widget WHERE _id = %d", widget["web_widget_id"])
		web_widgets := Query(db_web, sql)
		web_widget := web_widgets[0]

		udn_data["widget"].(map[string]interface{})[widget["name"].(string)] = web_widget["html"]
	}

	// Processing UDN: which updates the data pool at udn_data
	if widget_instance["udn_data_json"] != nil {
		ProcessSchemaUDNSet(db_web, udn_schema, widget_instance["udn_data_json"].(string), udn_data)
	} else {
		UdnLogLevel(udn_schema, log_debug, "UDN Execution: %s: None\n\n", site_page_widget["name"])
	}

	// We have prepared the data, we can now execute the Widget Instance UDN, which will string append the output to udn_data["widget_instance"]["output_location"] when done
	if web_widget_instance["udn_data_json"] != nil {
		ProcessSchemaUDNSet(db_web, udn_schema, web_widget_instance["udn_data_json"].(string), udn_data)
	} else {
		UdnLogLevel(udn_schema, log_debug, "Widget Instance UDN Execution: %s: None\n\n", site_page_widget["name"])
	}
}
