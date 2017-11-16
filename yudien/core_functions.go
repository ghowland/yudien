package yudien

import (
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
	} else {
		user_map["error"] = ldap_user.Error

		result.Result = user_map
		result.Error = ldap_user.Error
		return result
	}

	// Get the user (if it exists)
	filter := map[string]interface{}{}
	filter["name"] = []interface{}{"=", ldap_user.Username}

	filter_options := make(map[string]interface{})
	user_data_result := DatamanFilter("user", filter, filter_options)

	fmt.Printf("DatamanFilter: RESULT: %v\n", user_data_result)

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
			UdnLog(udn_schema, "Cannot marshal User Map data: %s\n", err)
		}
		user_data["ldap_data_json"] = string(user_map_json)

		// Save the new user into the DB
		user_data = DatamanSet("user", user_data)

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
		web_user_session = DatamanSet("web_user_session", web_user_session)

	} else {
		// Save the session information
		web_user_session = web_user_session_filter[0]
	}

	//TODO(g): Ensure they have a user account in our DB, save the ldap_user data, update UDN with their session data...

	// Trying to update the fetch code
	/*
		get_options := make(map[string]interface{})
		get_options["web_site_id"] = web_site["_id"]
		get_options["name"] = session_id_value
		user_session := DatamanGet("web_user_session", get_options)

		get_options = make(map[string]interface{})
		get_options["_id"] = user_session["user_id"]
		user_data := DatamanGet("user", get_options)
	*/

	// What we are doing currently...
	/*
		// Verify that this user is logged in, render the login page, if they arent logged in
		udn_data["session"] = make(map[string]interface{})
		udn_data["user"] = make(map[string]interface{})
		udn_data["user_data"] = make(map[string]interface{})
		udn_data["web_site"] = web_site
		udn_data["web_site_page"] = web_site_page
		if session_value, ok := udn_data["cookie"].(map[string]interface{})["opsdb_session"]; ok {
			session_sql := fmt.Sprintf("SELECT * FROM web_user_session WHERE web_site_id = %d AND name = '%s'", web_site["_id"], SanitizeSQL(session_value.(string)))
			session_rows := Query(db_web, session_sql)
			if len(session_rows) == 1 {
				session := session_rows[0]
				user_id := session["user_id"]

				fmt.Printf("Found User ID: %d  Session: %v\n\n", user_id, session)

				// Load session from json_data
				target_map := make(map[string]interface{})
				if session["data_json"] != nil {
					err := json.Unmarshal([]byte(session["data_json"].(string)), &target_map)
					if err != nil {
						log.Panic(err)
					}
				}

				fmt.Printf( "Session Data: %v\n\n", target_map)

				udn_data["session"] = target_map

				// Load the user data too
				user_sql := fmt.Sprintf("SELECT * FROM \"user\" WHERE _id = %d", user_id)
				user_rows := Query(db_web, user_sql)
				target_map_user := make(map[string]interface{})
				if len(user_rows) == 1 {
					// Set the user here
					udn_data["user"] = user_rows[0]

					// Load from user data from json_data
					if user_rows[0]["data_json"] != nil {
						err := json.Unmarshal([]byte(user_rows[0]["data_json"].(string)), &target_map_user)
						if err != nil {
							log.Panic(err)
						}
					}
				}
				fmt.Printf("User Data: %v\n\n", target_map_user)

				udn_data["user_data"] = target_map_user
			}
		}
	*/

	//TODO(g): Login returns the SESSION_ID

	result.Result = web_user_session["name"]

	return result
}

func UDN_DddRender(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "DDD Render: %v\n\nInput: %s\n\n", args, JsonDump(input))

	position_location := GetResult(args[0], type_string).(string)
	move_x := GetResult(args[1], type_int).(int64)
	move_y := GetResult(args[2], type_int).(int64)
	is_delete := GetResult(args[3], type_int).(int64)
	ddd_id := GetResult(args[4], type_int).(int64)
	data_location := GetResult(args[5], type_string).(string)          // The data (record) we are operating on should be at this location
	save_data := GetResult(args[6], type_map).(map[string]interface{}) // This is incoming data, and will be only for the position_location's data, not the complete record
	temp_id := GetResult(args[7], type_int).(int64)                    // Initial value is passed in as 0, not empty string or nil

	UdnLog(udn_schema, "\nDDD Render: Position: %s  Move X: %d  Y: %d  Is Delete: %d  DDD: %d  Data Location: %s\nSave Data:\n%s\n\n", position_location, move_x, move_y, is_delete, ddd_id, data_location, JsonDump(save_data))

	//TEST: Add some static rows...
	input_map := input.(map[string]interface{})
	input_map_rows := input_map["form"].([]interface{})

	//TODO(g): Process the move_x/y with position location.  Get a new position location.  Do this same thing with the buttons, and test each one for validity to see if we should add that button
	//		Just update the string with the move, then do the get.  Makes it simple, no working 2 things at once.  String is manipulated, and get.  That's it.

	// -- Do work here to change stuff

	// Move, if we need to
	position_location = DddMove(position_location, move_x, move_y)
	fmt.Printf("DDD Render: After move: %s\n", position_location)

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
		fmt.Printf("DddRender: Data Record: %s: %s\n\n", data_location, JsonDump(data_record))

		// Put this data into the temp table, and get our temp_id
		temp_data := make(map[string]interface{})
		temp_data["data_json"] = JsonDump(data_record)
		temp_data_result := DatamanSet("temp", temp_data)
		fmt.Printf("Temp data result: %v\n\n", temp_data_result)
		temp_id = temp_data_result["_id"].(int64)
	} else {
		// Get the ddd_data from the temp table
		temp_options := make(map[string]interface{})
		temp_record := DatamanGet("temp", int(temp_id), temp_options)

		err := json.Unmarshal([]byte(temp_record["data_json"].(string)), &data_record)
		if err != nil {
			panic(err)
		}
	}
	//fmt.Printf("DDD Data Record: (%d): %s\n\n", temp_id, JsonDump(data_record))

	// Get the DDD node, which has our
	ddd_label, ddd_node, ddd_cursor_data := DddGetNode(position_location, ddd_data, data_record, udn_data)

	fmt.Printf("DDD Node: %s\n\n", JsonDump(ddd_node))
	fmt.Printf("DDD Cursor Data: %s\n\n", JsonDump(ddd_cursor_data))

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

	fmt.Printf("\nDDD Render: Result:\n%s\n\n", JsonDump(input_map))

	return result
}

func UDN_Library_Query(db *sql.DB, sql string) []interface{} {
	// Query
	rs, err := db.Query(sql)
	if err != nil {
		log.Fatal(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}
	defer rs.Close()

	// create a fieldbinding object.
	var fArr []string
	fb := fieldbinding.NewFieldBinding()

	if fArr, err = rs.Columns(); err != nil {
		log.Fatal(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}

	fb.PutFields(fArr)

	// Final output, array of maps
	//result_list := list.New()
	result_list := make([]interface{}, 0)

	for rs.Next() {
		if err := rs.Scan(fb.GetFieldPtrArr()...); err != nil {
			log.Fatal(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
		}

		template_map := make(map[string]interface{})

		for key, value := range fb.GetFieldArr() {
			//UdnLog(udn_schema, "Found value: %s = %s\n", key, value)

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
		log.Fatal(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}

	return result_list
}

func UDN_QueryById(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}

	UdnLog(udn_schema, "Query: %v\n", args)

	//arg_0 := args.Front().Value.(*UdnResult)
	arg_0 := args[0]

	// The 2nd arg will be a map[string]interface{}, so ensure it exists, and get it from our args if it was passed in
	arg_1 := make(map[string]interface{})
	if len(args) > 1 {
		//UdnLog(udn_schema, "Query: %s  Stored Query: %s  Data Args: %v\n", udn_start.Value, arg_0, args[1])

		//TODO(g):VALIDATE: Validation and error handling
		arg_1 = GetResult(args[1], type_map).(map[string]interface{})
	}

	UdnLog(udn_schema, "Query: %s  Stored Query: %s  Data Args: %v\n", udn_start.Value, arg_0, arg_1)

	query_sql := fmt.Sprintf("SELECT * FROM datasource_query WHERE _id = %s", arg_0)

	//TODO(g): Make a new function that returns a list of UdnResult with map.string

	// This returns an array of TextTemplateMap, original method, for templating data
	query_result := Query(db, query_sql)

	sql_parameters := make(map[string]string)
	has_params := false
	if query_result[0]["parameter_json_data"] != nil {
		//UdnLog(udn_schema, "-- Has params: %v\n", query_result[0]["parameter_data_json"])
		err := json.Unmarshal([]byte(query_result[0]["parameter_json_data"].(string)), &sql_parameters)
		if err != nil {
			log.Panic(err)
		}
		has_params = true
	} else {
		UdnLog(udn_schema, "-- No params\n")
	}

	result_sql := fmt.Sprintf(query_result[0]["sql"].(string))

	UdnLog(udn_schema, "Query: SQL: %s   Params: %v\n", result_sql, sql_parameters)

	// If we have params, then format the string for each of them, from our arg map data
	if has_params {
		for param_key, _ := range sql_parameters {
			replace_str := fmt.Sprintf("{{%s}}", param_key)
			//value_str := fmt.Sprintf("%s", param_value)

			// Get the value from the arg_1
			value_str := fmt.Sprintf("%v", arg_1[param_key])

			//UdnLog(udn_schema, "REPLACE PARAM:  Query: SQL: %s   Replace: %s   Value: %s\n", result_sql, replace_str, value_str)

			result_sql = strings.Replace(result_sql, replace_str, value_str, -1)

			//UdnLog(udn_schema, "POST-REPLACE PARAM:  Query: SQL: %s   Replace: %s   Value: %s\n", result_sql, replace_str, value_str)
		}

		UdnLog(udn_schema, "Query: Final SQL: %s\n", result_sql)
	}

	// This query returns a list.List of map[string]interface{}, new method for more-raw data
	result.Result = UDN_Library_Query(db, result_sql)

	UdnLog(udn_schema, "Query: Result [Items: %d]: %s\n", len(result.Result.([]interface{})), SnippetData(GetResult(result, type_string), 60))

	//// DEBUG
	//result_list := result.Result.(*list.List)
	//for item := result_list.Front(); item != nil; item = item.Next() {
	//	real_item := item.Value.(map[string]interface{})
	//	UdnLog(udn_schema, "Query Result Value: %v\n", real_item)
	//}

	return result
}

func UDN_DebugOutput(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}
	result.Result = input

	type_str := fmt.Sprintf("%T", input)

	if type_str == "*list.List" {
		UdnLog(udn_schema, "Debug Output: List: %s: %v\n", type_str, SprintList(*input.(*list.List)))

	} else {
		UdnLog(udn_schema, "Debug Output: %s: %s\n", type_str, JsonDump(input))
	}

	return result
}

func UDN_TestReturn(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Test Return data: %s\n", args[0])

	result := UdnResult{}
	result.Result = args[0]

	return result
}

func UDN_Widget(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Widget: %v\n", args[0])

	udn_data_page := udn_data["page"].(map[string]interface{})

	result := UdnResult{}
	//result.Result = udn_data["widget"].Map[arg_0.Result.(string)]
	result.Result = udn_data_page[args[0].(string)] //TODO(g): We get this from the page map.  Is this is the best naming?  Check it...

	return result
}

func UDN_StringTemplateFromValueShort(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	//UdnLog(udn_schema, "\n\nShort Template: %v  Input: %v\n\n", SnippetData(args, 60), SnippetData(input, 60))
	//UdnLog(udn_schema, "\n\n--- Short Template ---: %v  Input:\n%v\n\n", SnippetData(args, 60), input)

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
				UdnLog(udn_schema, "Short Template: Converting from array to string: %s\n", SnippetData(actual_input, 60))
				actual_input = GetResult(actual_input, type_string)
			} else {
				UdnLog(udn_schema, "Short Template: Input is not an array: %s\n", SnippetData(actual_input, 60))
				//UdnLog(udn_schema, "String Template: Input is not an array: %s\n", actual_input)
			}
		} else {
			UdnLog(udn_schema, "Short Template: Input is nil\n")
		}*/

	template_str := GetResult(args[0], type_string).(string)

	UdnLog(udn_schema, "Short Template From Value: Template String: %s Template Input: %v\n\n", SnippetData(actual_input, 60), SnippetData(template_str, 60))

	// Use the actual_input, which may be input or arg_1
	input_template_map := GetResult(actual_input, type_map).(map[string]interface{})

	for key, value := range input_template_map {
		//fmt.Printf("Key: %v   Value: %v\n", key, value)
		key_replace := fmt.Sprintf("{{{%s}}}", key)
		value_str := GetResult(value, type_string).(string)
		template_str = strings.Replace(template_str, key_replace, value_str, -1)
	}

	result := UdnResult{}
	result.Result = template_str

	UdnLog(udn_schema, "Short Template From Value:  Result:  %v\n\n", template_str)

	return result
}

func UDN_StringTemplateFromValue(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	//UdnLog(udn_schema, "\n\nString Template: \n%v\n\n", args)

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
				UdnLog(udn_schema, "String Template: Converting from array to string: %s\n", SnippetData(actual_input, 60))
				actual_input = GetResult(actual_input, type_map)
			} else {
				UdnLog(udn_schema, "String Template: Input is not an array: %s\n", SnippetData(actual_input, 60))
				//UdnLog(udn_schema, "String Template: Input is not an array: %s\n", actual_input)
			}
		} else {
			UdnLog(udn_schema, "String Template: Input is nil\n")
		}
	*/

	template_str := GetResult(args[0], type_string).(string)

	UdnLog(udn_schema, "String Template From Value: Template Input: %s Template String: %v\n\n", SnippetData(actual_input, 60), SnippetData(template_str, 60))

	UdnLog(udn_schema, "String Template From Value: Template Input: %s\n\n", JsonDump(actual_input))

	// Use the actual_input, which may be input or arg_1
	input_template := NewTextTemplateMap()
	input_template.Map = GetResult(actual_input, type_map).(map[string]interface{})

	item_template := template.Must(template.New("text").Parse(template_str))

	item := StringFile{}
	err := item_template.Execute(&item, input_template)
	if err != nil {
		log.Fatal(err)
	}

	result := UdnResult{}
	result.Result = item.String

	return result
}

func UDN_StringTemplateMultiWrap(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {

	//UdnLog(udn_schema, "\n\nString Template: \n%v\n\n", args)

	wrap_key := GetResult(args[0], type_string).(string)

	// Ensure our arg count is correct
	if len(args) < 2 {
		panic("Wrong number of arguments.  Map Template takes N 2-tuples: set_key, map_data.  The first map_data may be skipped if there is only one set_key, input will be used.")
	} else if len(args) > 3 || len(args)%2 != 1 {
		panic("Wrong number of arguments.  Map Template takes N 2-tuples: set_key, map_data")
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

		UdnLog(udn_schema, "String Template From Value: Template String: %s Template Input: %v\n\n", SnippetData(current_input, 60), SnippetData(template_str, 60))

		// Use the actual_input, which may be input or arg_1
		input_template := NewTextTemplateMap()
		input_template.Map = GetResult(current_input, type_map).(map[string]interface{})

		item_template := template.Must(template.New("text").Parse(template_str))

		item := StringFile{}
		err := item_template.Execute(&item, input_template)
		if err != nil {
			log.Fatal(err)
		}

		// Set the current_output for return, and put it in our udn_data, so we can access it again
		current_output = item.String
	}

	result := UdnResult{}
	result.Result = current_output

	return result
}

func UDN_MapStringFormat(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Map String Format: %v\n", args)

	// Ensure our arg count is correct
	if len(args) < 2 || len(args)%2 != 0 {
		panic("Wrong number of arguments.  Map Template takes N 2-tuples: set_key, format")
	}

	items := len(args) / 2

	for count := 0; count < items; count++ {
		offset := count * 2

		set_key := GetResult(args[offset+0], type_string).(string)
		format_str := GetResult(args[offset+1], type_string_force).(string)

		UdnLog(udn_schema, "Format: %s  Format String: %s  Input: %v\n", set_key, SnippetData(format_str, 60), SnippetData(input, 60))

		if input != nil {
			input_template := NewTextTemplateMap()
			input_template.Map = input.(map[string]interface{})

			item_template := template.Must(template.New("text").Parse(format_str))

			item := StringFile{}
			err := item_template.Execute(&item, input_template)
			if err != nil {
				log.Fatal(err)
			}

			// Save the templated string to the set_key in our input, so we are modifying our input
			input.(map[string]interface{})[set_key] = item.String

			UdnLog(udn_schema, "Format: %s  Result: %s\n\n", set_key, item.String)
		} else {
			input.(map[string]interface{})[set_key] = format_str

			UdnLog(udn_schema, "Format: %s  Result (No Templating): %s\n\n", set_key, format_str)
		}

	}

	result := UdnResult{}
	result.Result = input

	UdnLog(udn_schema, "Map String Format: Result: %s\n\n", JsonDump(input))

	return result
}

func UDN_MapTemplate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Map Template: %v\n", args)

	// Ensure our arg count is correct
	if len(args) < 3 || len(args)%3 != 0 {
		panic("Wrong number of arguments.  Map Template takes N 3-tuples: set_key, text, map")
	}

	items := len(args) / 3

	for count := 0; count < items; count++ {
		offset := count * 3

		set_key := args[offset].(string)
		template_str := GetResult(args[offset+1], type_string).(string)
		template_data := GetResult(args[offset+2], type_map).(map[string]interface{})

		UdnLog(udn_schema, "Map Template: %s Template String: %s Template Data: %v Template Input: %v\n\n", set_key, SnippetData(template_str, 60), SnippetData(template_data, 60), SnippetData(input, 60))

		input_template := NewTextTemplateMap()
		input_template.Map = template_data

		item_template := template.Must(template.New("text").Parse(template_str))

		item := StringFile{}
		err := item_template.Execute(&item, input_template)
		if err != nil {
			log.Fatal(err)
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
	UdnLog(udn_schema, "Map Update: %s  Over Input: %s\n", SnippetData(update_map, 60), SnippetData(input, 60))

	for k, v := range update_map {
		input.(map[string]interface{})[k] = v
	}

	result := UdnResult{}
	result.Result = input

	fmt.Printf("Map Update: Result: %v", input)

	return result
}

func UDN_HtmlEncode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "HTML Encode: %v\n", SnippetData(input, 80))

	input_str := GetResult(input, type_string).(string)

	// Replace all the characters with their fixed HTML alternatives
	input_str = strings.Replace(input_str, "<", "&lt;", -1)
	input_str = strings.Replace(input_str, ">", "&gt;", -1)
	input_str = strings.Replace(input_str, "&", "&amp;", -1)

	result := UdnResult{}
	result.Result = input_str

	//UdnLog(udn_schema, "HTML Encode: Result: %v\n", SnippetData(input_str, 80))
	UdnLog(udn_schema, "HTML Encode: Result: %v\n", input_str)

	return result
}

func UDN_StringAppend(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "String Append: %v\n", args)

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

	UdnLog(udn_schema, "String Append: %v  Current: %s  Append (%T): %s\n\n", args, SnippetData(access_str, 60), input, SnippetData(input, 60))

	// Append
	access_str = fmt.Sprintf("%s%s", access_str, GetResult(input, type_string).(string))

	//UdnLog(udn_schema, "String Append: %v  Appended:\n%s\n\n", args, access_str)		//DEBUG

	// Save the appended string
	UDN_Set(db, udn_schema, udn_start, args, access_str, udn_data)

	result := UdnResult{}
	result.Result = access_str

	return result
}

func UDN_StringClear(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "String Clear: %v\n", args)

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

func UDN_StringConcat(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "String Concat: %v\n", args)

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
	UdnLog(udn_schema, "String Concat: %v\n", args)

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

func UDN_StringLower(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "String Lower: %v\n", args)

	arg_0 := GetResult(args[0], type_string).(string)

	result := UdnResult{}
	result.Result = strings.ToLower(arg_0)

	return result
}

func UDN_StringUpper(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "String Upper: %v\n", args)

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
		UdnLog(udn_schema, "Input: No args, returning input: %v\n", input)
		return result
	}

	UdnLog(udn_schema, "Input: %v\n", args[0])

	result := UdnResult{}
	result.Result = args[0]

	return result
}

func UDN_InputGet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	cur_result := input

	UdnLog(udn_schema, "Input Get: %v   Input: %v\n", args, SnippetData(input, 60))

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
	UdnLog(udn_schema, "Stored Function: %s\n", SnippetData(args, 80))

	function_name := GetResult(args[0], type_string).(string)

	function_domain_id := udn_data["web_site"].(map[string]interface{})["udn_stored_function_domain_id"]

	sql := fmt.Sprintf("SELECT * FROM udn_stored_function WHERE name = '%s' AND udn_stored_function_domain_id = %d", function_name, function_domain_id)

	function_rows := Query(db, sql)

	// Get all our args, after the first one (which is our function_name)
	udn_data["function_arg"] = GetResult(args[1:], type_map)

	//UdnLog(udn_schema, "Stored Function: Args: %d: %s\n", len(udn_data["function_arg"].(map[string]interface{})), SprintMap(udn_data["function_arg"].(map[string]interface{})))

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

	UdnLog(udn_schema, "Execute: UDN String As Target: %s\n", udn_target)

	// Execute the Target against the input
	result := UdnResult{}
	//result.Result = ProcessUDN(db, udn_schema, udn_source, udn_target, udn_data)

	result.Result = ProcessSingleUDNTarget(db, udn_schema, udn_target, input, udn_data)

	return result
}

func UDN_ArraySlice(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// UdnLog(udn_schema, "Slice: %v\n", SnippetData(args, 80))

	result := UdnResult{}

	start_index := 0
	end_index := 0
	args_len := len(args)
	input_len := 0

	// Find len of input array
	switch input.(type){
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
	} else if args_len == 1 { // Only start index given. Assume end_index is at end of array
		start_int, err := strconv.Atoi(args[0].(string))

		if err == nil{
			start_index = start_int
			end_index = input_len
		} else {
			result.Result = input
			return result
		}
	} else { // Both start and end indices are given
		start_int, err1 := strconv.Atoi(args[0].(string))
		end_int, err2 := strconv.Atoi(args[1].(string))

		if err1 == nil && err2 == nil {
			start_index = start_int
			end_index = end_int
		} else {
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
	case []interface{}:
		result.Result = input.([]interface{})[start_index:end_index]
	case []map[string]interface{}:
		result.Result = input.([]map[string]interface{})[start_index:end_index]
	default: // Cannot recognize input array type. Return input
		result.Result = input
	}

	return result
}

func UDN_ArrayAppend(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLog(udn_schema, "Array Append: %v\n", args)

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

	return result
}

func UDN_ArrayDivide(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	divisor, err := strconv.Atoi(args[0].(string))

	// Dont process this, if it isnt valid...  Just pass through
	if err != nil || divisor <= 0 {
		UdnLog(udn_schema, "ERROR: Divisor is invalid: %d\n", divisor)
		result := UdnResult{}
		result.Result = input
		return result
	}

	UdnLog(udn_schema, "Array Divide: %v\n", divisor)

	// Make the new array.  This will be a 2D array, from our 1D input array
	result_array := make([]interface{}, 0)
	current_array := make([]interface{}, 0)

	// Loop until we have taken account of all the elements in the array
	for count, element := range input.([]interface{}) {
		if count%divisor == 0 && count > 0 {
			result_array = AppendArray(result_array, current_array)
			current_array = make([]interface{}, 0)

			UdnLog(udn_schema, "Adding new current array: %d\n", len(result_array))
		}

		current_array = AppendArray(current_array, element)
		UdnLog(udn_schema, "Adding new current array: Element: %d\n", len(current_array))
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

	UdnLog(udn_schema, "Array Map Remap: %v\n", remap)

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

func UDN_RenderDataWidgetInstance(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//TODO(g): Take arg3 as optional argument, which is a map of control values.  Allow "dialog=true" to wrap any result in a dialog window.  This will allow non-dialog items to be rendered in a dialog.
	//

	//TODO(g): Make Dialog Form use this and change it to Form.  Then it is ready to be used in a normal page, and I can just wrap it with a Dialog...  Pass in the dialog title and any options (width).
	//

	UdnLog(udn_schema, "Render Data Widget Instance: %v\n", args)

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
		fmt.Printf("Render Data Widget Instance: Update udn_data: %s: %v\n", key, value)
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
	UdnLog(udn_schema, "JSON Decode: %v\n", args)

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

	//UdnLog(udn_schema, "JSON Decode: Result: %v\n", decoded_map)
	UdnLog(udn_schema, "JSON Decode: Result: %s\n", SnippetData(decoded_interface, 120))

	return result
}

func UDN_JsonEncode(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "JSON Encode: %v\n", args)

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

	UdnLog(udn_schema, "JSON Encode: Result: %v\n", result.Result)

	return result
}

func UDN_DataGet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Data Get: %v\n", args)

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
	UdnLog(udn_schema, "Data Set: %v\n", args)

	collection_name := GetResult(args[0], type_string).(string)
	record := GetResult(args[1], type_map).(map[string]interface{})

	result_map := DatamanSet(collection_name, record)

	result := UdnResult{}
	result.Result = result_map

	return result
}

func UDN_DataFilter(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Data Filter: %v\n", args)

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

func UDN_MapKeyDelete(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Map Key Delete: %v\n", args)

	for _, key := range args {
		delete(input.(map[string]interface{}), key.(string))
	}

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_MapKeySet(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Map Key Set: %v\n", args)

	// Ensure our arg count is correct
	if len(args) < 2 || len(args)%2 != 0 {
		panic("Wrong number of arguments.  Map Template takes N 2-tuples: set_key, format")
	}

	items := len(args) / 2

	for count := 0; count < items; count++ {
		offset := count * 2

		set_key := GetResult(args[offset+0], type_string).(string)
		value_str := GetResult(args[offset+1], type_string_force).(string)

		UdnLog(udn_schema, "Map Key Set: %s  Value String: %s  Input: %v\n", set_key, SnippetData(value_str, 60), SnippetData(input, 60))

		input.(map[string]interface{})[set_key] = value_str

	}

	result := UdnResult{}
	result.Result = input

	UdnLog(udn_schema, "Map Key Set: Result: %s\n\n", JsonDump(input))

	return result
}

func UDN_MapCopy(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Map Copy: %v\n", args)

	new_map := make(map[string]interface{})

	for key, value := range input.(map[string]interface{}) {
		new_map[key] = value
	}

	result := UdnResult{}
	result.Result = new_map

	return result
}

func UDN_CompareEqual(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Compare: Equal: %v\n", args)

	arg0 := GetResult(args[0], type_string_force).(string)
	arg1 := GetResult(args[1], type_string_force).(string)

	value := 1
	if arg0 != arg1 {
		value = 0
	}

	fmt.Printf("Compare: Equal: '%s' == '%s' : %d\n", arg0, arg1, value)

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_CompareNotEqual(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Compare: Equal: %v\n", args)

	arg0 := GetResult(args[0], type_string_force).(string)
	arg1 := GetResult(args[1], type_string_force).(string)

	value := 1
	if arg0 == arg1 {
		value = 0
	}

	fmt.Printf("Compare: Not Equal: '%s' != '%s' : %d\n", arg0, arg1, value)

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_Test(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Test Function\n")

	result := UdnResult{}
	result.Result = "Testing.  123."

	return result
}

func UDN_TestDifferent(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Different Test Function!!!\n")

	result := UdnResult{}
	result.Result = "Testing.  Differently."

	return result
}

func UDN_GetFirst(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Get First: %v\n", SnippetData(args, 300))

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
					UdnLog(udn_schema, "Get First: %v   Found: %v   Value: %v\n", SnippetData(args, 300), arg_str, result.Result)
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
				UdnLog(udn_schema, "Get First: %v   Found: %v\n", SnippetData(args, 300), arg_str)
			}
		}

		// Always stop if we have a result here
		if result.Result != nil {
			break
		}
	}

	//UdnLog(udn_schema, "Get: %v   Result: %v\n", SnippetData(args, 80), SnippetData(result.Result, 80))
	UdnLog(udn_schema, "Get First: %v   Result: %v\n", SnippetData(args, 300), result.Result)

	return result
}

func UDN_Get(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLog(udn_schema, "Get: %v\n", SnippetData(args, 80))

	result := UdnResult{}
	result.Result = MapGet(args, udn_data)

	//UdnLog(udn_schema, "Get: %v   Result: %v\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_Set(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLog(udn_schema, "Set: %v   Input: %s\n", SnippetData(args, 80), SnippetData(input, 40))

	result := UdnResult{}
	result.Result = MapSet(args, input, udn_data)

	//UdnLog(udn_schema, "Set: %v  Result: %s\n\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_GetIndex(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLog(udn_schema, "Get Index: %v\n", SnippetData(args, 80))

	result := UdnResult{}

	if len(args) > 0 {
		result.Result = MapGet(args, input)
	} else {
		result.Result = input
	}

	//UdnLog(udn_schema, "Get Index: %v   Result: %v\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_SetIndex(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLog(udn_schema, "Set: %v   Input: %s\n", SnippetData(args, 80), SnippetData(input, 40))

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

	//UdnLog(udn_schema, "Set: %v  Result: %s\n\n", SnippetData(args, 80), SnippetData(result.Result, 80))

	return result
}

func UDN_GetTemp(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	function_stack := udn_data["__function_stack"].([]map[string]interface{})
	function_stack_item := function_stack[len(function_stack)-1]
	function_uuid := function_stack_item["uuid"].(string)
	UdnLog(udn_schema, "Get Temp: %s: %v\n", function_uuid, SnippetData(args, 80))

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

func UDN_SetTemp(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	function_stack := udn_data["__function_stack"].([]map[string]interface{})
	function_stack_item := function_stack[len(function_stack)-1]
	function_uuid := function_stack_item["uuid"].(string)
	UdnLog(udn_schema, "Set Temp: %s: %v   Input: %s\n", function_uuid, SnippetData(args, 80), SnippetData(input, 40))

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

func UDN_Iterate(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	// Will loop over all UdnParts until it finds __end_iterate.  It expects input to hold a list.List, which use to iterate and execute the UdnPart blocks
	// It will set a variable that will be accessable by the "__get.current.ARG0"
	// Will return a list.List of each of the loops, which allows for filtering the iteration

	// This is our final input list, as an array, it always works and gets input to pass into the first function
	input_array := GetResult(input, type_array).([]interface{})

	UdnLog(udn_schema, "Iterate: [%s]  Input: %s\n\n", udn_start.Id, SnippetData(input_array, 240))

	// Our result will be a list, of the result of each of our iterations, with a UdnResult per element, so that we can Transform data, as a pipeline
	result := UdnResult{}
	result_list := make([]interface{}, 0)

	// If we have something to iterate over
	if len(input_array) > 0 {
		// Loop over the items in the input
		for _, item := range input_array {
			UdnLog(udn_schema, "\n====== Iterate Loop Start: [%s]  Input: %v\n\n", udn_start.Id, SnippetData(item, 300))

			// Get the input
			current_input := item

			// Variables for looping over functions (flow control)
			udn_current := udn_start

			// Loop over the UdnParts, executing them against the input, allowing it to transform each time
			for udn_current != nil && udn_current.Id != udn_start.BlockEnd.Id && udn_current.NextUdnPart != nil {
				udn_current = udn_current.NextUdnPart

				//UdnLog(udn_schema, "  Walking ITERATE block [%s]: Current: %s   Current Input: %v\n", udn_start.Id, udn_current.Value, SnippetData(current_input, 60))

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
			UdnLog(udn_schema, "\n====== Iterate Finished: [%s]  NextUdnPart: %v\n\n", udn_start.Id, result.NextUdnPart)
		} else if result.NextUdnPart.NextUdnPart != nil {
			UdnLog(udn_schema, "\n====== Iterate Finished: [%s]  NextUdnPart: %v\n\n", udn_start.Id, result.NextUdnPart)
		} else {
			UdnLog(udn_schema, "\n====== Iterate Finished: [%s]  NextUdnPart: End of UDN Parts\n\n", udn_start.Id)
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

func UDN_IfCondition(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	arg_0 := args[0]

	UdnLog(udn_schema, "If Condition: %s\n", arg_0)

	// If this is true, all other blocks (else-if, else) will be skipped.  It doesnt matter which block this is, an IF/ELSE-IF/ELSE chain only executes 1 block
	executed_a_block := false
	// Track when we leave the "then" (first) block
	outside_of_then_block := false
	// Used to control when we skip a block
	skip_this_block := false

	// Evaluate whether we will execute the IF-THEN (first) block.  (We dont use a THEN, but thats the saying)
	execute_then_block := true
	if arg_0 == "0" || arg_0 == nil || arg_0 == 0 || arg_0 == false || arg_0 == "" {
		execute_then_block = false

		UdnLog(udn_schema, "If Condition: Not Executing THEN: %s\n", arg_0)
	} else {
		// We will execute the "then" block, so we mark this now, so we skip any ELSE-IF/ELSE blocks
		// Execute A Block, means we should execute at least one
		executed_a_block = true

		UdnLog(udn_schema, "If Condition: Executing THEN: %s\n", arg_0)
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

		UdnLog(udn_schema, "Walking IF block: Current: %s   Current Input: %v\n", udn_current.Value, current_input)

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
				UdnLog(udn_schema, "Found non-main-if block, skipping: %s\n", udn_current.Value)
				break
			} else {
				// Else, we havent executed a block, so we need to determine if we should start executing.  This is only variable for "__else_if", "else" will always execute if we get here
				if udn_current.Value == "__else_if" {
					udn_current_arg_0 := udn_current.Children.Front().Value.(*UdnPart)
					// If we dont have a "true" value, then skip this next block
					if udn_current_arg_0.Value == "0" {
						skip_this_block = true
					} else {
						UdnLog(udn_schema, "Executing Else-If Block: %s\n", udn_current_arg_0.Value)
						// Mark block execution, so we wont do any more
						executed_a_block = true
					}
				} else {
					// This is an "__else", and we made it here, so we are executing the else.  Leaving this here to demonstrate that
					UdnLog(udn_schema, "Executing Else Block\n")
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
						UdnLog(udn_schema, "If: Flow Control: JUMPING to NextUdnPart: %s [%s]\n", current_result.NextUdnPart.Value, current_result.NextUdnPart.Id)
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
	UdnLog(udn_schema, "Else Condition\n")

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_ElseIfCondition(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Else If Condition\n")

	result := UdnResult{}
	result.Result = input

	return result
}

func UDN_Not(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Not: %v\n", SnippetData(input, 60))

	value := "0"
	if input != nil && input != "0" {
		value = "1"
	}

	result := UdnResult{}
	result.Result = value

	return result
}

func UDN_NotNil(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	UdnLog(udn_schema, "Not Nil: %v\n", SnippetData(input, 60))

	value := "0"
	if input != nil {
		value = "1"
	}

	result := UdnResult{}
	result.Result = value

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

		fmt.Printf("Web Data Widget Instance: %s\n", web_data_widget_instance["name"])

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

	fmt.Printf("Web Widget Instance: %s\n", web_widget_instance["name"])
	fmt.Printf("Web Widget Instance Data: %s\n", JsonDump(udn_data["widget_instance"]))

	// Get any static content associated with this page widget.  Then we dont need to worry about quoting or other stuff
	widget_static := make(map[string]interface{})
	udn_data["static_instance"] = widget_static
	if web_widget_instance["static_data_json"] != nil {
		err := json.Unmarshal([]byte(web_widget_instance["static_data_json"].(string)), &widget_static)
		if err != nil {
			log.Panic(err)
		}
	}

	fmt.Printf("Web Widget Instance Data Static: %s\n", JsonDump(udn_data["data_static"]))

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
		fmt.Printf("UDN Execution: %s: None\n\n", site_page_widget["name"])
	}

	// We have prepared the data, we can now execute the Widget Instance UDN, which will string append the output to udn_data["widget_instance"]["output_location"] when done
	if web_widget_instance["udn_data_json"] != nil {
		ProcessSchemaUDNSet(db_web, udn_schema, web_widget_instance["udn_data_json"].(string), udn_data)
	} else {
		fmt.Printf("Widget Instance UDN Execution: %s: None\n\n", site_page_widget["name"])
	}
}
