package yudien

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudiendata"
	. "github.com/ghowland/yudien/yudienutil"
	"github.com/segmentio/ksuid"
)


func DescribeUdnPart(part *UdnPart) string {

	depth_margin := strings.Repeat("  ", part.Depth)

	output := ""

	if part.PartType == part_function {
		output += fmt.Sprintf("%s%s: %-20s [%s]\n", depth_margin, PartTypeName[part.PartType], part.Value, part.Id)
	} else {
		output += fmt.Sprintf("%s%s: %-20s\n", depth_margin, PartTypeName[part.PartType], part.Value)
	}

	if part.BlockBegin != nil {
		output += fmt.Sprintf("%sBlock:  Begin: %s   End: %s\n", depth_margin, part.BlockBegin.Id, part.BlockEnd.Id)
	}

	if part.Children.Len() > 0 {
		output += fmt.Sprintf("%sArgs: %d\n", depth_margin, part.Children.Len())
		for child := part.Children.Front(); child != nil; child = child.Next() {
			output += DescribeUdnPart(child.Value.(*UdnPart))
		}
	}

	if part.NextUdnPart != nil {
		output += fmt.Sprintf("%sNext Command:\n", depth_margin)
		output += DescribeUdnPart(part.NextUdnPart)
	}

	return output
}

// Execution group allows for Blocks to be run concurrently.  A Group has Concurrent Blocks, which has UDN pairs of strings, so 3 levels of arrays for grouping
type UdnExecutionGroup struct {
	Blocks [][][]string
}

type UdnFunc func(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult

var UdnFunctions = map[string]UdnFunc{}

type LdapConfig struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	LoginDN     string `json:"login_dn"`
	Password    string `json:"password"`
	UserSearch  string `json:"user_search"`
	GroupSearch string `json:"group_search"`
}

var Ldap *LdapConfig
var DevelopmentUsers map[string]StaticUser
var UDNLogConfig *LoggingConfig

type LoggingConfig struct {
	OutputPath  string `json:"output_path"`
	Level string `json:"level"`
}

type WebsiteConfig struct {
	DefaultWebWidgetThemeId  int `json:"default_web_widget_theme_id"`
}

type StaticUser struct {
	Username  string `json:"username"`
	Password string `json:"password"`
	Data StaticUserData `json:"data"`
}

type StaticUserData struct {
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName string `json:"last_name"`
	Email string `json:"email"`
	HomeDir string `json:"home_dir"`
	Uid int64 `json:"uid"`
	Groups []string `json:"groups"`
}


type DatabaseAuthenticationName struct {
	Database string `json:"database"`
	Table string `json:"table"`
	Field string `json:"field"`
}

type DatabaseAuthenticationPassword struct {
	Database string `json:"database"`
	Table string `json:"table"`
	FieldDigest string `json:"field_digest"`
	FieldSalt string `json:"field_salt"`
	DigestMethod string `json:"digest_method"`
}

type DatabaseAuthentication struct {
	Name DatabaseAuthenticationName `json:"name"`
	Password DatabaseAuthenticationPassword `json:"password"`
	Verify string `json:"verify"`
}

type AuthenticationConfig struct {
	Method  string `json:"method"`
	IsProduction bool `json:"is_production"`
	DevelopmentUsers map[string]StaticUser `json:"development_users"`
	DatabaseAuthentication DatabaseAuthentication `json:"database_authentication"`
	LdapConfig LdapConfig `json:"ldap_authentication"`
}


func Configure(default_database *DatabaseConfig, databases map[string]DatabaseConfig, logging *LoggingConfig, authentication *AuthenticationConfig) {
	UdnLogLevel(nil, log_info,"Configuring Yudien\n")

	Ldap = &authentication.LdapConfig
	DevelopmentUsers = authentication.DevelopmentUsers

	UDNLogConfig = logging

	Debug_Udn_Log_Level = ParseUdnLogLevel(UDNLogConfig.Level) // see yudiencore/core.go func UdnLogLevels

	DefaultDatabase = default_database

	UdnLogLevel(nil, log_info,"\n\nConfig: Logging: %v\n\n", logging)
	//UdnLogLevel(nil, log_info,"\n\nConfig: Authentication: %v\n\n", authentication)

	InitDataman(*DefaultDatabase, databases)
}

func InitUdn() {
	Debug_Udn_Api = false // Legacy Logging
	Debug_Udn = false // Legacy Logging - see yudiencore/core.go func UdnLog

	UdnFunctions = map[string]UdnFunc{
		"__comment":      UDN_Comment,
		"__query":        UDN_QueryById,
		"__debug_output": UDN_DebugOutput,
		"__if":           UDN_IfCondition,
		"__end_if":       nil,
		"__else":         UDN_ElseCondition,
		"__end_else":     nil,
		"__else_if":      UDN_ElseIfCondition,
		"__end_else_if":  nil,
		"__not":          UDN_Not,
		"__not_nil":      UDN_NotNil,
		"__is_nil":       UDN_IsNil,
		"__iterate":      UDN_Iterate,
		"__end_iterate":  nil,
		"__while":        UDN_While,	 // While takes a condition (arg_0) and a max (arg_1:int) number of iterations, so it cannot run forever
		"__end_while":  nil,
		"__nil":          UDN_Nil,		// Returns nil
		"__get":          UDN_Get,
		"__set":          UDN_Set,
		"__get_index": 	  UDN_GetIndex, // Get data using input rather than args (otherwise same as __get)
		"__set_index": 	  UDN_SetIndex, // Set data like __set but does not the result is passed to output and not stored
		"__get_first":    UDN_GetFirst, // Takes N strings, which are dotted for udn_data accessing.  The first value that isnt nil is returned.  nil is returned if they all are
		"__get_temp":     UDN_GetTemp,  // Function stack based temp storage
		"__get_temp_key": UDN_GetTempKey, // Get the uuid of the current stack frame for temp variables
		"__set_temp":     UDN_SetTemp,  // Function stack based temp storage
		//"__temp_clear":          UDN_ClearTemp,
		//"__watch": UDN_WatchSyncronization,
		//"___watch_timeout": UDN_WatchTimeout,				//TODO(g): Should this just be an arg to __watch?  I think so...  Like if/else, watch can control the flow...
		//"__end_watch": nil,
		"__test_return":    UDN_TestReturn, // Return some data as a result
		"__test":           UDN_Test,
		"__test_different": UDN_TestDifferent,
		// Migrating from old functions
		//TODO(g): These need to be reviewed, as they are not necessarily the best way to do this, this is just the easiest/fastest way to do this
		"__widget": UDN_Widget,
		// New functions for rendering web pages (finally!)
		//"__template": UDN_StringTemplate,					// Does a __get from the args...
		"__template":       UDN_StringTemplateFromValue,      // Does a __get from the args...
		"__template_wrap":  UDN_StringTemplateMultiWrap,      // Takes N-2 tuple args, after 0th arg, which is the wrap_key, (also supports a single arg templating, like __template, but not the main purpose).  For each N-Tuple, the new map data gets "value" set by the previous output of the last template, creating a rolling "wrap" function.
		"__template_map":   UDN_MapTemplate,                  //TODO(g): Like format, for templating.  Takes 3*N args: (key,text,map), any number of times.  Performs template and assigns key into the input map
		"__format":         UDN_MapStringFormat,              //TODO(g): Updates a map with keys and string formats.  Uses the map to format the strings.  Takes N args, doing each arg in sequence, for order control
		"__template_short": UDN_StringTemplateFromValueShort, // Like __template, but uses {{{fieldname}}} instead of {{index .Max "fieldname"}}, using strings.Replace instead of text/template

		"__true":          UDN_True,
		"__false":          UDN_False,

		"__length":          UDN_Length,

		//TODO(g): DEPRICATE.  Longer name, same function.
		"__template_string": UDN_StringTemplateFromValue, // Templates the string passed in as arg_0

		"__string_append": UDN_StringAppend,
		"__string_clear":  UDN_StringClear, // Initialize a string to empty string, so we can append to it again
		"__string_replace":  UDN_StringReplace, // Initialize a string to empty string, so we can append to it again
		"__concat":        UDN_StringConcat,
		"__markdown_format": UDN_StringMarkdownFormat, // Format a string as HTML from markdown

		"__string_ends_with": UDN_StringEndsWith, // Returns boolean, if matches end of string
		"__string_begins_with": UDN_StringEndsWith, // Returns boolean, if matches beginning of string

		"__input":         UDN_Input,          //TODO(g): This takes any input as the first arg, and then passes it along, so we can type in new input to go down the pipeline...
		"__input_get":     UDN_InputGet,       // Gets information from the input, accessing it like __get
		"__function":      UDN_StoredFunction, //TODO(g): This uses the udn_stored_function.name as the first argument, and then uses the current input to pass to the function, returning the final result of the function.		Uses the web_site.udn_stored_function_domain_id to determine the stored function
		"__execute":       UDN_Execute,        // Can take single string or the tripple array of UDN statements

		"__html_encode":     UDN_HtmlEncode, // Encode HTML symbols so they are not taken as literal HTML

		"__array_append":    UDN_ArrayAppend, // Appends the input into the specified target location (args)
		"__array_append_array":    UDN_ArrayAppendArray, // Appends an array (input) into the specified location, like __array_append
		"__array_slice": 	 UDN_ArraySlice, // Slices an input array based on the start and end index
		"__array_divide":    UDN_ArrayDivide,   // Breaks an array up into a set of arrays, based on a divisor.  Ex: divide=4, a 14 item array will be 4 arrays, of 4/4/4/2 items each.
		"__array_remove":    UDN_ArrayRemove, // Removes the first instance of an element in an array.  Recquires exact match
		"__array_index":     UDN_ArrayIndex, // Gets the index of the first instance of an element in an array.  Requires exact match
		"__array_contains":  UDN_ArrayContains, // Returns boolean, if the specific array contains all the of input.  Input can be individual elemnent or an arry (converts to an array).
		"__array_contains_any":  UDN_ArrayContainsAny, // Returns boolean, if the specific array contains all the of input.  Input can be individual elemnent or an arry (converts to an array).
		"__array_map_update": UDN_ArrayMapUpdate, // Takes an array of maps, and overwrites all keys with the specified map
		"__array_map_remap": UDN_ArrayMapRemap, // Takes an array of maps, and makes a new array of maps, based on the arg[0] (map) mapping (key_new=key_old)
		"__array_map_to_map":  UDN_ArrayMapToMap, // Map an array of maps into a single map, using one of the keys
		"__array_map_to_series":  UDN_ArrayMapToSeries, // Map an array of maps into a single array, from one of the key.  Like a time series or other list of values.
		"__array_map_template":  UDN_ArrayMapTemplate, // Update all map's key's values with a template statement from each map's key/values
		"__array_map_key_set":  UDN_ArrayMapKeySet, // Update all map's with specified keys/values
		"__array_map_find":  UDN_ArrayMapFind, // Finds a single map element, and returns the result in a map of {key=key,value=value}
		"__array_map_find_update":  UDN_ArrayMapFindUpdate, //TODO(g): Remove once we transition everything to FilterUpdate, was incorrectly named as the first of it's kind. -- --  Finds all map elements that match the find map, and updates them with a passed in map, so we can find the map in-place of matching records.  Returns the array of maps
		"__array_map_filter_update":  UDN_ArrayMapFilterUpdate, //TODO(g): Update to FilterUpdate and replace code that uses it.  Unless worth having 2 functions.  Want to keep the name spaces and argument order/types similar accross all of them to avoid the PHP curse.  -- -- Finds all map elements that match the find map, and updates them with a passed in map, so we can find the map in-place of matching records.  Returns the array of maps
		"__array_map_filter_in":  UDN_ArrayMapFilterIn, // Returns all maps in an array, which keys were in the set of the array elements
		"__array_map_filter_contains":  UDN_ArrayMapFilterContains, // Returns all maps in an array, which keys values contains any of the possible array elements.  This works on arrays (, maps (match key/value), or single items (match)    //TODO(g): Add the non-array checks, only implemented array so far.
		"__array_map_filter_array_contains":  UDN_ArrayMapFilterArrayContains, // Returns all maps in an array, compares two arrays, and returns an array of all the matches between the 2 list.  Default is "any".  Options will be expanded to get more functionality when needed (need use case).

		"__array_string_join":  UDN_ArrayStringJoin, // Join an array of strings into a string (with separator)

		"__map_key_delete": UDN_MapKeyDelete, // Each argument is a key to remove
		"__map_key_set":    UDN_MapKeySet,    // Sets N keys, like __format, but with no formatting
		"__map_copy":       UDN_MapCopy,      // Make a copy of the current map, in a new map
		"__map_update":     UDN_MapUpdate,    // Input map has fields updated with arg0 map
		"__map_template_key":     UDN_MapTemplateKey,    // When we want to re-key a map, such as prefixing a UUID in front of the keys for replacement in an HTML document
		"__map_filter_array_contains":     UDN_MapFilterArrayContains,    // Filters elements in a map, if one of their keys contains at array we are comparing for containing values of another array
		"__map_filter_key":     UDN_MapFilterKey,    // Filters elements in a map based on their keys.  If their keys appear in a list, they are in the resulting map.  Otherwise they are filtered out.

		"__render_data": UDN_RenderDataWidgetInstance, // Renders a Data Widget Instance:  arg0 = web_data_widget_instance.id, arg1 = widget_instance map update

		"__json_decode": UDN_JsonDecode, // Decode JSON
		"__json_encode": UDN_JsonEncode, // Encode JSON
		"__json_encode_data": UDN_JsonEncodeData, // Encode JSON - Format as data.  No indenting, etc.

		"__base64_decode": UDN_Base64Decode, // Decode base64
		"__base64_encode": UDN_Base64Encode, // Encode base64


		//TODO(g): Make these the new defaults, which use CM
		"__change_get":    UDN_DataGet,    // Dataman Get
		"__change_set":    UDN_DataSet,    // Dataman Set
		"__change_submit":    UDN_ChangeDataSubmit,    // This accepts dotted notation and figures out what records/fields are being effected.  Example:  {"opsdb.schema_table_field.1050.name":"_id"}
		"__change_filter": UDN_DataFilter, // Dataman Filter
		"__change_filter_full": UDN_DataFilterFull, // Updated version of DatamanFilter that takes in JSON and allows multi-constraints
		//"__change_delete":    UDN_DataDelete,    // Dataman Delete
		//"__change_delete_filter":    UDN_DataDeleteFilter,    // Dataman Delete Filter
		//"__change_ensure_exists":    UDN_ChangeEnsureExists,    // Ensure that the specified data exists in the database.  Does not have Dataman equivalent functions, wrapper.
		//"__change_ensure_not_exists":    UDN_ChangeEnsureNotExists,    // Ensure that the specified data DOES NOT exist in the database.  Does not have Dataman equivalent functions, wrapper.

		"__safe_data_get":    UDN_SafeDataGet,    // Safe Dataman Get - Always connects to the correct database, and checks to ensure that
		"__safe_data_filter": UDN_SafeDataFilter, // Safe Dataman Filter - Correct DB and added filter args guarantee restricted access
		"__safe_data_filter_full": UDN_SafeDataFilterFull, // Safe Updated version of DatamanFilter that takes in JSON and allows multi-constraints
		//"__safe_data_set":    UDN_SafeDataSet,    // Safe Dataman Set - Ensures this is the correct business before allowing the set
		//"__safe_data_delete":    UDN_SafeDataDelete,    // Safe Dataman Delete
		//"__safe_data_delete_filter":    UDN_SafeDataDeleteFilter,    // Safe Dataman Delete Filter


		"__data_get":    UDN_DataGet,    // Dataman Get
		"__data_set":    UDN_DataSet,    // Dataman Set
		"__data_filter": UDN_DataFilter, // Dataman Filter
		"__data_filter_full": UDN_DataFilterFull, // Updated version of DatamanFilter that takes in JSON and allows multi-constraints
		"__data_delete":    UDN_DataDelete,    // Dataman Get
		"__data_delete_filter":    UDN_DataDeleteFilter,    // Dataman Set
		"__data_tombstone": UDN_DataTombstone, // Dataman "Delete" with a Tombstone marker: _is_deleted=true
		"__data_field_map_delete": UDN_DataFieldMapDelete, // Data field map delete - Go into JSON data and delete things


		"__time":          UDN_Time, // Return current time.Time object, and run AddDate if any args are passed in
		"__time_string":          UDN_TimeString, // Return string of the time
		"__time_string_date":          UDN_TimeStringDate, // Return string of the date

		"__time_series_get":    UDN_TimeSeriesGet,    // Time Series: Get
		"__time_series_filter":    UDN_TimeSeriesFilter,    // Time Series: Filter

		"__compare_equal":     UDN_CompareEqual,    // Compare equality, takes 2 args and compares them.  Returns 1 if true, 0 if false.  For now, avoiding boolean types...
		"__compare_not_equal": UDN_CompareNotEqual, // Compare equality, takes 2 args and compares them.  Returns 1 if true, 0 if false.  For now, avoiding boolean types...

		"__ddd_render": UDN_DddRender, // DDD Render.current: the JSON Dialog Form data for this DDD position.  Uses __ddd_get to get the data, and ___ddd_move to change position.

		"__uuid": UDN_Uuid, // Returns a UUID string

		"__login": UDN_Login, // Login through LDAP

		"__split":     UDN_StringSplit, // Split a string
		"__lower":     UDN_StringLower, // Lower case a string
		"__upper":     UDN_StringUpper, // Upper case a string
		"__join" :     UDN_StringJoin,  // Join an array into a string on a separator string

		"__debug_get_all_data": UDN_DebugGetAllUdnData, // Templates the string passed in as arg_0

		"__string_to_time": UDN_StringToTime, // Converts a string to Time.time object if possible (format is "2006-01-02T15:04:05")
		"__time_to_epoch": UDN_TimeToEpoch, // Converts a Time.time object to unix time in seconds
		"__time_to_epoch_ms": UDN_TimeToEpochMs, // Converts a Time.time object to unix time in milliseconds
		"__group_by": UDN_GroupBy, // Given a list of maps, group by some value based on that grouping
		"__math": UDN_Math,

		"__set_http_response": UDN_SetHttpResponseCode,
		"__num_to_string": UDN_NumberToString, // Given input number (int/int64/float64) and optional precision (int), outputs string (with specified precision/ original number)
		"__get_current_time": UDN_GetCurrentTime, // Given arg[0] string in the format 'YYYY-DD-MM hh:mm:ss'. If specific number given for YYYY, DD, MM, hh, mm, ss, use that number instead. Outputs go time.Time object of current time (UTC)
		"__get_local_time": UDN_GetLocalTime, // Given arg[0] string a valid time zone. Outputs go time.Time object of specified time zone, otherwise time.Time object of local time

		"__exec_command": UDN_ExecCommand,//UDN_ExecCommand, // Execute command line command. arg0 appname, arg1-n space delimited are args. 
		"__http_request": UDN_HttpRequest, //Sends http requests to the given url endpoint, returns the decoded response if have any.

		//TODO(g): I think I dont need this, as I can pass it to __ddd_render directly
		//"__ddd_move": UDN_DddMove,				// DDD Move position.current.x.y:  Takes X/Y args, attempted to move:  0.1.1 ^ 0.1.0 < 0.1 > 0.1.0 V 0.1.1
		//"__ddd_get": UDN_DddGet,					// DDD Get.current.{}
		//"__ddd_set": UDN_DddSet,					// DDD Set.current.{}
		//"__ddd_delete": UDN_DddDelete,			// DDD Delete.current: Delete the current item (and all it's sub-items).  Append will be used with __ddd_set/move

		"__increment": UDN_Increment,				// Given arg[0] int, int32, int64, float32, float64. Increment value by 1
		"__decrement": UDN_Decrement,   			// Given arg[0] int, int32, int64, float32, float64. Decrement value by 1
		//"__render_page": UDN_RenderPage,			// Render a page, and return it's widgets so they can be dynamically updated

		// New

		//"__map_update_prefix": UDN_MapUpdatePrefix,			//TODO(g): Merge a the specified map into the input map, with a prefix, so we can do things like push the schema into the row map, giving us access to the field names and such
		//"__map_clear": UDN_MapClear,			//TODO(g): Clears everything in a map "bucket", like: __map_clear.'temp'

		//"__function_domain": UDN_StoredFunctionDomain,			//TODO(g): Just like function, but allows specifying the udn_stored_function_domain.id as well, so we can use different namespaces.
		//"__capitalize": UDN_StringCapitalize,			//TODO(g): This capitalizes words, title-style
		//"__pluralize": UDN_StringPluralize,			//TODO(g): This pluralizes words, or tries to at least
		//"__starts_with": UDN_StringStartsWith,			//TODO(g): Returns bool if a string starts with the specified arg[0] string
		//"__ends_with": UDN_StringEndsWith,			//TODO(g): Returns bool if a string starts with the specified arg[0] string
		//"__get_session_data": UDN_SessionDataGet,			//TODO(g): Get something from a safe space in session data (cannot conflict with internal data)
		//"__set_session_data": UDN_SessionDataGet,			//TODO(g): Set something from a safe space in session data (cannot conflict with internal data)
		//"__continue": UDN_IterateContinue,		// Skip to next iteration
		// -- Dont think I need this -- //"__break": UDN_IterateBreak,				//TODO(g): Break this iteration, we are done.  Is this needed?  Im not sure its needed, and it might suck

		"__log_level": UDN_SetLogLevel,   	// Set the log level

		"__custom_populate_schedule_duty_responsibility": UDN_Custom_PopulateScheduleDutyResponsibility,   			// CUSTOM: Populate Schedule for Duty Responsibilities

		"__code": UDN_Custom_Code,   			// Code Execution from data.  First argument is DB to use, second is code_id, third is input_data override. //NOTE(g): This requires database tables to exist in the DB that match the functions requirements, which most UDN functions dont require.  Putting it in Custom code for now, but it will eventually be integrated into normal UDN once I have the schema population routines implemented.

		"__custom_health_check_promql": UDN_Custom_Health_Check_PromQL,	// CUSTOM: ...
		//"__custom_metric_filter": UDN_Custom_Metric_Filter,   			// CUSTOM: Fetch Metrics by name/labelset
		//"__custom_metric_get_values": UDN_Custom_Metric_Get_Values,   			// CUSTOM: Get TS values for list of metrics
		//"__custom_metric_rule_match_percent": UDN_Custom_Metric_Rule_Match_Percent,   	// CUSTOM: Returns a scalar, % of matches in the rules
		//"__custom_metric_handle_outage": UDN_Custom_Metric_Handle_Outage,   	// CUSTOM: Handles an any outages from health check failures on metrics
		//"__custom_metric_process_open_outages": UDN_Custom_Metric_Process_Open_Outages,   	// CUSTOM: Handles an any outages from health check failures on metrics
		"__custom_metric_process_alert_notifications": UDN_Custom_Metric_Process_Alert_Notifications,   	// CUSTOM: Processes any open Alert Notifications
		"__custom_metric_escalation_policy_oncall": UDN_Custom_Metric_Escalation_Policy_Oncall,   	// CUSTOM: Get the team/oncall members of the Escalation Policy

		"__custom_duty_shift_summary": UDN_Custom_Duty_Shift_Summary,	// CUSTOM: Get the Duty shift summary over a time range
		"__current_duty_responsibility_current_user": UDN_Custom_Duty_Responsibility_Current_User, // CUSTOM: ...

		"__customer_duty_roster_user_shift_info": UDN_Custom_Duty_Roster_User_Shift_Info,   	//CUSTOM: ....

		"__custom_weekly_activity": UDN_Custom_Activity_Daily, //CUSTOM: Weekly activity on a database/table/field

		"__custom_date_range_parse": UDN_Custom_Date_Range_Parse, //CUSTOM: Weekly activity on a database/table/field

		//"__customer_duty_responsibility_user_shift_next": UDN_Custom_Duty_Responsibility_User_Shift_Next,   	//CUSTOM: ....
		//"__customer_duty_responsibility_user_shift_previous": UDN_Custom_Duty_Responsibility_User_Shift_Previous,   	//CUSTOM: ....

		"__custom_monitor_post_process_change": UDN_Custom_Monitor_Post_Process_Change,   	// CUSTOM: Post change submit, process the data
		"__customer_monitor_post_process_change": UDN_Custom_Monitor_Post_Process_Change,   	//TODO(g):REMOVE: This one is a typo.  Switch to the above __custom_*// CUSTOM: Post change submit, process the data

		"__custom_dashboard_item_edit": UDN_Custom_Dashboard_Item_Edit,   	// CUSTOM:...

		"__custom_dataman_create_filter_html": UDN_Custom_Dataman_Create_Filter_Html,   	// CUSTOM:...

		"__custom_dataman_add_rule": UDN_Custom_Dataman_Add_Rule, // Add a rule for Dataman filter

		"__custom_login": UDN_Custom_Login,		// CUSTOM:...
		"__custom_auth": UDN_Custom_Auth,		// CUSTOM:...
	}

	PartTypeName = map[int]string{
		int(part_unknown):  "Unknown",
		int(part_function): "Function",
		int(part_item):     "Item",
		int(part_string):   "String",
		int(part_compound): "Compound",
		int(part_list):     "List",
		int(part_map):      "Map",
		int(part_map_key):  "Map Key",
	}
}

func init() {
	fmt.Printf("Initializing Yudien\n")
	InitUdn()
}

func Lock(lock string) {
	// This must lock things globally.  Global lock server required, only for this Customer though, since "global" can be customer oriented.
	UdnLogLevel(nil, log_debug, "Locking: %s\n", lock)

	// Acquire a lock, wait forever until we get it.  Pass in a request UUID so I can see who has the lock.
}

func Unlock(lock string) {
	// This must lock things globally.  Global lock server required, only for this Customer though, since "global" can be customer oriented.
	UdnLogLevel(nil, log_debug, "Unlocking: %s\n", lock)

	// Release a lock.  Should we ensure we still had it?  Can do if we gave it our request UUID
}

func ProcessSchemaUDNSet(db *sql.DB, udn_schema map[string]interface{}, udn_data_json string, udn_data map[string]interface{}) interface{} {
	UdnLogLevel(udn_schema, log_debug,"ProcessSchemaUDNSet: JSON:\n%s\n\n", udn_data_json)

	var result interface{}

	if udn_data_json != "" {
		// Extract the JSON into a list of list of lists (2), which gives our execution blocks, and UDN pairs (Source/Target)
		udn_execution_group := UdnExecutionGroup{}

		// Decode the JSON data for the widget data
		err := json.Unmarshal([]byte(udn_data_json), &udn_execution_group.Blocks)
		if err != nil {
			log.Panic(err)
		}

		// Ensure there is a Function Stack
		if udn_data["__function_stack"] == nil {
			udn_data["__function_stack"] = make([]map[string]interface{}, 0)
		}

		// Add the new stack to the stack
		new_function_stack := make(map[string]interface{})
		new_function_stack["uuid"] = ksuid.New().String()
		udn_data["__function_stack"] = append(udn_data["__function_stack"].([]map[string]interface{}), new_function_stack)

		//fmt.Printf("UDN Execution Group: %v\n\n", udn_execution_group)

		// Process all the UDN Execution blocks
		//TODO(g): Add in concurrency, right now it does it all sequentially
		for _, udn_group := range udn_execution_group.Blocks {
			for _, udn_group_block := range udn_group {
				result = ProcessUDN(db, udn_schema, udn_group_block, udn_data)
			}
		}

		// Remove the udn_data["__temp_UUID"] data, so it doesn't just pollute the udn_data space
		if udn_data["__temp"] != nil {
			delete(udn_data["__temp"].(map[string]interface{}), new_function_stack["uuid"].(string))
		}

		// Remove the latest function stack, that we just put on
		udn_data["__function_stack"] = udn_data["__function_stack"].([]map[string]interface{})[0:len(udn_data["__function_stack"].([]map[string]interface{}))-1]

	} else {
		UdnLogLevel(udn_schema, log_info,"UDN Execution Group: None\n\n")
	}

	return result
}

// Prepare UDN processing from schema specification -- Returns all the data structures we need to parse UDN properly
func PrepareSchemaUDN(db *sql.DB) map[string]interface{} {
	// Config
	sql := "SELECT * FROM udn_config ORDER BY name"

	result := Query(db, sql)

	udn_config_map := make(map[string]interface{})

	// Add base_page_widget entries to page_map, if they dont already exist
	for _, value := range result {
		//fmt.Printf("UDN Config: %s = \"%s\"\n", value.Map["name"], value.Map["sigil"])

		// Save the config value and sigil
		//udn_config_map[string(value.Map["name"].(string))] = string(value.Map["sigil"].(string))

		// Create the TextTemplateMap
		udn_config_map[string(value["name"].(string))] = string(value["sigil"].(string))
	}

	//fmt.Printf("udn_config_map: %v\n", udn_config_map)

	// Function
	sql = "SELECT * FROM udn_function ORDER BY name"

	result = Query(db, sql)

	udn_function_map := make(map[string]string)
	udn_function_id_alias_map := make(map[int64]string)
	udn_function_id_function_map := make(map[int64]string)

	// Add base_page_widget entries to page_map, if they dont already exist
	for _, value := range result {
		//fmt.Printf("UDN Function: %s = \"%s\"\n", value.Map["alias"], value.Map["function"])

		// Save the config value and sigil
		udn_function_map[string(value["alias"].(string))] = string(value["function"].(string))
		udn_function_id_alias_map[value["_id"].(int64)] = string(value["alias"].(string))
		udn_function_id_function_map[value["_id"].(int64)] = string(value["function"].(string))
	}

	//fmt.Printf("udn_function_map: %v\n", udn_function_map)
	//fmt.Printf("udn_function_id_alias_map: %v\n", udn_function_id_alias_map)
	//fmt.Printf("udn_function_id_function_map: %v\n", udn_function_id_function_map)

	// Group
	sql = "SELECT * FROM udn_group ORDER BY name"

	result = Query(db, sql)

	//udn_group_map := make(map[string]*TextTemplateMap)
	udn_group_map := make(map[string]interface{})

	// Add base_page_widget entries to page_map, if they dont already exist
	for _, value := range result {
		//fmt.Printf("UDN Group: %s = Start: \"%s\"  End: \"%s\"  Is Key Value: %v\n", value.Map["name"], value.Map["sigil"])

		udn_group_map[string(value["name"].(string))] = make(map[string]interface{})
	}

	// Load the user functions
	sql = "SELECT * FROM udn_stored_function ORDER BY name"

	result = Query(db, sql)

	//udn_group_map := make(map[string]*TextTemplateMap)
	udn_stored_function := make(map[string]interface{})

	// Add base_page_widget entries to page_map, if they dont already exist
	for _, value := range result {
		udn_stored_function[string(value["name"].(string))] = make(map[string]interface{})
	}

	//fmt.Printf("udn_group_map: %v\n", udn_group_map)

	// Pack a result map for return
	result_map := make(map[string]interface{})

	result_map["function_map"] = udn_function_map
	result_map["function_id_alias_map"] = udn_function_id_alias_map
	result_map["function_id_function_map"] = udn_function_id_function_map
	result_map["group_map"] = udn_group_map
	result_map["config_map"] = udn_config_map
	result_map["stored_function"] = udn_stored_function

	// By default, do not debug this request
	result_map["udn_debug"] = false

	// By default, logging is turned on
	result_map["allow_logging"] = true

	// Debug information, for rendering the debug output
	UdnDebugReset(result_map)

	UdnLogLevel(nil, log_debug, "=-=-=-=-= UDN Schema Created =-=-=-=-=\n")

	return result_map
}

// Pass in a UDN string to be processed - Takes function map, and UDN schema data and other things as input, as it works stand-alone from the application it supports
func ProcessUDN(db *sql.DB, udn_schema map[string]interface{}, udn_value_list []string, udn_data map[string]interface{}) interface{} {
	UdnLogLevel(udn_schema, log_debug, "\n\nProcess UDN: \n\n")

	var udn_command_value interface{} // used to track the piped input/output of UDN commands

	// Walk through each UDN string in the list - the output of one UDN string is piped onto the input of the next
	for i := 0; i < len(udn_value_list); i++ {
		UdnLogLevel(udn_schema, log_trace, "\n\nProcess UDN statement:  %s   \n\n", udn_value_list[i])
		udn_command := ParseUdnString(db, udn_schema, udn_value_list[i])

		 //UdnLogLevel(udn_schema, log_trace, "\n-------DESCRIPTION: -------\n\n%s", DescribeUdnPart(udn_command))

		UdnDebugIncrementChunk(udn_schema)
		UdnLogHtml(udn_schema, log_debug, "------- UDN: COMMAND -------\n%s\n", udn_value_list[i])
		UdnLogLevel(udn_schema, log_debug, "------- BEGIN EXECUTION: -------\n\n")

		// Execute the UDN Command
		udn_command_value = ExecuteUdn(db, udn_schema, udn_command, udn_command_value, udn_data)

		UdnLogLevel(udn_schema, log_debug, "\n------- END EXECUTION: -------\n\n")

		UdnLogLevel(udn_schema, log_debug, "------- RESULT: %v\n\n", SnippetData(udn_command_value, 1600))
		//fmt.Printf("------- RESULT: %v\n\n", JsonDump(udn_command_value))
	}

	return udn_command_value
}

func ProcessSingleUDNTarget(db *sql.DB, udn_schema map[string]interface{}, udn_value_target string, input interface{}, udn_data map[string]interface{}) interface{} {
	UdnLogLevel(udn_schema, log_debug, "\n\nProcess Single UDN: Target:  %s  Input: %s\n\n", udn_value_target, SnippetData(input, 80))

	udn_target := ParseUdnString(db, udn_schema, udn_value_target)

	target_result := ExecuteUdn(db, udn_schema, udn_target, input, udn_data)

	UdnLogLevel(udn_schema, log_debug, "-------RETURNING: TARGET: %v\n\n", SnippetData(target_result, 300))
	return target_result
}

func ProcessUdnArguments(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, input interface{}, udn_data map[string]interface{}) []interface{} {
	if udn_start.Children.Len() > 0 {
		UdnLogLevel(udn_schema, log_trace, "Processing UDN Arguments: %s [%s]  Starting: Arg Count: %d \n", udn_start.Value, udn_start.Id, udn_start.Children.Len())
	}

	// Argument list
	args := make([]interface{}, 0)

	if udn_start.PartType == part_function || udn_start.PartType == part_compound {
		// Look through the children, adding them to the args, as they are processed.
		for child := udn_start.Children.Front(); child != nil; child = child.Next() {
			arg_udn_start := child.Value.(*UdnPart)

			if arg_udn_start.PartType == part_compound {
				// In a Compound part, the NextUdnPart is the function (currently)
				//TODO(g): This could be anything in the future, but at this point it should always be a function in a compound...  As it's a sub-statement.
				if arg_udn_start.NextUdnPart != nil {
					//UdnLogLevel(udn_schema, log_trace, "-=-=-= Args Execute from Compound -=-=-=-\n")
					arg_result := ExecuteUdn(db, udn_schema, arg_udn_start.NextUdnPart, input, udn_data)
					//UdnLogLevel(udn_schema, log_trace, "-=-=-= Args Execute from Compound -=-=-=-  RESULT: %T: %v\n", arg_result, arg_result)
					//fmt.Printf("Compound Part: %s\n", DescribeUdnPart(arg_udn_start.NextUdnPart))
					//fmt.Printf("Compound Parent: %s\n", DescribeUdnPart(arg_udn_start))

					args = AppendArray(args, arg_result)
				} else {
					//UdnLogLevel(udn_schema, log_trace, "  UDN Args: Skipping: No NextUdnPart: Children: %d\n\n", arg_udn_start.Children.Len())
					//UdnLogLevel(udn_schema, log_trace, "  UDN Args: Skipping: No NextUdnPart: Value: %v\n\n", arg_udn_start.Value)
				}
			} else if arg_udn_start.PartType == part_function {
				//UdnLogLevel(udn_schema, log_trace, "-=-=-= Args Execute from Function -=-=-=-\n")
				arg_result := ExecuteUdn(db, udn_schema, arg_udn_start, input, udn_data)

				args = AppendArray(args, arg_result)
			} else if arg_udn_start.PartType == part_map {
				// Take the value as a literal (string, basically, but it can be tested and converted)

				arg_result_result := make(map[string]interface{})

				//UdnLogLevel(udn_schema, log_trace, "--Starting Map Arg--\n\n")
				// Then we populate it with data, by processing each of the keys and values
				//TODO(g): Will first assume all keys are strings.  We may want to allow these to be dynamic as well, letting them be set by UDN, but forcing to a string afterwards...
				for child := arg_udn_start.Children.Front(); child != nil; child = child.Next() {
					key := child.Value.(*UdnPart).Value

					//ORIGINAL:
					//TODO(z): Is ExecuteUdnCompound necessary for part_map instead of ExecuteUdnPart? Change if not
					udn_part_value := child.Value.(*UdnPart).Children.Front().Value.(*UdnPart)
					//udn_part_result := ExecuteUdnPart(db, udn_schema, udn_part_value, input, udn_data)
					udn_part_result := ExecuteUdnCompound(db, udn_schema, udn_part_value, input, udn_data)
					arg_result_result[key] = udn_part_result.Result

					UdnLogLevel(udn_schema, log_trace, "--  Map:  Key: %s  Value: %v (%T)--\n\n", key, udn_part_result.Result, udn_part_result.Result)
				}
				//UdnLogLevel(udn_schema, log_trace, "--Ending Map Arg--\n\n")

				args = AppendArray(args, arg_result_result)
			} else if arg_udn_start.PartType == part_list {
				// Take each list item and process it as UDN, to get the final result for this arg value
				// Populate the list
				//list_values := list.New()
				array_values := make([]interface{}, 0)

				//TODO(g): Convert to an array.  I tried it naively, and it didnt work, so it needs a little more work than just these 2 lines...
				//list_values := make([]interface{}, 0)

				// Then we populate it with data, by processing each of the keys and values
				for child := arg_udn_start.Children.Front(); child != nil; child = child.Next() {
					udn_part_value := child.Value.(*UdnPart)

					UdnLogLevel(udn_schema, log_trace, "List Arg Eval: %v\n", udn_part_value)

					udn_part_result := ExecuteUdnPart(db, udn_schema, udn_part_value, input, udn_data)
					//list_values.PushBack(udn_part_result.Result)
					array_values = AppendArray(array_values, udn_part_result.Result)
				}

				//UdnLogLevel(udn_schema, log_trace, "  UDN Argument: List: %v\n", SprintList(*list_values))

				//args = AppendArray(args, list_values)
				args = AppendArray(args, array_values)
			} else {
				args = AppendArray(args, arg_udn_start.Value)
			}
		}
	} else if udn_start.PartType == part_list {
		// Look through the children of the list and add to args
		for child := udn_start.Children.Front(); child != nil; child = child.Next() {
			udn_part_value := child.Value.(*UdnPart)

			udn_part_result := ExecuteUdnPart(db, udn_schema, udn_part_value, input, udn_data)

			args = AppendArray(args, udn_part_result.Result)
		}
	} else if udn_start.PartType == part_map {
		// Look through the children of the map and add to args
		arg_result_result := make(map[string]interface{})

		//UdnLogLevel(udn_schema, log_trace, "--Starting Map Arg--\n\n")

		for child := udn_start.Children.Front(); child != nil; child = child.Next() {
			key := child.Value.(*UdnPart).Value

			udn_part_value := child.Value.(*UdnPart).Children.Front().Value.(*UdnPart)
			//udn_part_result := ExecuteUdnPart(db, udn_schema, udn_part_value, input, udn_data)
			udn_part_result := ExecuteUdnCompound(db, udn_schema, udn_part_value, input, udn_data)
			arg_result_result[key] = udn_part_result.Result

			UdnLogLevel(udn_schema, log_trace, "--  Map:  Key: %s  Value: %v (%T)--\n\n", key, udn_part_result.Result, udn_part_result.Result)
		}
		//UdnLogLevel(udn_schema, log_trace, "--Ending Map Arg--\n\n")

		args = AppendArray(args, arg_result_result)
	} else {
		// Default would be to just add to args (ex: string/item)
		args = AppendArray(args, udn_start.Value)
	}

	// Only log if we have something to say, otherwise its just noise
	if len(args) > 0 {
		UdnLogLevel(udn_schema, log_trace, "Processing UDN Arguments: %s [%s]  Result: %s\n", udn_start.Value, udn_start.Id, SnippetData(args, 400))
	}

	return args
}

// Execute a single UDN command and return the result
//NOTE(g): This function does not return UdnPart, because we want to get direct information, so we return interface{}
func ExecuteUdn(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, input interface{}, udn_data map[string]interface{}) interface{} {
	// Process all our arguments, Executing any functions, at all depths.  Furthest depth first, to meet dependencies

	UdnLogLevel(udn_schema, log_trace, "\nExecuteUDN: %s [%s]  Args: %d  Input: %s\n", udn_start.Value, udn_start.Id, udn_start.Children.Len(), SnippetData(input, 40))

	// In case the function is nil, just pass through the input as the result.  Setting it here because we need this declared in function-scope
	var result interface{}

	// If this is a real function (not an end-block nil function)
	if UdnFunctions[udn_start.Value] != nil {
		udn_result := ExecuteUdnPart(db, udn_schema, udn_start, input, udn_data)
		result = udn_result.Result

		// If we have more to process, do it
		if udn_result.NextUdnPart != nil {
			UdnLogLevel(udn_schema, log_trace, "ExecuteUdn: Flow Control: JUMPING to NextUdnPart: %s [%s]\n", udn_result.NextUdnPart.Value, udn_result.NextUdnPart.Id)
			// Our result gave us a NextUdnPart, so we can assume they performed some execution flow control themeselves, we will continue where they told us to
			result = ExecuteUdn(db, udn_schema, udn_result.NextUdnPart, result, udn_data)
		} else if udn_start.NextUdnPart != nil {
			UdnLogLevel(udn_schema, log_trace, "ExecuteUdn: Flow Control: STEPPING to NextUdnPart: %s [%s]\n", udn_start.NextUdnPart.Value, udn_start.NextUdnPart.Id)
			// We have a NextUdnPart and we didnt recieve a different NextUdnPart from our udn_result, so execute sequentially
			result = ExecuteUdn(db, udn_schema, udn_start.NextUdnPart, result, udn_data)
		}
	} else {
		// Set the result to our input, because we got a nil-function, which doesnt change the result
		result = input
	}

	// If the UDN Result is a list, convert it to an array, as it's easier to read the output
	//TODO(g): Remove all the list.List stuff, so everything is an array.  Better.
	result_type_str := fmt.Sprintf("%T", result)
	if result_type_str == "*list.List" {
		result = GetResult(result, type_array)
	}

	UdnLogLevel(udn_schema, log_trace, "ExecuteUDN: End Function: %s [%s]: Result: %s\n\n", udn_start.Value, udn_start.Id, SnippetData(result, 40))

	// Return the result directly (interface{})
	return result
}

// Execute a single UdnPart.  This is necessary, because it may not be a function, it might be a Compound, which has a function inside it.
//		At the top level, this is not necessary, but for flow control, we need to wrap this so that each Block Executor doesnt need to duplicate logic.
//NOTE(g): This function must return a UdnPart, because it is necessary for Flow Control (__iterate, etc)
func ExecuteUdnPart(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, input interface{}, udn_data map[string]interface{}) UdnResult {
	//UdnLogLevel(udn_schema, log_trace, "Executing UDN Part: %s [%s]\n", udn_start.Value, udn_start.Id)

	// Process the arguments
	args := ProcessUdnArguments(db, udn_schema, udn_start, input, udn_data)

	UdnDebug(udn_schema, input, "View Input", fmt.Sprintf("Execute UDN Part: %s: %v", udn_start.Value, SnippetData(args, 300)))

	// Store this so we can access it if we want
	//TODO(g): Is this required?  Is it the best place for it?  Investiage at some point...  Not sure re-reading it.
	udn_data["arg"] = args

	// What we return, unified return type in UDN
	udn_result := UdnResult{}

	if udn_start.PartType == part_function {
		if UdnFunctions[udn_start.Value] != nil {
			// Execute a function
			UdnLogLevel(udn_schema, log_trace, "Executing: %s [%s]   Args: %v\n", udn_start.Value, udn_start.Id, SnippetData(args, 80))

			udn_result = UdnFunctions[udn_start.Value](db, udn_schema, udn_start, args, input, udn_data)
		} else {
			//UdnLogLevel(udn_schema, log_trace, "Skipping Execution, nil function, result = input: %s\n", udn_start.Value)
			udn_result.Result = input
		}
	} else if udn_start.PartType == part_compound {
		// Execute the first part of the Compound (should be a function, but it will get worked out)
		udn_result = ExecuteUdnPart(db, udn_schema, udn_start.NextUdnPart, input, udn_data)
	} else if udn_start.PartType == part_map {
		if len(args) > 0 {
			udn_result.Result = args[0]
		} else {
			udn_result.Result = udn_start.Value
		}
	} else if udn_start.PartType == part_list {
		udn_result.Result = args
	} else {
		// We just store the value, if it is not handled as a special case above
		udn_result.Result = udn_start.Value
	}

	//UdnLogLevel(udn_schema, log_trace, "=-=-=-=-= Executing UDN Part: End: %s [%s] Full Result: %v\n\n", udn_start.Value, udn_start.Id, udn_result.Result)	// DEBUG

	UdnDebug(udn_schema, udn_result.Result, "View Output", "")

	return udn_result
}

// Execute a UDN Compound
func ExecuteUdnCompound(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, input interface{}, udn_data map[string]interface{}) UdnResult {
	udn_result := UdnResult{}

	if udn_start.NextUdnPart != nil {
		// If this is a Compound, process it
		udn_current := udn_start.NextUdnPart

		done := false

		for !done {
			//fmt.Printf("Execute UDN Compound: %s\n", DescribeUdnPart(udn_current))
			//fmt.Printf("Execute UDN Compound: Input: %s\n", SnippetData(input, 60))

			udn_result = ExecuteUdnPart(db, udn_schema, udn_current, input, udn_data)
			input = udn_result.Result

			if udn_current.NextUdnPart == nil {
				done = true
				//fmt.Print("  UDN Compound: Finished\n")
			} else {
				udn_current = udn_current.NextUdnPart
				//fmt.Printf("  Next UDN Compound: %s\n", udn_current.Value)
			}
		}
	} else {
		udn_result = ExecuteUdnPart(db, udn_schema, udn_start, input, udn_data)
	}

	return udn_result
}
