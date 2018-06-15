package yudien

import (
	"database/sql"
	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudiendata"
	. "github.com/ghowland/yudien/yudienutil"
	"time"
	"strings"
	"strconv"
	"fmt"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"log"
	"io/ioutil"
	"bytes"
)

func UDN_Custom_PopulateScheduleDutyResponsibility(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	database := GetResult(args[0], type_string).(string)
	responsibility_id := GetResult(args[1], type_int).(int64)
	start_populating := GetResult(args[2], type_string).(string)
	business_user_id := GetResult(args[3], type_int).(int64)

	start_populating = strings.Replace(start_populating," ", "T", -1)

	start_time, err := time.Parse("2006-01-02T15:04:05", start_populating)

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: %v\n", start_time, err)


	result := UdnResult{}
	result.Result = nil

	options := make(map[string]interface{})
	options["db"] = database

	// Get the Responsibility
	responsibility := DatamanGet("duty_responsibility", int(responsibility_id), options)
	if responsibility["_error"] != nil {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting responsibility: %v\n", responsibility["_error"])
		return result
	}

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Responsibility: %v\n", responsibility)

	// Get the Duty
	duty := DatamanGet("duty", int(responsibility["duty_id"].(int64)), options)
	if duty["_error"] != nil {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting duty: %v\n", duty["_error"])
		return result
	}

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: duty: %v\n", duty)

	// Get the Roster
	roster := DatamanGet("duty_roster", int(responsibility["duty_roster_id"].(int64)), options)
	if roster["_error"] != nil {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Roster: %v\n", roster["_error"])
		return result
	}
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Roster: %v\n", roster)

	// Get the Roster Users ordered by priority
	options["sort"] = []string{"priority"}
	filter := map[string]interface{}{
		"duty_roster_id": []interface{}{"=", roster["_id"]},
	}
	roster_users := DatamanFilter("duty_roster_business_user", filter, options)
	if len(roster_users) == 0 {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Roster Users: %d\n", len(roster_users))
		return result
	}
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Roster Users: %v\n", roster_users)

	// Get the Businsess Users
	options["sort"] = nil
	filter = map[string]interface{}{
		"business_id": []interface{}{"=", duty["business_id"]},
	}
	business_users := DatamanFilter("business_user", filter, options)
	if len(business_users) == 0 {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Business Users: %d\n", len(business_users))
		return result
	}
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Business Users: %v\n", business_users)

	// Get the Duty Responsbility Shifts
	options["sort"] = nil
	filter = map[string]interface{}{
		"duty_responsibility_id": []interface{}{"=", responsibility["_id"]},
	}
	shifts := DatamanFilter("duty_responsibility_shift", filter, options)
	if responsibility["_error"] != nil {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Shifts: %d\n", len(shifts))
		return result
	}
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Shifts: %v\n", shifts)

	// Get the Timeline
	timeline := DatamanGet("schedule_timeline", int(responsibility["schedule_timeline_id"].(int64)), options)
	if timeline["_error"] != nil {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Schedule Timeline: %v\n", timeline["_error"])
		return result
	}
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Schedule Timeline: %v\n", timeline)

	// Get the Schedule Timeline Items
	options["sort"] = []string{"time_start"}
	filter = map[string]interface{}{
		"schedule_timeline_id": []interface{}{"=", responsibility["schedule_timeline_id"]},
	}
	timeline_items := DatamanFilter("schedule_timeline_item", filter, options)
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Schedule Timeline Items: %v\n", timeline_items)


	EvaluateShiftTimes(database, responsibility, shifts, start_time, business_user_id, roster_users, business_users)


	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Result: %v\n", result.Result)

	return result
}

func EvaluateShiftTimes(database string, responsibility map[string]interface{}, shifts []map[string]interface{}, start_time time.Time, business_user_id int64, roster_users []map[string]interface{}, business_users []map[string]interface{}) {
	UdnLogLevel(nil, log_debug, "Evaluate Shift Times: %v\n", shifts)

	time_layout := "2006-01-02 15:04:05"

	options := make(map[string]interface{})
	options["db"] = database

	// How long we want to populate for; when we want to stop populating
	population_duration := time.Duration(responsibility["populate_schedule_duration"].(int64)) * time.Second
	population_end_time := start_time.Add(population_duration)

	// We are going to walk forward until we have populated all we were asked to do
	cur_start_time := start_time

	// Get the current user
	cur_roster_user := FindRosterUser(business_user_id, roster_users)

	// If we have an automated adjustment of the current user for this responsibility, then get the new current user
	if responsibility["populate_shift_user_priority_offset"].(int64) != 0 {
		cur_roster_user = FindNextRosterUser(cur_roster_user["priority"].(int64) + responsibility["populate_shift_user_priority_offset"].(int64), roster_users)
	}

	for {
		for _, shift := range shifts {
			business_user := GetBusinessUser(cur_roster_user["business_user_id"].(int64), business_users)

			// Assume we will get our start/end time from the current shift, but we may actually get it from another shift
			shift_time := shift

			// If we want to get our start/end time from a different shift, then do that
			if shift["start_sync_with_duty_responsibility_shift_id"] != nil {
				shift_time = DatamanGet("duty_responsibility_shift", int(shift["start_sync_with_duty_responsibility_shift_id"].(int64)), options)
			}

			// Get our shift time from the specified shift_time (current or specified shift)
			shift_start, shift_end := GetShiftTimeStartEnd(cur_start_time, shift_time, shifts)
			UdnLogLevel(nil, log_debug, "Evaluate Shift Times: %s: %v -> %v  User: %s\n", shift["name"], shift_start, shift_end, business_user["name"])

			// Create our timeline record
			timeline_item := map[string]interface{}{
				"schedule_timeline_id": responsibility["schedule_timeline_id"],
				"time_start": shift_start.Format(time_layout),
				"time_stop": shift_end.Format(time_layout),
				"business_user_id": cur_roster_user["business_user_id"],
			}

			// Save the timeline item
			DatamanSet("schedule_timeline_item", timeline_item, options)

			// Update our current start time, to be the end of the previous shift
			cur_start_time = shift_end
			// Go to the next timeline user
			cur_roster_user = FindNextRosterUser(cur_roster_user["priority"].(int64) + shift["duty_roster_priority_increment"].(int64), roster_users)
		}

		// If we have past the time we want to populate until
		if cur_start_time.After(population_end_time) {
			break
		}
	}
}

func GetBusinessUser(business_user_id int64, business_users []map[string]interface{}) map[string]interface{} {
	var user map[string]interface{}

	for _, item := range business_users {
		if item["_id"].(int64) == business_user_id {
			user = item
		}
	}

	return user
}

func FindNextRosterUser(priority int64, roster_users []map[string]interface{}) map[string]interface{} {
	var roster_user map[string]interface{}

	if int(priority) >= len(roster_users) {
		priority = 0
	} else if int(priority) < 0 {
		priority = int64(len(roster_users) - 1)
	}

	for _, item := range roster_users {
		if item["priority"].(int64) == priority {
			roster_user = item
		}
	}

	return roster_user
}

func FindRosterUser(business_user_id int64, roster_users []map[string]interface{}) map[string]interface{} {
	var roster_user map[string]interface{}

	for _, item := range roster_users {
		if item["business_user_id"].(int64) == business_user_id {
			roster_user = item
		}
	}

	return roster_user
}

func GetShiftTimeStartEnd(start_time time.Time, shift map[string]interface{}, shifts []map[string]interface{}) (time.Time, time.Time) {
	// Find the shift start
	shift_start := start_time.AddDate(0, 0, int(shift["start_day_of_week"].(int64)) - int(start_time.Weekday()))

	start_hour, start_minute, start_second := shift_start.Clock()
	start_hour_duration := GetTimeOfDayDuration(start_hour, start_minute, start_second)
	shift_start_zero_day := shift_start.Add(-start_hour_duration)

	hour, minute, second := GetTimeOfDayFromString(shift["start_time_of_day"].(string))
	time_seconds_duration := GetTimeOfDayDuration(hour, minute, second)
	shift_start_zero := shift_start_zero_day.Add(time_seconds_duration)

	shift_end := shift_start_zero.Add(time.Duration(shift["duration"].(int64)) * time.Second)

	return shift_start_zero, shift_end
}

func GetTimeOfDayFromString(time_of_day string) (int, int, int) {
	time_parts := strings.Split(time_of_day, ":")

	hour, _ := strconv.ParseInt(time_parts[0], 10, 64)
	minute, _ := strconv.ParseInt(time_parts[1], 10, 64)
	second, _ := strconv.ParseInt(time_parts[2], 10, 64)

	return int(hour), int(minute), int(second)
}

func GetTimeOfDayDuration(hour int, minute int, second int) time.Duration {
	time_seconds := hour * 60 * 60 + minute * 60 + second
	time_seconds_duration := time.Duration(time_seconds) * time.Second

	return time_seconds_duration
}

func UDN_Custom_TaskMan_AddTask(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	database_table := GetResult(args[1], type_string).(string)
	connection_database_name := GetResult(args[2], type_string).(string)
	server_connection_table := GetResult(args[3], type_string).(string)
	server_connection_id := int(GetResult(args[4], type_int).(int64))

	//uuid := GetResult(args[5], type_string).(string)
	//executor := GetResult(args[6], type_string).(string)
	//monitor_protocol := GetResult(args[7], type_string).(string)
	//interval := GetResult(args[8], type_int).(int64)
	//monitor_url := GetResult(args[9], type_string).(string)


	tablename := GetResult(args[5], type_string).(string)

	input_data :=  GetResult(args[6], type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Input Data: %s\n", JsonDump(input_data))

	//uuid := GetResult(args[5], type_string).(string)
	//executor := GetResult(args[6], type_string).(string)
	//monitor_protocol := GetResult(args[7], type_string).(string)
	//interval := GetResult(args[8], type_int).(int64)
	//monitor_url := GetResult(args[9], type_string).(string)


	fieldname_customer_service_id := "time_store_item_id"
	fieldname_created := "created"
	fieldname_data_json := "data_json"

	result := UdnResult{}
	result.Result = nil

	options := make(map[string]interface{})
	options["db"] = internal_database_name

	// Get the Roster Users ordered by priority
	options["sort"] = []string{"priority"}
	filter := map[string]interface{}{
		"name": []interface{}{"=", connection_database_name},
	}
	connection_database_array := DatamanFilter(database_table, filter, options)
	if len(connection_database_array) == 0 {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: Error getting Connection Database: %d\n", len(connection_database_array))
		return result
	}
	connection_database := connection_database_array[0]
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: Connection Database: %v\n", connection_database)

	// Process data we need
	input_data["service_environment_namespace_id"], _ = strconv.ParseInt(input_data["service_environment_namespace_id"].(string), 10, 64)
	input_data["business_id"], _ = strconv.ParseInt(input_data["business_id"].(string), 10, 64)
	input_data["interval"], _ = strconv.ParseInt(input_data["interval"].(string), 10, 64)
	input_data["interval_ms"] = int(input_data["interval"].(int64) * 1000)
	input_data["service_id"], _ = strconv.ParseInt(input_data["service_id"].(string), 10, 64)
	input_data["service_monitor_type_id"], _ = strconv.ParseInt(input_data["service_monitor_type_id"].(string), 10, 64)


	// Get data from our input_data information
	//business := DatamanGet("business", input_data["business_id"].(int), options)
	//service := DatamanGet("service", input_data["service_id"].(int), options)
	service_environment_namespace := DatamanGet("service_environment_namespace", int(input_data["service_environment_namespace_id"].(int64)), options)
	service_monitor_type := DatamanGet("service_monitor_type", int(input_data["service_monitor_type_id"].(int64)), options)

	// Create the service monitor
	service_monitor := make(map[string]interface{})
	service_monitor["name"] = input_data["name"]
	service_monitor["service_id"] = input_data["service_id"]
	service_monitor["service_environment_namespace_id"] = input_data["service_environment_namespace_id"]
	service_monitor["service_monitor_type_id"] = input_data["service_monitor_type_id"]
	service_monitor["data_json"] = make(map[string]interface{})
	service_monitor["data_json"].(map[string]interface{})["url"] = input_data["url"]
	service_monitor["info"] = input_data["info"]
	service_monitor["interval_ms"] = input_data["interval_ms"]
	service_monitor["service_environment_namespace_id"] = input_data["service_environment_namespace_id"]

	// Insert the Monitor
	service_monitor_result := DatamanSet("service_monitor", service_monitor, options)

	// Create the time_store_item
	time_store_item := make(map[string]interface{})
	time_store_item["business_id"] = input_data["business_id"]
	time_store_item["time_store_id"] = service_environment_namespace["default_time_store_id"]
	time_store_item["shared_group_id"] = 1	// Service Monitor shared_group
	time_store_item["record_id"] = service_monitor_result["_id"]
	time_store_item["name"] = input_data["metric_name"]

	// Insert the s_e_n_m
	time_store_item_result := DatamanSet("time_store_item", time_store_item, options)


	// Create the service_environment_namespace_metric
	service_environment_namespace_metric := make(map[string]interface{})
	service_environment_namespace_metric["service_environment_namespace_id"] = service_monitor["service_environment_namespace_id"]
	service_environment_namespace_metric["name"] = input_data["metric_name"]
	service_environment_namespace_metric["time_store_item_id"] = time_store_item_result["_id"]
	service_environment_namespace_metric["service_monitor_id"] = service_monitor_result["_id"]

	// Insert the s_e_n_m
	service_environment_namespace_metric_result := DatamanSet("service_environment_namespace_metric", service_environment_namespace_metric, options)

	// Update the service_monitor, with the s_e_n_m
	service_monitor_result["service_environment_namespace_metric_id"] = service_environment_namespace_metric_result["_id"]
	_ = DatamanSet("service_monitor", service_monitor_result, options)

	// Update the time_store_item, with the s_e_n_m
	time_store_item_result["service_environment_namespace_metric_id"] = service_environment_namespace_metric_result["_id"]
	_ = DatamanSet("time_store_item", time_store_item_result, options)


	data := make(map[string]interface{})
	data["uuid"] = fmt.Sprintf("%d", service_monitor_result["_id"])
	data["executor"] = "monitor"
	executor_args := make(map[string]interface{})
	data_returner_args := make(map[string]interface{})
	data_returner_args["type"] = connection_database["database_type"]
	data_returner_args["info"] = connection_database["database_connect_string"]
	data_returner_args["tablename"] = tablename
	data_returner_args["fieldname_customer_service_id"] = fieldname_customer_service_id
	data_returner_args["fieldname_created"] = fieldname_created
	data_returner_args["fieldname_data_json"] = fieldname_data_json
	executor_args["data_returner_args"] = data_returner_args
	executor_args["interval"] = fmt.Sprintf("%ds", input_data["interval"].(int64))
	executor_args["monitor"] = service_monitor_type["name_taskman"]
	monitor_args := make(map[string]interface{})
	monitor_args["url"] = service_monitor["data_json"].(map[string]interface{})["url"]
	executor_args["monitor_args"] = monitor_args
	data["executor_args"] = executor_args

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: %s\n", JsonDump(data))

	taskman_server := DatamanGet(server_connection_table, server_connection_id, options)

	http_result := HttpsRequest(taskman_server["host"].(string), int(taskman_server["port"].(int64)), taskman_server["default_path"].(string), taskman_server["client_certificate"].(string), taskman_server["client_private_key"].(string), taskman_server["certificate_authority"].(string), JsonDump(data))

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: Result: %s\n", JsonDump(http_result))

	return result
}

func HttpsRequest(hostname string, port int, uri string, client_cert string, client_key string, certificate_authority string, data_json string) []byte {
	// Use strings, not file loading
	cert, err := tls.X509KeyPair([]byte(client_cert), []byte(client_key))
	if err != nil {
		log.Panic(err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(certificate_authority))

	// Setup HTTPS client
	//tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: caCertPool,}		// This will restrict the client to connect to a valid CA
	tlsConfig := &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}

	url := fmt.Sprintf("https://%s:%d/%s", hostname, port, uri)

	UdnLogLevel(nil, log_trace, "HttpsRequest: URL: %s\n", url)

	// Form the request
	request, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(data_json)))
	if err != nil {
		log.Panic(err)
	}
	request.Header.Add("Content-Type", "application/json")


	// Do the request
	resp, err := client.Do(request)
	if err != nil {
		log.Panic(err)
	}
	defer resp.Body.Close()

	UdnLogLevel(nil, log_trace, "HttpsRequest: %s: %d\n", resp.Status, resp.StatusCode)

	// Dump response
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panic(err)
	}

	return data
}



func UDN_Custom_Code(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	input_val := input

	internal_database_name := GetResult(args[0], type_string).(string)
	code_id := int(GetResult(args[1], type_int).(int64))
	config_map := GetResult(args[2], type_map).(map[string]interface{})

	if len(args) > 2 {
		input_val = args[2]
	}

	result := UdnResult{}
	result.Result = CodeExecute(internal_database_name, code_id, config_map, input_val, db, udn_schema, udn_data)

	return result
}

func CodeExecute(database string, code_id int, config_map map[string]interface{}, input interface{}, db *sql.DB, udn_schema map[string]interface{}, udn_data map[string]interface{}) interface{} {
	options := make(map[string]interface{})
	options["db"] = database

	code := DatamanGet("code", code_id, options)
	filter := map[string]interface{}{
		"code_id": []interface{}{"=", code_id},
	}
	options["sort"] = []string{"priority",}
	code_args := DatamanFilter("code_arg", filter, options)

	// Get the results for our args
	args := make([]interface{}, 0)

	// Config is always the first argument, so it always going to our Code Functions
	args = append(args, config_map)
	// Input is always the second argument, so it always going to our Code Functions, which dont take input, only args
	args = append(args, input)

	for _, code_arg := range code_args {
		arg_result := CodeExecute(database, int(code_arg["execute_code_id"].(int64)), config_map, code_arg["input_data_json"], db, udn_schema, udn_data)

		args = append(args, arg_result)
	}

	// Get the actual UDN we need -> code_function -> shared_udn (for now, this allows abstraction later if I want to change things at the code level, above the Shared UDN level)
	options["sort"] = nil
	code_function := DatamanGet("code_function", int(code["code_function_id"].(int64)), options)
	shared_udn := DatamanGet("shared_udn", int(code_function["shared_udn_id"].(int64)), options)

	// Set the args into the __get.function_arg array, like a Stored Function (__function), since ProcessUDN doesnt take input
	function_set_args := []interface{}{"function_arg"}
	MapSet(function_set_args, args, udn_data)

	// Execute the Shared UDN
	//TODO(g): Make this better than dumping into JSON?  Seems like a waste if we already have it in data format and just parse it again, but deal with it later
	result := ProcessSchemaUDNSet(db, udn_schema, JsonDump(shared_udn["execute_udn_data_json"]), udn_data)

	// If we have a code chain (next), then execute it and it will execute any others and pass back their results
	if code["next_code_id"] != nil {
		// Set the args into the __get.function_arg array, like a Stored Function (__function), since ProcessUDN doesnt take input
		function_set_args := []interface{}{"function_arg"}
		args := make([]interface{}, 0)
		args = append(args, input)
		MapSet(function_set_args, args, udn_data)

		result = CodeExecute(database, int(code["next_code_id"].(int64)), config_map, result, db, udn_schema, udn_data)
	}

	return result
}

func UDN_Custom_Metric_Filter(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	metric_name_array := GetResult(args[1], type_array).([]interface{})
	labelset_map := GetResult(args[2], type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Metric: Filter: %v: %v\n", metric_name_array, labelset_map)

	options := make(map[string]interface{})
	options["db"] = internal_database_name


	filter := map[string]interface{}{
		"name": []interface{}{"in", metric_name_array},
	}
	//TODO(g): May want to add a sort option that can be passed in as arg3, since we could organize these somehow.  Remove comment if not needed.
	name_filtered := DatamanFilter("service_environment_namespace_metric", filter, options)

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Metric: Filter: Name filtered array: %v\n", name_filtered)

	labelset_filtered := make([]map[string]interface{}, 0)

	for _, metric := range name_filtered {
		// Assume we match, easier to falsify as it only takes one miss
		matched_labelset := true

		for label, value_array := range labelset_map {
			UdnLogLevel(udn_schema, log_debug, "CUSTOM: Metric: Filter: Labelset: %s: %v\n", label, value_array)

			if metric["labelset_data_jsonb"] == nil {
				// Missing a labelset we wanted to test for
				matched_labelset = false

			} else if metric["labelset_data_jsonb"].(map[string]interface{})[label] != nil {
				// It only takes 1 match per key to be a success
				found_value := false

				for _, value_item := range value_array.([]interface{}) {
					if value_item == metric["labelset_data_jsonb"].(map[string]interface{})[label] {
						found_value = true
					}
				}

				if !found_value {
					// Did not match one of the labelset options
					matched_labelset = false
					break
				}
			} else {
				// Did not contain the label we had values to test against
				matched_labelset = false
				break
			}
		}

		if matched_labelset {
			// Nothing failed to match this metric, it has passed the labelset_filter
			labelset_filtered = append(labelset_filtered, metric)
		}
	}


	result := UdnResult{}
	result.Result = labelset_filtered

	return result
}


func UDN_Custom_Metric_Get_Values(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	duration_ms := GetResult(args[1], type_int).(int64)
	offset_ms := GetResult(args[2], type_int).(int64)

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Metric: Get Values: %d: %d\n", duration_ms, offset_ms)

	options := make(map[string]interface{})
	options["db"] = internal_database_name




	result := UdnResult{}
	result.Result = nil

	return result
}
