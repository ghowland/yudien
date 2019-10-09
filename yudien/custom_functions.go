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
	"net/smtp"
	"sort"
	"encoding/base64"
	"net/url"
	"crypto"
)

const (
	time_format_db = "2006-01-02 15:04:05"
	time_format_go = "2006-01-02T15:04:05"
	time_format_date = "2006-01-02"
)

func UDN_Custom_PopulateScheduleDutyResponsibility(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	database := GetResult(args[0], type_string).(string)
	responsibility_id := GetResult(args[1], type_int).(int64)
	start_populating := GetResult(args[2], type_string).(string)
	business_user_id := GetResult(args[3], type_int).(int64)

	start_populating = strings.Replace(start_populating," ", "T", -1)

	start_time, err := time.Parse(time_format_go, start_populating)

	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: %v\n", start_time, err)


	result := UdnResult{}
	result.Result = nil

	options := make(map[string]interface{})
	options["db"] = database

	// Get the Responsibility
	responsibility := DatamanGet("duty_responsibility", int(responsibility_id), options)
	if responsibility["_error"] != nil {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting responsibility: %v\n", responsibility["_error"])
		return result
	}

	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Responsibility: %v\n", responsibility)

	// Get the Duty
	duty := DatamanGet("duty", int(responsibility["duty_id"].(int64)), options)
	if duty["_error"] != nil {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting duty: %v\n", duty["_error"])
		return result
	}

	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: duty: %v\n", duty)

	// Get the Roster
	roster := DatamanGet("duty_roster", int(responsibility["duty_roster_id"].(int64)), options)
	if roster["_error"] != nil {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Roster: %v\n", roster["_error"])
		return result
	}
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Roster: %v\n", roster)

	// Get the Roster Users ordered by priority
	options["sort"] = []string{"priority"}
	filter := map[string]interface{}{
		"duty_roster_id": []interface{}{"=", roster["_id"]},
	}
	roster_users := DatamanFilter("duty_roster_business_user", filter, options)
	if len(roster_users) == 0 {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Roster Users: %d\n", len(roster_users))
		return result
	}
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Roster Users: %v\n", roster_users)

	// Get the Businsess Users
	options["sort"] = nil
	filter = map[string]interface{}{
		"business_id": []interface{}{"=", duty["business_id"]},
	}
	business_users := DatamanFilter("business_user", filter, options)
	if len(business_users) == 0 {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Business Users: %d\n", len(business_users))
		return result
	}
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Business Users: %v\n", business_users)

	// Get the Duty Responsbility Shifts
	options["sort"] = nil
	filter = map[string]interface{}{
		"duty_responsibility_id": []interface{}{"=", responsibility["_id"]},
	}
	shifts := DatamanFilter("duty_responsibility_shift", filter, options)
	if responsibility["_error"] != nil {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Shifts: %d\n", len(shifts))
		return result
	}
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Shifts: %v\n", shifts)

	// Get the Timeline
	timeline := DatamanGet("schedule_timeline", int(responsibility["schedule_timeline_id"].(int64)), options)
	if timeline["_error"] != nil {
		UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting Schedule Timeline: %v\n", timeline["_error"])
		return result
	}
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Schedule Timeline: %v\n", timeline)

	// Get the Schedule Timeline Items
	options["sort"] = []string{"time_start"}
	filter = map[string]interface{}{
		"schedule_timeline_id": []interface{}{"=", responsibility["schedule_timeline_id"]},
	}
	timeline_items := DatamanFilter("schedule_timeline_item", filter, options)
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Schedule Timeline Items: %v\n", timeline_items)


	EvaluateShiftTimes(database, responsibility, shifts, start_time, business_user_id, roster_users, business_users)


	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Populate Schedule: Duty Responsibility: Result: %v\n", result.Result)

	return result
}

func EvaluateShiftTimes(database string, responsibility map[string]interface{}, shifts []map[string]interface{}, start_time time.Time, business_user_id int64, roster_users []map[string]interface{}, business_users []map[string]interface{}) {
	UdnLogLevel(nil, log_trace, "Evaluate Shift Times: %v\n", shifts)

	time_layout := time_format_db

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
			UdnLogLevel(nil, log_trace, "Evaluate Shift Times: %s: %v -> %v  User: %s\n", shift["name"], shift_start, shift_end, business_user["name"])

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

func HttpsRequest(hostname string, port int, path string, method string, client_cert string, client_key string, certificate_authority string, data_json string) []byte {

	transport := &http.Transport{}

	// If we have a client cert, make a TLS client config
	if client_cert != "" {
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

		transport = &http.Transport{TLSClientConfig: tlsConfig}
	} else {
		// No client cert
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		tlsConfig.BuildNameToCertificate()
	}

	client := &http.Client{Transport: transport}

	url := fmt.Sprintf("https://%s:%d/%s", hostname, port, path)

	UdnLogLevel(nil, log_trace, "HttpsRequest: URL: %s: %s\n", method, url)

	// Form the request
	request, err := http.NewRequest(method, url, bytes.NewBuffer([]byte(data_json)))
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

	UdnLogLevel(nil, log_trace, "HttpsRequest: %s: %s: %d\n", method, resp.Status, resp.StatusCode)

	// Dump response
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Panic(err)
	}

	return data
}


func HttpRequest(hostname string, port int, path string, method string, data_json string, user string, password string) []byte {

	transport := &http.Transport{}

	client := &http.Client{Transport: transport}

	url := fmt.Sprintf("http://%s:%d/%s", hostname, port, path)

	UdnLogLevel(nil, log_trace, "HttpRequest: URL: %s: %s\n", method, url)

	// Form the request
	request, err := http.NewRequest(method, url, bytes.NewBuffer([]byte(data_json)))
	if err != nil {
		log.Panic(err)
	}
	request.Header.Add("Content-Type", "application/json")

	// Basic auth, is user is set
	if user != "" {
		auth_str := fmt.Sprintf("%s:%s", user, password)
		auth_base64 := base64.StdEncoding.EncodeToString([]byte(auth_str))
		//UdnLogLevel(nil, log_trace, "HttpRequest: Auth: %s:   Basic %s\n", auth_str, auth_base64)
		request.Header.Add("Authorization"," Basic " + auth_base64)
	}

	// Do the request
	resp, err := client.Do(request)
	if err != nil {
		log.Panic(err)
	}
	defer resp.Body.Close()

	UdnLogLevel(nil, log_trace, "HttpRequest: %s: %s\n", method, resp.Status)

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

func UDN_Custom_Health_Check_PromQL(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	config := GetResult(args[1], type_map).(map[string]interface{})
	api_server_connection_table := GetResult(args[2], type_string).(string)
	api_server_connection_id := GetResult(args[3], type_int).(int64)

	business := udn_data["business"].(map[string]interface{})

	HealthCheckPromQL(internal_database_name, config, api_server_connection_table, api_server_connection_id, business)

	result := UdnResult{}
	result.Result = nil

	return result
}

func HealthCheckPromQL(internal_database_name string, config map[string]interface{}, api_server_connection_table string, api_server_connection_id int64, business map[string]interface{}) {
	UdnLogLevel(nil, log_trace, "HealthCheckPromQL: Init: %s: %s: %d: %s\n", internal_database_name, api_server_connection_table, api_server_connection_id, JsonDump(config))
	UdnLogLevel(nil, log_trace, "HealthCheckPromQL: Business: %s\n", JsonDump(business))

	UdnLogLevel(nil, log_trace, "HealthCheckPromQL: Query: %s\n", config["query_promql"])

	duration_ms_str := config["health_check"].(map[string]interface{})["code_data_json"].(map[string]interface{})["duration_ms"].(string)
	duration_ms, _ := strconv.ParseInt(duration_ms_str, 10, 64)

	match_percent_str := config["health_check"].(map[string]interface{})["code_data_json"].(map[string]interface{})["match_percent"].(string)
	match_percent_int, _ := strconv.ParseInt(match_percent_str, 10, 64)
	match_percent := float64(match_percent_int) / 100.0

	invert_match := config["health_check"].(map[string]interface{})["code_data_json"].(map[string]interface{})["invert_match"].(bool)

	// Handle time range
	start := time.Now().Add(time.Millisecond * time.Duration(-duration_ms)).Unix()
	end := time.Now().Unix()
	step := 5

	// Server info for API
	options := map[string]interface{}{"db": internal_database_name}
	api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

	// Encode query_arg
	query_arg := url.Values{}
	query_arg.Add("query", config["query_promql"].(string))
	query_encoded := query_arg.Encode()

	health_check := config["health_check"].(map[string]interface{})
	health_check_code_data_json := health_check["code_data_json"].(map[string]interface{})
	business_environment_namespace_id, _ := strconv.ParseInt(health_check_code_data_json["business_environment_namespace_id"].(string), 10, 64)

	business_environment_namespace := DatamanGet("business_environment_namespace", int(business_environment_namespace_id), options)
	environment := DatamanGet("environment", int(business_environment_namespace["environment_id"].(int64)), options)

	source := "mm"
	env := environment["api_name"]
	namespace := business_environment_namespace["api_name"]

	robot_business_user := DatamanGet("business_user", int(business["api_robot_business_user_id"].(int64)), options)

	user := robot_business_user["name"].(string)
	password := robot_business_user["password_digest"].(string)


	// This is the new query
	path := fmt.Sprintf("v1/metrics/%s/%s/%s/prometheus/api/v1/query_range?%s&start=%d&end=%d&step=%d", source, env, namespace, query_encoded, start, end, step)

	UdnLogLevel(nil, log_trace, "DashboardItemEdit: API Prom Path: Business: %s\n", path)

	http_result := HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "GET", JsonDump(make(map[string]interface{})), user, password)

	api_payload, _ := JsonLoadMap(string(http_result))

	UdnLogLevel(nil, log_trace, "HealthCheckPromQL: API Result: %s\n", JsonDump(api_payload))

	payload_result_data := api_payload["data"].(map[string]interface{})
	payload_result_array := payload_result_data["result"].([]interface{})


	if len(payload_result_array) > 0 {
		for _, payload_result := range payload_result_array {
			metric_map := payload_result.(map[string]interface{})["metric"].(map[string]interface{})
			value_array := payload_result.(map[string]interface{})["values"].([]interface{})

			hasher := crypto.SHA512.New()
			labelset_map := MapCopy(metric_map)
			//TODO(g): Is this the only key I have to remove to make it the same?
			delete(labelset_map, "__name__")
			delete(labelset_map, "hostname")
			UdnLogLevel(nil, log_trace, "LabelSet Map: %v\n", labelset_map)
			hasher.Write([]byte(JsonDump(labelset_map)))
			metric_map_hash := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

			duration := end - start
			duration_steps := duration / int64(step)

			percentage_of_match := float64(len(value_array)) / float64(duration_steps)

			// If we want to invert this match
			if invert_match {
				UdnLogLevel(nil, log_trace, "DashboardItemEdit: Invert Match: %f  -- %f\n", percentage_of_match, 1.0 - percentage_of_match)

				percentage_of_match = 1.0 - percentage_of_match
			}

			if percentage_of_match > 1.0 {
				percentage_of_match = 1.0
			}

			is_match := false
			if match_percent <= percentage_of_match {
				is_match = true
			}

			UdnLogLevel(nil, log_trace, "DashboardItemEdit: service_monitor_id: %s  Duration: %v   Duration Steps: %d  Result Percent: %f  Match Percent Needed: %f  Is Match: %v\n", metric_map["service_monitor_id"], duration_ms, duration_steps, percentage_of_match, match_percent, is_match)

			// If this is a match, create an outage item (and outage if none is open for new items)
			if is_match {
				PopulateOutageItem(internal_database_name, metric_map, value_array, percentage_of_match, match_percent, invert_match, business, business_environment_namespace, health_check, metric_map_hash)
			}
		}
	} else {
		// payload_result_array is empty.  This matters for inverted matches
		if invert_match {
			//// It was 0.0, not it's 1.0, because we are inverted
			//percentage_of_match := 1.0

			UdnLogLevel(nil, log_trace, "TBD: How do we process inverted access where they all fail?  I will need to do a match on something besides values, because there are no values, so no metric_map to make a labelset from...")
		}
	}


	/*
		// This is the new data
		return_data["api_render_data"] = API_Generate_Graph_Data(internal_database_name, api_payload)
		return_data["query"] = strings.Replace(query, "\"", "&quot;", -1)
		return_data["query_env"] = env
		return_data["query_namespace"] = namespace
	*/
}


func PopulateOutageItem(internal_database_name string, metric_map map[string]interface{}, value_array []interface{}, percentage_of_match float64, match_percent float64, invert_match bool, business map[string]interface{}, business_environment_namespace map[string]interface{}, health_check map[string]interface{}, metric_map_hash string) {
	UdnLogLevel(nil, log_trace, "PopulateOutageItem: %f: %f: %s: %v\n", percentage_of_match, match_percent, metric_map_hash, metric_map)

	// Check to see if there are any open outages
	options := make(map[string]interface{})
	options["db"] = internal_database_name


	// Check to see if this alert is part of the open outages, and update them
	filter := map[string]interface{}{
		"business_id": []interface{}{"=", business["_id"]},
		"time_stop": []interface{}{"=", nil},
	}
	//TODO(g): May want to add a sort option that can be passed in as arg3, since we could organize these somehow.  Remove comment if not needed.
	outage_array := DatamanFilter("outage", filter, options)

	var outage map[string]interface{}


	filter = map[string]interface{}{
		"labelset_hash": []interface{}{"=", metric_map_hash},
		"business_environment_namespace_id": []interface{}{"=", business_environment_namespace["_id"]},		// Ensure we are on the right business, and cant get bad data
	}
	business_environment_namespace_metric_array := DatamanFilter("business_environment_namespace_metric", filter, options)
	business_environment_namespace_metric := business_environment_namespace_metric_array[0]

	// Find an outage that is open and ready to take new items, or that it already has information about this outage in it's items.
	// 		Either a new outage, or that this is an update to an existing element -- Need to turn service_monitor_id, and also the metric labelset.
	//
	// 		- Hash on the labelshet_hash


	time_now := time.Now()

	// Check to see if we have a suitable outage already open
	if len(outage_array) != 0 {
		for _, outage_record := range outage_array {
			// Check to see if this metric item is already in the outage, that will select this outage to update
			found_metric := false

			filter = map[string]interface{}{
				"outage_id": []interface{}{"=", outage_record["_id"]},
				"business_environment_namespace_metric_id": []interface{}{"=", business_environment_namespace_metric["_id"]},
			}
			outage_item_array := DatamanFilter("outage_item", filter, options)
			if len(outage_item_array) > 0 {
				found_metric = true

				outage = outage_record
				outage_item := outage_item_array[0]

				// Update the outage_item that it's still down, and save it
				outage_item["time_updated"] = time.Now()
				DatamanSet("outage_item", outage_item, options)

				UdnLogLevel(nil, log_trace, "PopulateOutageItem: Found Outage that has this Metric already: outage_item: %v\n", outage_item)

				// We are done for now.  We don't know if we need to alert, because it's not a new Outage.  Let the cron handle that
				return
			}

			// If we didnt find the metric, Check to see if this is still open to accept new metrics (learning period)
			if !found_metric {
				start, _ := time.Parse(time_format_db, outage_record["time_start"].(string))
				//TODO(g): Fix this hard coded 5 minutes.  For now, anything that happens for 5 minutes after the start will go into the same outage
				// Is Now before Start+N Minutes.
				start_plus_accept := start.Add(time.Minute * 5)
				UdnLogLevel(nil, log_trace, "PopulateOutageItem: NOW(%s) after 5MINS(%s)\n", time_now.Format(time_format_db), start_plus_accept.Format(time_format_db))
				if time_now.After(start_plus_accept) {
					outage = outage_record
					UdnLogLevel(nil, log_trace, "PopulateOutageItem: Found Outage based on time: %v\n", outage)
				} else {
					UdnLogLevel(nil, log_trace, "PopulateOutageItem: Not After: %s < %s = %v\n", time_now.Format(time_format_db), start_plus_accept.Format(time_format_db), time_now.After(start_plus_accept))
				}
			}
		}
	}

	// If we don't already have an outage, make one
	if outage == nil {
		new_outage := make(map[string]interface{})

		new_outage["name"] = fmt.Sprintf("%s: Outage: %s", health_check["name"], time.Now().Format(time_format_db))
		new_outage["business_id"] = business["_id"]
		new_outage["time_start"] = time.Now()

		// Save the new outage
		outage = DatamanSet("outage", new_outage, options)
		UdnLogLevel(nil, log_trace, "PopulateOutageItem: New Outage: %d\n", outage["_id"])
	}

	// Add this alert to the new Outage
	new_outage_item := make(map[string]interface{})
	//new_outage_item["business_id"] = health_check["business_id"]
	new_outage_item["outage_id"] = outage["_id"]
	new_outage_item["outage_item_type_id"] = 1	// Activated
	new_outage_item["health_check_id"] = health_check["_id"]
	new_outage_item["time_start"] = time.Now()
	new_outage_item["business_environment_namespace_metric_id"] = business_environment_namespace_metric["_id"]

	item_name := fmt.Sprintf("%s: %s: Failed: %0.2f%%  Required: %0.2f%%", ShortenString(health_check["name"].(string), 25), JsonDumpData(metric_map), percentage_of_match * 100.0, match_percent)
	new_outage_item["name"] = item_name

	item_info_name := fmt.Sprintf("%s: Failed: %0.2f%%  Required: %0.2f%%", ShortenString(health_check["name"].(string), 25), percentage_of_match * 100.0, match_percent)
	item_info := fmt.Sprintf("%s\n\n%s:\n\n%s", item_info_name, time_now.Format(time_format_db), JsonDumpData(metric_map))
	new_outage_item["info"] = item_info

	outage_item := DatamanInsert("outage_item", new_outage_item, options)

	// Outage Alert...  Starting
	OutageAlert(internal_database_name, outage, outage_item, 1, health_check["escalation_policy_id"])

	UdnLogLevel(nil, log_trace, "PopulateOutageItem: Service Outage Item: %v\n", outage_item)
}

/*
func UDN_Custom_Metric_Filter(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	metric_name_array := GetResult(args[1], type_array).([]interface{})
	labelset_map := GetResult(args[2], type_map).(map[string]interface{})

	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Filter: %v: %v\n", metric_name_array, labelset_map)

	options := make(map[string]interface{})
	options["db"] = internal_database_name


	filter := map[string]interface{}{
		//"name": []interface{}{"in", metric_name_array},		// Name is in the labelset now as __name__
	}
	//TODO(g): May want to add a sort option that can be passed in as arg3, since we could organize these somehow.  Remove comment if not needed.
	name_filtered := DatamanFilter("business_environment_namespace_metric", filter, options)

	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Filter: Name filtered array: %v\n", name_filtered)

	labelset_filtered := make([]map[string]interface{}, 0)

	for _, metric := range name_filtered {
		// Assume we match, easier to falsify as it only takes one miss
		matched_labelset := true

		for label, value_array := range labelset_map {
			UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Filter: Labelset: %s: %v\n", label, value_array)

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



	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Filter: Result: %s\n", JsonDump(labelset_filtered))

	result := UdnResult{}
	result.Result = labelset_filtered
	
	return result
}

func UDN_Custom_Metric_Get_Values(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	duration_ms := GetResult(args[1], type_int).(int64)
	offset_ms := GetResult(args[2], type_int).(int64)

	time_store_values := MetricGetValues(internal_database_name, duration_ms, offset_ms, input)

	result := UdnResult{}
	result.Result = time_store_values

	return result
}

func MetricGetValues(internal_database_name string, duration_ms int64, offset_ms int64, input interface{}) map[int64]interface{} {
	UdnLogLevel(nil, log_trace, "MetricGetValues: %d: %d\n", duration_ms, offset_ms)

	options := make(map[string]interface{})
	options["db"] = internal_database_name

	time_store_values := make(map[int64]interface{})

	for _, metric := range input.([]map[string]interface{}) {
		filter := map[string]interface{}{
			"time_store_item_id": []interface{}{"=", metric["time_store_item_id"]},
			"created": []interface{}{">", time.Now().Add(-time.Millisecond * time.Duration(duration_ms))},
		}
		//TODO(g): Will have to do N queries for all the different tables the data is in
		metric_values := DatamanFilter("time_store_partition_timestorepartitionid", filter, options)

		time_store_values[metric["time_store_item_id"].(int64)] = metric_values
	}

	return time_store_values
}

func UDN_Custom_Metric_Rule_Match_Percent(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	rules := GetResult(args[1], type_array).([]interface{})

	result_map := MetricRuleMatchPercent(internal_database_name, rules, input.(map[int64]interface{}))

	result := UdnResult{}
	result.Result = result_map

	return result
}

func MetricRuleMatchPercent(internal_database_name string, rules []interface{}, input map[int64]interface{}) map[int64]float64 {
	UdnLogLevel(nil, log_trace, "MetricRuleMatchPercent: %v\n", rules)

	options := make(map[string]interface{})
	options["db"] = internal_database_name

	input_val := input

	result_map := make(map[int64]float64)

	for time_store_item_id, datapoints := range input_val {
		input_count := len(datapoints.([]map[string]interface{}))
		input_matched := 0

		for _, datapoint := range datapoints.([]map[string]interface{}) {
			//matched_all_rules := true

			//UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Rule Match Percent: %d: %v\n", time_store_item_id, datapoint)

			matched_all_rules := MetricMatchRules(datapoint, rules)

			if matched_all_rules {
				input_matched++
			}

		}

		UdnLogLevel(nil, log_trace, "MetricRuleMatchPercent: Matched: %d of %d\n", input_matched, input_count)

		input_percent := float64(input_matched) / float64(input_count)

		result_map[time_store_item_id] = input_percent
	}

	return result_map
}

func MetricMatchRules(data map[string]interface{}, rules []interface{}) bool {
	// Start true and falsify this
	matched_all_rules := true

	for _, rule := range rules {
		rule_wrapper := rule.(map[string]interface{})
		//UdnLogLevel(nil, log_trace, "Metric Match Rules: Wrapper: %v\n", rule_wrapper)

		for field, rule_map := range rule_wrapper {
			//UdnLogLevel(nil, log_trace, "Metric Match Rules: %s: %v\n", field, rule_map)

			for term, value := range rule_map.(map[string]interface{}) {
				is_match := MetricMatchRuleTerm(data["data_json"].(map[string]interface{}), field, term, value)

				UdnLogLevel(nil, log_trace, "Metric Match Rules: %s: %v:  Matched: %v\n", field, rule_map, is_match)


				if !is_match {
					matched_all_rules = false
					break
				}
			}
		}

	}

	return matched_all_rules
}

func MetricMatchRuleTerm(data map[string]interface{}, field string, term string, value interface{}) bool {
	//UdnLogLevel(nil, log_trace, "Metric Match Rule Term: Data: %s\n", JsonDump(data))

	// Get the field value from the data
	field_args := SimpleDottedStringToArray(field, ".")
	field_value := MapGet(field_args, data)
	//UdnLogLevel(nil, log_trace, "Metric Match Rule Term: Field Args: %v: %s\n", field_args, field_value)

	// Uppercase term so we have consistent values
	term = strings.ToUpper(term)

	//UdnLogLevel(nil, log_trace, "Metric Match Rule Term: %s: %v: %s: %v\n", field, field_value, term, value)

	if term == "IN" {
		found_term := false

		value_array := strings.Split(value.(string), ",")

		for _, item := range value_array {
			field_value_str := GetResult(field_value, type_string).(string)
			item_str := GetResult(item, type_string).(string)

			UdnLogLevel(nil, log_trace, "Metric Match Rule Term: %s: %s == %s\n", field, field_value_str, item_str)

			if item_str == field_value_str {
				found_term = true
				break
			}
		}

		if found_term {
			return true
		}
	} else if term == "<" {
		field_num := GetResult(field_value, type_float).(float64)
		value_num := GetResult(value, type_float).(float64)

		UdnLogLevel(nil, log_trace, "Metric Match Rule Term: %s: %f < %f\n", field, field_num, value_num)

		if field_num < value_num {
			return true
		} else {
			return false
		}
	}

	return false
}
*/

/*
func UDN_Custom_Metric_Handle_Outage(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	var input_val map[int64]float64

	if input != nil {
		input_val = input.(map[int64]float64)
	}

	internal_database_name := GetResult(args[0], type_string).(string)
	config := GetResult(args[1], type_map).(map[string]interface{})

	// Take input as 3rd argument, if present
	if len(args) > 2 {
		input_val = args[2].(map[int64]float64)
	}

	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Handle Outage: Config: %s\n", JsonDump(config))
	UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Handle Outage: Input: %s\n", JsonDump(input_val))

	options := make(map[string]interface{})
	options["db"] = internal_database_name

	alert_threshold := GetResult(config["alert_threshold"], type_float).(float64)

	for time_store_item_id, value := range input_val {
		// If this TS value is less than our alert threshold, then alert!
		if value < alert_threshold {
			UdnLogLevel(udn_schema, log_trace, "CUSTOM: Metric: Handle Outage: Alert: %d: %f < %f\n", time_store_item_id, value, alert_threshold)

			if config["health_check"] != nil {
				MetricPopulateOutage(internal_database_name, config, time_store_item_id, value, alert_threshold)
			} else {
				UdnLogLevel(udn_schema, log_trace, "WARNNG: Metric: Handle Outage: Cant Populate Outage, because Health Check data is missing from config: health_check == nil\n")
			}
		}
	}


	result := UdnResult{}
	result.Result = nil

	return result
}

func MetricPopulateOutage(internal_database_name string, config map[string]interface{}, time_store_item_id int64, value float64, alert_threshold float64) {
	health_check := config["health_check"].(map[string]interface{})

	UdnLogLevel(nil, log_trace, "CUSTOM: Metric: Populate Outage: %d: %f: %s\n", time_store_item_id, value, health_check["name"])

	// Check to see if there are any open outages
	options := make(map[string]interface{})
	options["db"] = internal_database_name


	// Check to see if this alert is part of the open outages, and update them
	filter := map[string]interface{}{
		"business_id": []interface{}{"=", health_check["business_id"]},
	}
	//TODO(g): May want to add a sort option that can be passed in as arg3, since we could organize these somehow.  Remove comment if not needed.
	outage_array := DatamanFilter("outage", filter, options)

	var outage map[string]interface{}

	// If there are no open outages, create a new Outage
	if len(outage_array) == 0 {
		// Get the default service
		filter := map[string]interface{}{
			"is_default": []interface{}{"=", true},
		}
		options["sort"] = []string{"_id"}		// Always in the same order, so we have a consistent default
		business_service_array := DatamanFilter("business_service", filter, options)
		options["sort"] = nil

		var service_id int64

		if len(business_service_array) > 0 {
			service_id = business_service_array[0]["service_id"].(int64)
		}

		new_outage := make(map[string]interface{})

		new_outage["business_id"] = health_check["business_id"]
		new_outage["service_id"] = service_id
		new_outage["time_start"] = time.Now()

		// Save the new outage
		outage := DatamanSet("outage", new_outage, options)
		UdnLogLevel(nil, log_trace, "CUSTOM: Metric: New Outage: %d\n", outage["_id"])
	} else {
		outage = outage_array[0]
	}

	// Get the time_store_item for this Health Check value
	time_store_item := DatamanGet("time_store_item", int(time_store_item_id), options)

	// Add this alert to the new Outage
	new_outage_item := make(map[string]interface{})
	//new_outage_item["business_id"] = health_check["business_id"]
	new_outage_item["outage_id"] = outage["_id"]
	new_outage_item["outage_item_type_id"] = 1	// Activated
	new_outage_item["health_check_id"] = health_check["_id"]
	new_outage_item["time_start"] = time.Now()
	new_outage_item["business_environment_namespace_metric_id"] = time_store_item["business_environment_namespace_metric_id"]
	new_outage_item["name"] = fmt.Sprintf("%s: Failed: %f%%  Required: %f%%", health_check["name"], value, alert_threshold)

	outage_item := DatamanInsert("outage_item", new_outage_item, options)

	// Outage Alert...  Starting
	OutageAlert(internal_database_name, outage, 1, health_check["escalation_policy_id"])

	UdnLogLevel(nil, log_trace, "CUSTOM: Metric: Populate Outage: Service Outage Item: %v\n", outage_item)

	// Kick off the Escalation Policy to determine if it's time to Alert

}

func UDN_Custom_Metric_Process_Open_Outages(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	ProcessOpenOutages(internal_database_name)

	result := UdnResult{}
	result.Result = nil

	return result
}

func ProcessOpenOutages(internal_database_name string) {
	// Check to see if there are any open outages
	options := make(map[string]interface{})
	options["db"] = internal_database_name


	//TODO(g): Check to see if we need to alert again, or we can close the outage, or if we are flapping, etc.  This is the state handler.

	// All activated (time_stop==NULL) outage_items, check to see if they have healed, and deal with that
	//TODO(g)...


	filter := map[string]interface{}{
		"time_stop": []interface{}{"=", nil},
	}
	options["sort"] = []string{"_id"}		// Always in the same order, so we have a consistent default
	outage_item_array := DatamanFilter("outage_item", filter, options)
	options["sort"] = nil

	// Call GetMetricValues from our open outage_item array, to test if they have changed
	for _, outage_item := range outage_item_array {
		health_check := DatamanGet("health_check", int(outage_item["helath_check_id"].(int64)), options)
		businsess_environment_namespace := DatamanGet("businsess_environment_namespace", int(outage_item["business_environment_namespace_metric_id"].(int64)), options)

		businsess_environment_namespace_array := []map[string]interface{}{businsess_environment_namespace}
		time_store_values := MetricGetValues(internal_database_name, health_check["duration_ms"].(int64), health_check["offset_ms"].(int64), businsess_environment_namespace_array)

		// Get the percentage
		rules := health_check["code_data_json"].(map[string]interface{})["rules"].([]interface{})
		match_percentage_map := MetricRuleMatchPercent(internal_database_name, rules, time_store_values)

		health_check_percentage := match_percentage_map[businsess_environment_namespace["time_store_item_id"].(int64)]

		alert_threshold := GetResult(health_check["code_data_json"].(map[string]interface{})["alert_threshold"], type_float).(float64)

		// If this Health Check is no longer failing...
		if health_check_percentage < alert_threshold {
			// Heal this outage_item and store it
			outage_item["time_stop"] = time.Now()

			_ = DatamanSet("outage_item", outage_item, options)
		}
	}

	// Get all the open Service Outages
	filter = map[string]interface{}{
		"time_stop": []interface{}{"=", nil},
	}
	outage_array := DatamanFilter("outage", filter, options)

	// Check all the outages to see if they are still open, or are now closed
	for _, outage := range outage_array {
		outage_item_array := DatamanFilter("outage_item", filter, options)

		// If all of them have been closed, then close this Outage
		if len(outage_item_array) == 0 {
			outage["time_stop"] = time.Now()

			outage_result := DatamanSet("outage", outage, options)

			// Outage Alert...  Stopping
			OutageAlert(internal_database_name, outage_result, 3, nil)
		}
	}

}
*/

func OutageAlert(internal_database_name string, outage map[string]interface{}, outage_item map[string]interface{}, outage_alert_notication_type int64, escalation_policy_id interface{}) {
	// Check to see if there are any open outages
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	//TODO(g): Make a decision making system here.  For now, I am just doing the simple "make alert when told" thing.

	filter := map[string]interface{}{
		"outage_id": []interface{}{"=", outage["_id"]},
		"time_stop": []interface{}{"=", nil},
	}
	options["sort"] = []string{"time_start"}		// Always in the same order, so we have a consistent default
	outage_item_array := DatamanFilter("outage_item", filter, options)
	options["sort"] = nil

	// If outage_alert_notication_type==1 Create an Alert and Mark it for Send
	if outage_alert_notication_type == 1 {
		outage_name := "Unknown"

		// Find first outage item that isnt closed
		for _, outage_item := range outage_item_array {
			if outage_item["time_stop"] == nil && outage_item["name"] != nil {
				outage_name = outage_item["name"].(string)
				break
			}
		}

		new_alert := make(map[string]interface{})
		new_alert["business_id"] = outage["business_id"]
		new_alert["name"] = fmt.Sprintf("Outage: %s", outage_name)

		//TODO(g): I only set this once, and on the first Health Check.  We could have a NON-URGENT be first, then later URGENTs are discovered, so need to do more data validation around this
		new_alert["escalation_policy_id"] = escalation_policy_id.(int64)

		alert := DatamanInsert("alert", new_alert, options)

		new_alert_notification := make(map[string]interface{})
		new_alert_notification["alert_id"] = alert["_id"]
		new_alert_notification["business_id"] = outage["business_id"]
		new_alert_notification["shared_group_id"] = 7 // Outage
		new_alert_notification["record_id"] = outage["_id"]
		new_alert_notification["alert_notification_type_id"] = outage_alert_notication_type
		new_alert_notification["content_subject"] = fmt.Sprintf("Outage: %s", outage_name)
		new_alert_notification["content_body"] = fmt.Sprintf("Outage Created Body: %s", outage_name)
		new_alert_notification["created"] = time.Now()

		//TODO(g): Get this from the Escalation Policy Method
		new_alert_notification["alert_notification_method_id"] = 1 // Email

		escalation_policy_item_id, escalation_policy_item_info := GetAlertEscalationPolicyItemIdAndInfo(internal_database_name, alert)
		if escalation_policy_item_id == -1 {
			UdnLogLevel(nil, log_error, "OutageAlert: ERROR: No Escalation Policy found for Alert: Service Outage: %v -- Alert: %v\n", outage, alert)
			return
		}

		new_alert_notification["escalation_policy_item_id"] = escalation_policy_item_id
		new_alert_notification["escalation_policy_item_info"] = escalation_policy_item_info
		new_alert_notification["business_user_contact_id"] = GetEscalationPolicyUserContactId(internal_database_name, alert["escalation_policy_id"].(int64), time.Now())

		_ = DatamanInsert("alert_notification", new_alert_notification, options)

		// Make the outage_alert record
		new_outage_alert := make(map[string]interface{})
		new_outage_alert["outage_id"] = outage["_id"]
		new_outage_alert["alert_id"] = alert["_id"]

		_ = DatamanInsert("outage_alert", new_outage_alert, options)

	} else if outage_alert_notication_type == 3 {
		outage_name := "Unknown"

		filter := map[string]interface{}{
			"outage_id": []interface{}{"=", outage["_id"]},
		}
		options["sort"] = []string{"time_stop"}		// Always in the same order, so we have a consistent default
		outage_item_array := DatamanFilter("outage_item", filter, options)
		options["sort"] = nil

		// Find first outage item that isnt closed
		for _, outage_item := range outage_item_array {
			if outage_item["name"] != nil {
				outage_name = outage_item["name"].(string)
				break
			}
		}

		var alert map[string]interface{}

		// Find the alert, by getting the outage_alerts
		filter = map[string]interface{}{
			"outage_id": []interface{}{"=", outage["_id"]},
		}
		outage_alert_array := DatamanFilter("outage_alert", filter, options)
		if len(outage_alert_array) != 0 {
			alert_notification_id := outage_alert_array[0]["alert_notification_id"].(int64)

			alert_notification := DatamanGet("alert_notification", int(alert_notification_id), options)
			alert = DatamanGet("alert", int(alert_notification["alert_id"].(int64)), options)

		}

		if alert == nil {
			UdnLogLevel(nil, log_error, "OutageAlert: ERROR: No alert found: Service Outage %v\n", outage)
			return
		}

		new_alert_notification := make(map[string]interface{})
		new_alert_notification["alert_id"] = alert["_id"]
		new_alert_notification["business_id"] = outage["business_id"]
		new_alert_notification["shared_group_id"] = 7 // Outage
		new_alert_notification["record_id"] = outage["_id"]
		new_alert_notification["alert_notification_type_id"] = outage_alert_notication_type
		new_alert_notification["content_subject"] = fmt.Sprintf("Outage Started: %s", outage_name)
		new_alert_notification["content_body"] = fmt.Sprintf("Outage Created:\n\n%s", outage_item["info"])
		new_alert_notification["created"] = time.Now()

		//TODO(g): Get this from the Escalation Policy Method
		new_alert_notification["alert_notification_method_id"] = 1 // Email

		escalation_policy_item_id, escalation_policy_item_info := GetAlertEscalationPolicyItemIdAndInfo(internal_database_name, alert)
		if escalation_policy_item_id == -1 {
			UdnLogLevel(nil, log_error, "OutageAlert: ERROR: No Escalation Policy found for Alert: Service Outage: %v -- Alert: %v\n", outage, alert)
			return
		}

		new_alert_notification["escalation_policy_item_id"] = escalation_policy_item_id
		new_alert_notification["escalation_policy_item_info"] = escalation_policy_item_info
		new_alert_notification["business_user_contact_id"] = GetEscalationPolicyUserContactId(internal_database_name, alert["escalation_policy_id"].(int64), time.Now())

		alert_notification := DatamanInsert("alert_notification", new_alert_notification, options)

		// Make the outage_alert record
		new_outage_alert := make(map[string]interface{})
		new_outage_alert["outage_id"] = outage["_id"]
		new_outage_alert["alert_notification_id"] = alert_notification["_id"]

		_ = DatamanInsert("outage_alert", new_outage_alert, options)
	}

}

func GetEscalationPolicyUserContactId(internal_database_name string, escalation_policy_item_id int64, at_time time.Time) int64 {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	var business_user_contact_id int64

	escalation_policy_item := DatamanGet("escalation_policy_item", int(escalation_policy_item_id), options)

	duty_responsibility_shift := DatamanGet("duty_responsibility_shift", int(escalation_policy_item["duty_responsibility_shift_id"].(int64)), options)

	duty_responsibility := DatamanGet("duty_responsibility", int(duty_responsibility_shift["duty_responsibility_id"].(int64)), options)


	filter := map[string]interface{}{
		"schedule_timeline_id": []interface{}{"=", duty_responsibility["schedule_timeline_id"]},
		"time_start": []interface{}{"<", at_time},
		"time_stop": []interface{}{">", at_time},
	}
	schedule_timeline_item_array := DatamanFilter("schedule_timeline_item", filter, options)

	UdnLogLevel(nil, log_trace, "GetEscalationPolicyUserContactId: Found Schedule Timeline Items: %v\n", schedule_timeline_item_array)

	if len(schedule_timeline_item_array) > 0 {
		schedule_timeline_item := schedule_timeline_item_array[0]

		business_user_id := schedule_timeline_item["business_user_id"].(int64)

		filter := map[string]interface{}{
			"business_user_id": []interface{}{"=", business_user_id},
		}
		business_user_contact_array := DatamanFilter("business_user_contact", filter, options)

		//TODO(g): Do a selection here, for now Im just taking the first one and assuming it's fine
		business_user_contact := business_user_contact_array[0]

		// Set the business_user_contact_id
		business_user_contact_id = business_user_contact["_id"].(int64)
	} else {
		UdnLogLevel(nil, log_error, "GetEscalationPolicyUserContactId: Failed to find Schedule Timeline Item for Escalation Policy Item: %v\n", escalation_policy_item)
		business_user_contact_id = -1
	}


	return business_user_contact_id
}

func GetAlertEscalationPolicyItemIdAndInfo(internal_database_name string, alert map[string]interface{}) (int64, string) {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	//TODO(g): Make a decision making system here.  For now, I am just doing the simple "make alert when told" thing.

	// Find the first Escalation Policy (parent_id==NULL) for this Alert.  This is where we start.
	filter := map[string]interface{}{
		"escalation_policy_id": []interface{}{"=", alert["escalation_policy_id"]},
		"parent_id": []interface{}{"=", nil},
	}
	escalation_policy_item_array := DatamanFilter("escalation_policy_item", filter, options)

	if len(escalation_policy_item_array) > 0 {
		escalation_policy_item := escalation_policy_item_array[0]

		return escalation_policy_item["_id"].(int64), escalation_policy_item["name"].(string)
	} else {
		UdnLogLevel(nil, log_error, "GetAlertEscalationPolicyItemIdAndInfo: Failed to find Escalation Policy Item Parent for Alert: %v\n", alert)

		return -1, "Error"
	}
}

func UDN_Custom_Metric_Process_Alert_Notifications(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	ProcessAlertNotifications(internal_database_name)

	result := UdnResult{}
	result.Result = nil

	return result
}

func ProcessAlertNotifications(internal_database_name string) {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	UdnLogLevel(nil, log_trace, "ProcessAlertNotifications\n")

	// Find alert notifications that havent been sent yet
	filter := map[string]interface{}{
		"time_sent": []interface{}{"=", nil},
	}
	alert_notification_array := DatamanFilter("alert_notification", filter, options)

	for _, alert_notification := range alert_notification_array {
		SendAlert(internal_database_name, alert_notification)
	}
}

func SendAlert(internal_database_name string, alert_notification map[string]interface{}) {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	business_user_contact := DatamanGet("business_user_contact", int(alert_notification["business_user_contact_id"].(int64)), options)
	business_user := DatamanGet("business_user", int(business_user_contact["business_user_id"].(int64)), options)

	to_str := fmt.Sprintf("%s <%s>", business_user["name"], business_user_contact["value"])

	from_str := "geoff@gmail.com"

	body := fmt.Sprintf("Subject: %s\n\n%s\n\n", alert_notification["content_subject"].(string), alert_notification["content_body"].(string))

	err := smtp.SendMail(
		"localhost:25",
		nil,
		from_str,
		[]string{to_str},
		[]byte(body),
	)
	if err != nil {
		UdnLogLevel(nil, log_error, "SendAlert: %s\n", err)
	}


	/*
	c, err := smtp.Dial("localhost:25")
	if err != nil {
		UdnLogLevel(nil, log_error, "SendAlert: %s\n", err)
	}
	defer c.Close()

	// Set the sender and recipient.
	c.Mail(from_str)
	c.Rcpt(to_str)

	// Send the email body.
	wc, err := c.Data()
	if err != nil {
		UdnLogLevel(nil, log_error, "SendAlert: %s\n", err)
	}
	defer wc.Close()

	buf := bytes.NewBufferString(alert_notification["content_body"].(string))
	if _, err = buf.WriteTo(wc); err != nil {
		UdnLogLevel(nil, log_error, "SendAlert: %s\n", err)
	}

	*/
}

func UDN_Custom_Metric_Escalation_Policy_Oncall(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	escalation_policy_id:= GetResult(args[1], type_int).(int64)

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	data := EscalationPolicyGetOncall(internal_database_name, escalation_policy_id, time.Now())

	result := UdnResult{}
	result.Result = data

	return result
}

func EscalationPolicyGetOncall(internal_database_name string, escalation_policy_id int64, at_time time.Time) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	data := GetEscalationPolicyInfo(internal_database_name, escalation_policy_id, at_time)

	return data
}

func GetEscalationPolicyInfo(internal_database_name string, escalation_policy_id int64, at_time time.Time) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	filter := map[string]interface{}{
		"escalation_policy_id": []interface{}{"=", escalation_policy_id},
	}
	options["sort"] = []string{"format_priority"}		// Always in the same order, so we have a consistent default
	escalation_policy_item_array := DatamanFilter("escalation_policy_item", filter, options)
	options["sort"] = nil

	// Make our return map data
	data := make(map[string]interface{})

	oncall_users := ""

	for _, escalation_policy_item := range escalation_policy_item_array {
		item := GetEscalationPolicyItemInfo(internal_database_name, escalation_policy_item["_id"].(int64), at_time)

		UdnLogLevel(nil, log_trace, "GetEscalationPolicyInfo: %d: %v\n", escalation_policy_item["_id"], item)

		if item["skip"] == nil {
			if oncall_users != "" {
				oncall_users += ", "
			}
			oncall_users += item["oncall_user"].(string)

			data["team"] = item["oncall_user_team"]
		}
	}

	// Set final data values
	data["oncall_users"] = oncall_users

	UdnLogLevel(nil, log_trace, "GetEscalationPolicyInfo: Result: %v\n", data)

	return data
}

func GetEscalationPolicyItemInfo(internal_database_name string, escalation_policy_item_id int64, at_time time.Time) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	// Make our return map data
	data := make(map[string]interface{})

	escalation_policy_item := DatamanGet("escalation_policy_item", int(escalation_policy_item_id), options)

	// If we dont have a Duty Responsibility Shift, dont show this
	if escalation_policy_item["duty_responsibility_shift_id"] == nil {
		data["skip"] = true
		return data
	}

	duty_responsibility_shift := DatamanGet("duty_responsibility_shift", int(escalation_policy_item["duty_responsibility_shift_id"].(int64)), options)

	duty_responsibility := DatamanGet("duty_responsibility", int(duty_responsibility_shift["duty_responsibility_id"].(int64)), options)

	var business_user_contact_id int64

	filter := map[string]interface{}{
		"schedule_timeline_id": []interface{}{"=", duty_responsibility["schedule_timeline_id"]},
		"time_start": []interface{}{"<", at_time},
		"time_stop": []interface{}{">", at_time},
	}
	schedule_timeline_item_array := DatamanFilter("schedule_timeline_item", filter, options)

	UdnLogLevel(nil, log_trace, "GetEscalationPolicyUserContactId: Found Schedule Timeline Items: %v\n", schedule_timeline_item_array)

	if len(schedule_timeline_item_array) > 0 {
		schedule_timeline_item := schedule_timeline_item_array[0]

		business_user_id := schedule_timeline_item["business_user_id"].(int64)

		filter := map[string]interface{}{
			"business_user_id": []interface{}{"=", business_user_id},
		}
		business_user_contact_array := DatamanFilter("business_user_contact", filter, options)

		//TODO(g): Do a selection here, for now Im just taking the first one and assuming it's fine
		business_user_contact := business_user_contact_array[0]

		// Set the business_user_contact_id
		business_user_contact_id = business_user_contact["_id"].(int64)
	} else {
		UdnLogLevel(nil, log_error, "GetEscalationPolicyUserContactId: Failed to find Schedule Timeline Item for Escalation Policy Item: %v\n", escalation_policy_item)
		business_user_contact_id = -1
	}

	//TODO(g): This will break if user_contact == -1...
	business_user := DatamanGet("business_user", int(business_user_contact_id), options)
	business_team := DatamanGet("business_team", int(business_user["business_team_id"].(int64)), options)

	// Set final return values
	data["oncall_user"] = fmt.Sprintf("%s %s.", business_user["name_first"], business_user["name_last"].(string)[0:1])
	data["oncall_user_team"] = business_team["name"]

	return data
}

func UDN_Custom_Monitor_Post_Process_Change(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	ts_database_table := GetResult(args[1], type_string).(string)
	ts_connection_database_name := GetResult(args[2], type_string).(string)
	api_server_connection_table := GetResult(args[3], type_string).(string)
	api_server_connection_id := GetResult(args[4], type_int).(int64)
	ts_tablename := GetResult(args[5], type_string).(string)


	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	error_map := MonitorPostProcessChange(internal_database_name, ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)

	result := UdnResult{}
	result.Result = error_map

	return result
}

func MonitorPostProcessChange(internal_database_name string, ts_database_table string, ts_connection_database_name string, ts_tablename string, api_server_connection_table string, api_server_connection_id int64) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	// Find Monitors that dont have metrics, create the metrics and add to Api
	filter := make(map[string]interface{})
	filter["business_environment_namespace_metric_id"] = []interface{}{"=", nil}
	monitor_list := DatamanFilter("service_monitor", filter, options)

	UdnLogLevel(nil, log_trace, "MonitorPostProcessChange: %d\n", len(monitor_list))

	for _, monitor := range monitor_list {
		if monitor["business_environment_namespace_metric_id"] == nil {
			UdnLogLevel(nil, log_trace, "MonitorPostProcessChange: %v\n", monitor)

			business_environment_namespace := DatamanGet("business_environment_namespace", int(monitor["business_environment_namespace_id"].(int64)), options)

			time_store := DatamanGet("time_store", int(business_environment_namespace["default_time_store_id"].(int64)), options)

			// Create the time_store_item for this metric
			new_time_store_item := map[string]interface{}{
				"business_id": monitor["business_id"],
				"time_store_id": time_store["_id"],
				"shared_group_id": 1,
				"record_id": monitor["_id"],
				"name": monitor["name"],
				//"business_environment_namespace_metric_id": nil,
			}

			time_store_item := DatamanInsert("time_store_item", new_time_store_item, options)

			// Create the business_environment_namespace_metric record
			new_business_environment_namespace_metric := map[string]interface{}{
				"business_environment_namespace_id": monitor["business_environment_namespace_id"],
				"name": monitor["name"],
				"service_monitor_id": monitor["_id"],
				"time_store_item_id": time_store_item["_id"],
			}

			business_environment_namespace_metric := DatamanInsert("business_environment_namespace_metric", new_business_environment_namespace_metric, options)

			// Update the monitor with the b_e_n metric
			monitor["business_environment_namespace_metric_id"] = business_environment_namespace_metric["_id"]
			DatamanSet("service_monitor", monitor, options)

			// Update the time_store_item with the b_e_n metric
			time_store_item["business_environment_namespace_metric_id"] = business_environment_namespace_metric["_id"]
			DatamanSet("time_store_item", time_store_item, options)

			// Add the Monitor to Api
			Api_AddTask(internal_database_name, monitor["_id"].(int64), ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)
		}
	}

	// Go through all our monitors and update any that have changed, add any that are missing, remove (STOP) any that we dont know about
	MonitorUpdateAll(internal_database_name, ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)


	// If we have errors, put them back in with field_label dotted keys, so we can re-render them in the form
	error_map := make(map[string]interface{})
	return error_map
}

func MonitorUpdateAll(internal_database_name string, ts_database_table string, ts_connection_database_name string, ts_tablename string, api_server_connection_table string, api_server_connection_id int64) {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	filter := make(map[string]interface{})
	monitor_list := DatamanFilter("service_monitor", filter, options)

	// Get all our current Api tasks
	tasks := Api_GetTasks(internal_database_name, api_server_connection_table, api_server_connection_id)

	UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: Tasks: %s\n", JsonDump(tasks))

	time_store_item_monitor_map := make(map[string]map[string]interface{})

	for _, service_monitor := range monitor_list {
		business_environment_namespace_metric := DatamanGet("business_environment_namespace_metric", int(service_monitor["business_environment_namespace_metric_id"].(int64)), options)

		UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: %s: %d\n", service_monitor["name"], business_environment_namespace_metric["time_store_item_id"])

		time_store_item_id_str := fmt.Sprintf("%d", business_environment_namespace_metric["time_store_item_id"])

		if tasks[time_store_item_id_str] != nil {
			UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: FOUND: %s: %d\n", service_monitor["name"], business_environment_namespace_metric["time_store_item_id"])

			Api_StopTask(internal_database_name, business_environment_namespace_metric["time_store_item_id"].(int64), api_server_connection_table, api_server_connection_id)
			Api_AddTask_Delay(internal_database_name, service_monitor["_id"].(int64), ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)
		} else {
			UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: NOT FOUND: %s: %d\n", service_monitor["name"], business_environment_namespace_metric["time_store_item_id"])

			Api_AddTask(internal_database_name, service_monitor["_id"].(int64), ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)
		}

		time_store_item_monitor_map[time_store_item_id_str] = service_monitor
	}

	// Update the tasks after our above changes
	tasks = Api_GetTasks(internal_database_name, api_server_connection_table, api_server_connection_id)

	UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: Task Map: %s\n", JsonDump(tasks))
	UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: Time Store Monitor Map: %s\n", JsonDump(time_store_item_monitor_map))


	// Go through all the tasks and any that we dont know about, remove
	for task_key, _ := range tasks {
		UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: Testing for Missing: %s: %v\n\n", task_key, time_store_item_monitor_map[task_key])
		if time_store_item_monitor_map[task_key] == nil {
			task_id, err := strconv.ParseInt(task_key, 10, 64)
			if err != nil {
				UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: Remove: Error parsing key: %s\n", task_key)
			} else {
				UdnLogLevel(nil, log_trace, "CUSTOM: MonitorUpdateAll: Remove: Stopping: %s\n", task_key)
				Api_StopTask(internal_database_name, task_id, api_server_connection_table, api_server_connection_id)
			}
		}
	}
}

func Api_GetTasks(internal_database_name string, api_server_connection_table string, api_server_connection_id int64) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

	data := make(map[string]interface{})
	http_result := HttpsRequest(api_server["host"].(string), int(api_server["port"].(int64)), api_server["default_path"].(string), "GET", api_server["client_certificate"].(string), api_server["client_private_key"].(string), api_server["certificate_authority"].(string), JsonDump(data))

	result_map, err := JsonLoadMap(string(http_result))
	if err != nil {
		UdnLogLevel(nil, log_trace, "Api_GetTasks: Error JSON Parse: %s\n", err)
		UdnLogLevel(nil, log_trace, "Api_GetTasks: Error: Result: %s\n", string(http_result))
	}

	return result_map
}

func Api_GetData(internal_database_name string, service_monitor_id int64, ts_database_table string, ts_connection_database_name string, ts_tablename string, api_server_connection_table string, api_server_connection_id int64) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	service_monitor := DatamanGet("service_monitor", int(service_monitor_id), options)
	service_monitor_type := DatamanGet("service_monitor_type", int(service_monitor["service_monitor_type_id"].(int64)), options)

	business := DatamanGet("business", int(service_monitor["business_id"].(int64)), options)
	business_user_robot := DatamanGet("business_user", int(business["api_robot_business_user_id"].(int64)), options)

	business_environment_namespace := DatamanGet("business_environment_namespace", int(service_monitor["business_environment_namespace_id"].(int64)), options)

	environment := DatamanGet("environment", int(business_environment_namespace["environment_id"].(int64)), options)

	business_environment_namespace_metric := DatamanGet("business_environment_namespace_metric", int(service_monitor["business_environment_namespace_metric_id"].(int64)), options)

	// Get the Time Store Item
	time_store_item := DatamanGet("time_store_item", int(business_environment_namespace_metric["time_store_item_id"].(int64)), options)

	// Interval, in seconds, from milliseconds
	interval := service_monitor["interval_ms"].(int64) / 1000

	data := make(map[string]interface{})
	data["uuid"] = fmt.Sprintf("%d", time_store_item["_id"])
	data["executor"] = "monitor"
	executor_args := make(map[string]interface{})
	executor_args["data_returner"] = "api"
	data_returner_args := make(map[string]interface{})
	data_returner_args["url"] = fmt.Sprintf("http://localhost:8888/v1/metrics/mm/%s/%s/mmp/write", environment["api_name"], business_environment_namespace["api_name"])
	data_returner_args["username"] = business_user_robot["name"]		// unique names for now
	data_returner_args["password"] = business_user_robot["password_digest"]

	label_map := make(map[string]interface{})
	label_map["service_monitor_name"] = business_environment_namespace_metric["name"]
	label_map["service_monitor_id"] = fmt.Sprintf("%d", service_monitor["_id"])
	label_map["type"] = service_monitor_type["name_api"]
	data_returner_args["labels"] = label_map

	// Save the labelset
	//TODO(g): Can this be done somewhere more related to setting it?
	hasher := crypto.SHA512.New()
	hasher.Write([]byte(JsonDump(label_map)))
	labelset_hash := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	business_environment_namespace_metric["labelset_data_jsonb"] = label_map
	business_environment_namespace_metric["labelset_hash"] = labelset_hash

	business_environment_namespace_metric_result := DatamanSet("business_environment_namespace_metric", business_environment_namespace_metric, options)

	UdnLogLevel(nil, log_trace,"Api_GetData: Metric Result Stored: %s\n", JsonDump(business_environment_namespace_metric_result))

	executor_args["data_returner_args"] = data_returner_args
	executor_args["interval"] = fmt.Sprintf("%ds", interval)
	executor_args["monitor"] = service_monitor_type["name_api"]
	monitor_args := make(map[string]interface{})
	monitor_args["url"] = service_monitor["data_json"].(map[string]interface{})["url"]
	executor_args["monitor_args"] = monitor_args
	data["executor_args"] = executor_args

	return data
}

func Api_UpdateTask(internal_database_name string, service_monitor_id int64, ts_database_table string, ts_connection_database_name string, ts_tablename string, api_server_connection_table string, api_server_connection_id int64) bool {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	data := Api_GetData(internal_database_name, service_monitor_id, ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)

	UdnLogLevel(nil, log_trace, "CUSTOM: Api: Update Task: %s\n", JsonDump(data))

	api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

	uri_path := fmt.Sprintf("%s/%s", api_server["default_path"].(string), data["uuid"])

	http_result := HttpsRequest(api_server["host"].(string), int(api_server["port"].(int64)), uri_path, "POST", api_server["client_certificate"].(string), api_server["client_private_key"].(string), api_server["certificate_authority"].(string), JsonDump(data))

	UdnLogLevel(nil, log_trace, "CUSTOM: Api: Update Task: Result: %s\n", JsonDump(http_result))

	return true
}

func Api_StopTask(internal_database_name string, time_store_item_id int64, api_server_connection_table string, api_server_connection_id int64) bool {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

	uri_path := fmt.Sprintf("%s/%d", api_server["default_path"].(string), time_store_item_id)

	http_result := HttpsRequest(api_server["host"].(string), int(api_server["port"].(int64)), uri_path, "STOP", api_server["client_certificate"].(string), api_server["client_private_key"].(string), api_server["certificate_authority"].(string), JsonDump(make(map[string]interface{})))

	UdnLogLevel(nil, log_trace, "CUSTOM: Api: Stop Task: Result: %s\n", JsonDump(http_result))

	return true
}

func Api_AddTask_Delay(internal_database_name string, service_monitor_id int64, ts_database_table string, ts_connection_database_name string, ts_tablename string, api_server_connection_table string, api_server_connection_id int64) bool {
	// Defer the add for a few seconds, allowing the stop to finish
	time.Sleep(5 * time.Second)

	result := Api_AddTask(internal_database_name, service_monitor_id, ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)

	return result
}

func Api_AddTask(internal_database_name string, service_monitor_id int64, ts_database_table string, ts_connection_database_name string, ts_tablename string, api_server_connection_table string, api_server_connection_id int64) bool {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	data := Api_GetData(internal_database_name, service_monitor_id, ts_database_table, ts_connection_database_name, ts_tablename, api_server_connection_table, api_server_connection_id)

	UdnLogLevel(nil, log_trace, "CUSTOM: Api: Add Task: %s\n", JsonDump(data))

	api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

	http_result := HttpsRequest(api_server["host"].(string), int(api_server["port"].(int64)), api_server["default_path"].(string), "PUT", api_server["client_certificate"].(string), api_server["client_private_key"].(string), api_server["certificate_authority"].(string), JsonDump(data))

	UdnLogLevel(nil, log_trace, "CUSTOM: Api: Add Task: Result: %s\n", JsonDump(http_result))

	return true
}

func UDN_Custom_Duty_Shift_Summary(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	duty_id := GetResult(args[1], type_int).(int64)
	time_start_str := GetResult(args[2], type_string).(string)
	time_stop_str := GetResult(args[3], type_string).(string)

	time_start, _ := time.Parse(time_format_go, time_start_str)
	time_stop, _ := time.Parse(time_format_go, time_stop_str)


	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	error_map := GetDutyShiftSummary(internal_database_name, duty_id, time_start, time_stop)

	result := UdnResult{}
	result.Result = error_map

	return result
}

func GetDutyShiftSummary(internal_database_name string, duty_id int64, time_start time.Time, time_stop time.Time) []map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	duty := DatamanGet("duty", int(duty_id), options)

	UdnLogLevel(nil, log_trace, "GetDutyShiftSummary: %d: %s  Start: %v  Stop: %v", duty_id, duty["name"], time_start, time_stop)

	filter := map[string]interface{}{
		"duty_id": []interface{}{"=", duty_id},
	}
	options["sort"] = []string{"priority"}
	duty_responsibility_array := DatamanFilter("duty_responsibility", filter, options)
	options["sort"] = nil

	result_map := make(map[string]map[string]interface{})

	for _, duty_responsibility := range duty_responsibility_array {
		filter := map[string]interface{}{
			"schedule_timeline_id": []interface{}{"=", duty_responsibility["schedule_timeline_id"]},
			"time_stop": []interface{}{">", time_start},
			"time_start": []interface{}{"<", time_stop},
		}
		options["sort"] = []string{"time_start"}
		schedule_timeline_item_array := DatamanFilter("schedule_timeline_item", filter, options)
		options["sort"] = nil

		for _, schedule_timeline_item := range schedule_timeline_item_array {
			UdnLogLevel(nil, log_trace, "Duty Responsibility: %s  Start: %v  Stop: %v  User: %d\n", duty_responsibility["name"], schedule_timeline_item["time_start"], schedule_timeline_item["time_stop"], schedule_timeline_item["business_user_id"])

			business_user := DatamanGet("business_user", int(schedule_timeline_item["business_user_id"].(int64)), options)

			// Ensure we also have a record for this key
			key := schedule_timeline_item["time_start"].(string)
			if result_map[key] == nil {
				result_record := make(map[string]interface{})
				result_map[key] = result_record
			}

			// Populate the result_map[key] fields
			result_map[key]["time_start"] = schedule_timeline_item["time_start"]
			result_map[key]["time_stop"] = schedule_timeline_item["time_stop"]

			summary := fmt.Sprintf("%s: <b>%s %s</b>", duty_responsibility["name"], business_user["name_first"], business_user["name_last"])

			// Add separator to the summary
			if result_map[key]["summary"] != nil {
				result_map[key]["summary"] = fmt.Sprintf("%s ", result_map[key]["summary"])
				// Append the new summary
				result_map[key]["summary"] = fmt.Sprintf("%s%s", result_map[key]["summary"], summary)
			} else {
				result_map[key]["summary"] = fmt.Sprintf("%s", summary)
			}

		}
	}

	// Sort the result map keys, and insert into result_array
	result_array := make([]map[string]interface{}, 0)

	//TODO(g): Sort and go from keys, use result_map[key] to make upgrading immediate
	//TODO(g): These are already sorted from the DB, but we should always double check here, because we have more complex cases coming where the line up changes
	for key, _ := range result_map {
		result_array = append(result_array, result_map[key])
	}

	//TODO(g): Duplicate the last entry so we can see when it ends?  Remove, keep, make it an option?  Add option map?  Can be optional on UDN side
	if len(result_array) > 0 {
		last_item := MapCopy(result_array[len(result_array)-1])

		// This allows us to see whend the last event ends
		last_item["time_start"] = last_item["time_stop"]

		result_array = append(result_array, last_item)
	}


	return result_array
}

func UDN_Custom_Duty_Responsibility_Current_User(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	duty_responsibility_id := GetResult(args[1], type_int).(int64)

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	user := GetDutyResponsibilityCurrentUser(internal_database_name, duty_responsibility_id)

	result := UdnResult{}
	result.Result = user

	return result
}

func GetDutyResponsibilityCurrentUser(internal_database_name string, duty_responsibility_id int64) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name


	duty_responsibility := DatamanGet("duty_responsibility", int(duty_responsibility_id), options)

	now := time.Now()

	filter := map[string]interface{}{
		"schedule_timeline_id": []interface{}{"=", duty_responsibility["schedule_timeline_id"]},
		"time_start": []interface{}{"<", now},
		"time_stop": []interface{}{">", now},
	}
	options["sort"] = []string{"time_start"}
	schedule_timeline_item_array := DatamanFilter("schedule_timeline_item", filter, options)
	options["sort"] = nil

	business_user_id := schedule_timeline_item_array[0]["business_user_id"].(int64)

	business_user := DatamanGet("business_user", int(business_user_id), options)

	return business_user
}

func UDN_Custom_Duty_Roster_User_Shift_Info(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	duty_roster_id := GetResult(args[1], type_int).(int64)
	duty_responsibility_id := GetResult(args[2], type_int).(int64)

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	user := GetDutyRosterUserShiftInfo(internal_database_name, duty_roster_id, duty_responsibility_id)

	result := UdnResult{}
	result.Result = user

	return result
}

func GetDutyRosterUserShiftInfo(internal_database_name string, duty_roster_id int64, duty_responsibility_id int64) []map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	result_array := make([]map[string]interface{}, 0)

	duty_responsibility := DatamanGet("duty_responsibility", int(duty_responsibility_id), options)


	filter := map[string]interface{}{
		"duty_roster_id": []interface{}{"=", duty_roster_id},
	}
	options["sort"] = []string{"priority"}
	duty_roster_business_user_array := DatamanFilter("duty_roster_business_user", filter, options)
	options["sort"] = nil

	now := time.Now()

	for _, duty_roster_business_user := range duty_roster_business_user_array {
		business_user := DatamanGet("business_user", int(duty_roster_business_user["business_user_id"].(int64)), options)

		UdnLogLevel(nil, log_trace, "GetDutyRosterUserShiftInfo: User: %v\n", business_user)

		filter := map[string]interface{}{
			"schedule_timeline_id": []interface{}{"=", duty_responsibility["schedule_timeline_id"]},
			"business_user_id": []interface{}{"=", business_user["_id"]},
		}
		options["sort"] = []string{"time_start"}
		schedule_timeline_item_array := DatamanFilter("schedule_timeline_item", filter, options)
		options["sort"] = nil

		// Augment busienss_user with our scheduling information.  Are they currently on-call?
		business_user["oncall_color"] = "default"
		business_user["oncall_icon"] = "icon-bell3"
		business_user["oncall_next"] = fmt.Sprintf("Next %s: Never", duty_responsibility["name"])
		business_user["oncall_previous"] = fmt.Sprintf("Last %s: Never", duty_responsibility["name"])

		for _, schedule_timeline_item := range schedule_timeline_item_array {
			time_start, _ := time.Parse(time_format_db, schedule_timeline_item["time_start"].(string))
			time_stop, _ := time.Parse(time_format_db, schedule_timeline_item["time_stop"].(string))

			UdnLogLevel(nil, log_trace, "GetDutyRosterUserShiftInfo: Now: %v  Start: %v  Stop: %v\n", now, time_start, time_stop)

			if now.After(time_start) && now.Before(time_stop) {
				UdnLogLevel(nil, log_trace, "GetDutyRosterUserShiftInfo: Now: %v  Start: %v  Stop: %v -- INSIDE!\n", now, time_start, time_stop)
				business_user["oncall_color"] = duty_responsibility["format_color_class"]

				business_user["oncall_next"] = fmt.Sprintf("Currently %s", duty_responsibility["name"])

				break
			} else {

				// Get the Next Oncall
				if business_user["next_oncall"] == nil {
					business_user["next_oncall"] = time_start
					business_user["oncall_next"] = fmt.Sprintf("Starts %s at %s", duty_responsibility["name"], business_user["next_oncall"].(time.Time).Format(time_format_db))
				} else {
					if time_start.After(business_user["next_oncall"].(time.Time)) {
						business_user["next_oncall"] = time_start
						business_user["oncall_next"] = fmt.Sprintf("Starts %s at %s", duty_responsibility["name"], business_user["next_oncall"].(time.Time).Format(time_format_db))
					}
				}
			}

			// Get the Last Oncall
			if business_user["previous_oncall"] == nil {
				business_user["previous_oncall"] = time_stop
				business_user["oncall_previous"] = fmt.Sprintf("Last %s at %s", duty_responsibility["name"], business_user["previous_oncall"].(time.Time).Format(time_format_db))
			} else {
				if time_start.After(business_user["previous_oncall"].(time.Time)) && time_start.Before(now) {
					business_user["previous_oncall"] = time_stop
					business_user["oncall_previous"] = fmt.Sprintf("Last %s at %s", duty_responsibility["name"], business_user["previous_oncall"].(time.Time).Format(time_format_db))
				}
			}
		}

		// Append business_user to the result_array
		result_array = append(result_array, business_user)
	}

	return result_array
}

func UDN_Custom_Activity_Daily(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	table_name := GetResult(args[1], type_string).(string)
	time_start_field_name := GetResult(args[2], type_string).(string)
	days := GetResult(args[3], type_int).(int64)
	field_match_map := GetResult(args[4], type_map).(map[string]interface{})
	time_start := ""
	if len(args) > 5 {
		time_start = GetResult(args[5], type_string).(string)
	}

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	data := ActivityDaily(internal_database_name, table_name, time_start_field_name, int(days), field_match_map, time_start)

	result := UdnResult{}
	result.Result = data

	return result
}

func ActivityDaily(internal_database_name string, table_name string, time_start_field_name string, days int, field_match_map map[string]interface{}, time_start_str string) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	// All queries must have business_id in their table schema, because we need to enforce security
	business := GetUserBusiness(internal_database_name)

	start := time.Now()
	if time_start_str != "" {
		start, _ = time.Parse(time_format_db, time_start_str)
	}


	duration_from_start_of_day := time.Duration(start.Hour() * 3600 + start.Second())
	today_start := start.Add(-duration_from_start_of_day)
	one_week_ago := today_start.AddDate(0, 0, -days)

	//NOTE(g): No need to filter on the end, because we are tracking to NOW.  Only if we
	filter := map[string]interface{}{
		"business_id": []interface{}{"=", business["_id"]},
		time_start_field_name: []interface{}{">", one_week_ago},
		time_start_field_name: []interface{}{">", one_week_ago},
	}

	// Dynamically add elements of the field_map into this to further contrain the query
	for key, value := range field_match_map {
		filter[key] = []interface{}{"=", value}
	}

	// Query the dynamic talbe_name
	options["sort"] = []string{time_start_field_name}
	activity_array := DatamanFilter(table_name, filter, options)
	options["sort"] = nil

	// Make the return and tallying arrays
	result_map := make(map[string]interface{})
	day_array := make([]interface{}, days)
	value_array := make([]interface{}, days)
	result_map["days"] = day_array
	result_map["values"] = value_array
	result_map["max"] = 10

	// Popualte the days and initial values
	for day := 0; day < days ; day++ {
		// Get how many days_ago from the start we will move backwards.  0 being the beginning of today
		days_ago := (days-1) - day
		cur_time := start.AddDate(0, 0, -days_ago)

		day_array[day] = cur_time.Weekday().String()
		value_array[day] = 0
	}

	// Tally the events into days
	for _, activity := range activity_array {
		activity_start_str := activity[time_start_field_name].(string)
		activity_start, _ := time.Parse(time_format_db, activity_start_str)

		for day := 0; day < days ; day++ {
			// Get how many days_ago from the start we will move backwards.  0 being the beginning of today
			days_ago := (days-1) - day
			cur_time := start.AddDate(0, 0, -days_ago)
			cur_time_next_day := cur_time.AddDate(0, 0, 1)

			//TODO(g): Does this take care of edge boundaries, or could an exact midnight time get skipped?
			if activity_start.After(cur_time) && activity_start.Before(cur_time_next_day) {
				value_array[day] = value_array[day].(int) + 1
				break
			}
		}
	}

	// Check for a higher daily max
	for day := 0; day < days ; day++ {
		if value_array[day].(int) > result_map["max"].(int) {
			result_map["max"] = value_array[day]
		}
	}

	return result_map
}

func GetUserBusiness(internal_database_name string) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	//TODO(g): Actually get this from the current user
	business_id := 1

	business := DatamanGet("business", business_id, options)

	return business
}

func UDN_Custom_Date_Range_Parse(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	page_args := GetResult(args[0], type_map).(map[string]interface{})

	default_duration_start := ""
	default_duration_stop := ""

	if len(args) > 1 {
		default_duration_start = GetResult(args[1], type_string).(string)
	}

	if len(args) > 2 {
		default_duration_stop = GetResult(args[2], type_string).(string)
	}

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	data_range_str := DateRangeParseFromMap(page_args, default_duration_start, default_duration_stop)

	result := UdnResult{}
	result.Result = data_range_str

	return result
}

func DateRangeParseFromMap(page_args map[string]interface{}, default_duration_start string, default_duration_stop string) string {
	result_str := ""

	start := time.Now()
	duration_from_start_of_day_int := (start.Hour()*3600) + (start.Minute()*60) + start.Second()

	duration_from_start_of_day := time.Duration(-duration_from_start_of_day_int*1000000000)

	UdnLogLevel(nil, log_trace, "DateRangeParse: duration_from_start_of_day: %v %T   Default: %s %s\n", duration_from_start_of_day, duration_from_start_of_day, default_duration_start, default_duration_stop)
	UdnLogLevel(nil, log_trace, "DateRangeParse: duration_from_start_of_day: %v %v %v\n", start.Hour(), start.Minute(), start.Second())

	today_start := start.Add(duration_from_start_of_day)
	UdnLogLevel(nil, log_trace, "DateRangeParse: today_start: %v\n", today_start)
	UdnLogLevel(nil, log_trace, "DateRangeParse: page args: %s\n", JsonDump(page_args))


	if page_args["from_days_ago"] != nil && page_args["to_days_ago"] != nil {
		from_days_ago_int := int(GetResult(page_args["from_days_ago"], type_int).(int64))
		to_days_ago_int := int(GetResult(page_args["to_days_ago"], type_int).(int64))

		from_days_ago := today_start.AddDate(0, 0, -from_days_ago_int)
		to_days_ago := today_start.AddDate(0, 0, -to_days_ago_int)

		result_str = fmt.Sprintf("%s - %s", from_days_ago.Format(time_format_db), to_days_ago.Format(time_format_db))
		UdnLogLevel(nil, log_trace, "DateRangeParse: Result: %s\n", result_str)
	}

	// If there is no valid result_str, then use the default info
	if result_str == "" && default_duration_start != "" {
		//TODO(g): Handle errors
		start_duration, _ := time.ParseDuration(default_duration_start)

		// Use now (-0s) if we dont have a stop, or parse it if it exists
		stop_duration, _ := time.ParseDuration("-0s")
		if default_duration_stop != "" {
			stop_duration, _ = time.ParseDuration(default_duration_stop)
		}

		UdnLogLevel(nil, log_trace, "DateRangeParse: Default: Duration: %v  -  %v\n", start_duration, stop_duration)

		from_days_ago := start.Add(start_duration)
		to_days_ago := start.Add(stop_duration)

		result_str = fmt.Sprintf("%s - %s", from_days_ago.Format(time_format_db), to_days_ago.Format(time_format_db))
		UdnLogLevel(nil, log_trace, "DateRangeParse: Default: Result: %s\n", result_str)
	}

	return result_str
}

func UDN_Custom_Dashboard_Item_Edit(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	dashboard_item_id_or_nil := args[1]
	input_map := GetResult(args[2], type_map).(map[string]interface{})

	api_server_connection_table := GetResult(args[3], type_string).(string)
	api_server_connection_id := GetResult(args[4], type_int).(int64)

	business := GetResult(args[5], type_map).(map[string]interface{})

	input_data_map := GetResult(args[6], type_map).(map[string]interface{})

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	data := DashboardItemEdit(internal_database_name, business, dashboard_item_id_or_nil, input_map, input_data_map, api_server_connection_table, api_server_connection_id)

	result := UdnResult{}
	result.Result = data

	return result
}

func DashboardItemEdit(internal_database_name string, business map[string]interface{}, dashboard_item_id_or_nil interface{}, input_map map[string]interface{}, input_data_map map[string]interface{}, api_server_connection_table string, api_server_connection_id int64) map[string]interface{} {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	//// All queries must have business_id in their table schema, because we need to enforce security
	//business := GetUserBusiness(internal_database_name)

	// Assume this is a new dashboard_item
	graph := make(map[string]interface{})
	
	// If this is an existing dashboard item.  Load it...
	if dashboard_item_id_or_nil != nil {
		dashboard_item_id := GetResult(dashboard_item_id_or_nil, type_int).(int64)

		graph = DatamanGet("dashboard_item", int(dashboard_item_id), options)
	} else {
		graph["name"] = fmt.Sprintf("%s", time.Now().Format(time_format_db))
	}


	UdnLogLevel(nil, log_trace, "DashboardItemEdit: Starting Graph: %s\n", JsonDump(graph))
	UdnLogLevel(nil, log_trace, "DashboardItemEdit: Input Map: %s\n", JsonDump(input_map))
	UdnLogLevel(nil, log_trace, "DashboardItemEdit: Input Data Map: %s\n", JsonDump(input_data_map))


	// Return data structure
	return_data := make(map[string]interface{})
	return_data["data_point_array"] = make([]map[string]interface{}, 0)	// These are all the different data elements we want to render in this graph

	// -- Process the input_map and determine what to data to populate into our return data --

	// If this is a new graph
	if graph["_id"] == nil {
		var source string
		var env string
		var namespace string
		var query string

		robot_business_user := DatamanGet("business_user", int(business["api_robot_business_user_id"].(int64)), options)

		user := robot_business_user["name"].(string)
		password := robot_business_user["password_digest"].(string)

		// Look inside input_map and determine what to initially populate the return_data with
		if input_data_map["query"] != nil {
			source = "mm"
			env = GetResult(input_data_map["query_env"], type_string).(string)
			namespace = GetResult(input_data_map["query_namespace"], type_string).(string)

			query = GetResult(input_data_map["query"], type_string).(string)
		} else if input_map["service_monitor_id"] != nil {
			service_monitor_id := GetResult(input_map["service_monitor_id"], type_int).(int64)

			// We are getting directed to render information ahout this service_monitor, so start off the graph with a default representation
			service_monitor := DatamanGet("service_monitor", int(service_monitor_id), options)

			business_environment_namespace := DatamanGet("business_environment_namespace", int(service_monitor["business_environment_namespace_id"].(int64)), options)
			environment := DatamanGet("environment", int(business_environment_namespace["environment_id"].(int64)), options)

			source = "mm"
			env = environment["api_name"].(string)
			namespace = business_environment_namespace["api_name"].(string)

			query = fmt.Sprintf("{service_monitor_id=\"%d\"}", service_monitor_id)
		} else {
			source = "mm"
			env = GetResult(input_map["query_env"], type_string).(string)
			namespace = GetResult(input_map["query_namespace"], type_string).(string)

			query = GetResult(input_map["query"], type_string).(string)
		}

		// Handle time range
		start := time.Now().Add(-1 * time.Hour).Unix()
		end := time.Now().Unix()
		step := 20

		// Server info for API
		api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

		// Encode query_arg
		query_arg := url.Values{}
		query_arg.Add("query", query)
		query_encoded := query_arg.Encode()

		// This is the new query
		path := fmt.Sprintf("v1/metrics/%s/%s/%s/prometheus/api/v1/query_range?%s&start=%d&end=%d&step=%d", source, env, namespace, query_encoded, start, end, step)

		UdnLogLevel(nil, log_trace, "DashboardItemEdit: API Prom Path: Business: %s\n", path)

		http_result := HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "GET", JsonDump(make(map[string]interface{})), user, password)

		api_payload, _ := JsonLoadMap(string(http_result))


		// This is the new data
		return_data["api_render_data"] = API_Generate_Graph_Data(internal_database_name, api_payload)
		return_data["query"] = strings.Replace(query, "\"", "&quot;", -1)
		return_data["query_env"] = env
		return_data["query_namespace"] = namespace
	} else {
		// Else, this is an saved graph
	}

	// Process more input_map stuff
	if input_map["dashboard_item_name"] != nil {
		//TODO(g): This goes into return_data, not the graph/dashboard_item.  It's muddled at the moment, needs to be clarified
		return_data["name"] = input_map["dashboard_item_name"]
	}

	// Save this stuff?
	//TODO(g): Decide whether to always save or not...  Should only be done on demand.  input_map["save_dashboard_item"]==true, then we set it to nil or whatever

	return return_data
}


func API_Generate_Graph_Data(internal_database_name string, api_payload map[string]interface{}) map[string]interface{} {
	data := make(map[string]interface{})

	options := make(map[string]interface{})
	options["db"] = internal_database_name

	/*
		"json_data": [
		  {
			"z": [],
			"x": [],
			"type": "scatter"
		  }
		],

		"json_layout": {
		  "title": "That One Thing We Measure",

		  "showlegend": false

		},

		"json_options": {
		  "displayModeBar": false
		}
	*/

	title := fmt.Sprintf("API Title")

	data["json_layout"] = map[string]interface{}{
		"title": title,
		"showlegend": true,
	}
	data["json_options"] = map[string]interface{}{
		"displayModeBar": true,
		"displaylogo": false,
		"modeBarButtonsToRemove": []interface{}{"sendDataToCloud", "autoScale2d", "pan2d", "zoom2d", "select2d", "lasso2d", "zoomIn2d", "zoomOut2d"},
	}

	// Array of Data Item maps
	json_data_array := make([]interface{}, 0)

	//UdnLogLevel(nil, log_trace, "API_Generate_Graph_Data: Payload: %s\n", JsonDump(api_payload))

	api_result := api_payload["data"].(map[string]interface{})["result"].([]interface{})

	for _, item := range api_result {
		//UdnLogLevel(nil, log_trace, "API_Generate_Graph_Data: Item: %s\n", JsonDump(item))

		metric_map := item.(map[string]interface{})["metric"].(map[string]interface{})
		values := item.(map[string]interface{})["values"].([]interface{})

		// Skip this one, it's just the start time and graphs poorly
		if metric_map["__name__"] == "meta_start_time" || metric_map["__name__"] == "meta_duration" {
			continue
		}

		label_map := MapCopy(metric_map)
		delete(label_map, "__name__")
		delete(label_map, "hostname")
		text_value := strings.Replace(JsonDump(label_map), "\n", "", -1)

		x_array := make([]interface{}, 0)			// Times - Array of Series(Array)
		y_array := make([]interface{}, 0)			// Values - Array of Series(Array)
		text_array := make([]interface{}, 0)			// Text Label per Point - Array of Series(Array)

		for _, value_array := range values {
			x_value_int := int64(value_array.([]interface{})[0].(float64))

			x_value := time.Unix(x_value_int, 0).Format(time_format_db)

			y_str := value_array.([]interface{})[1].(string)
			y_value, _ := strconv.ParseFloat(y_str, 64)

			/*
				  "metric": {
					"__name__": "duration",
					"hostname": "Geoffs-MacBook-Pro-2.local",
					"service_monitor_id": "30",
					"type": "http"
				  },
			*/

			//UdnLogLevel(nil, log_trace, "API_Generate_Graph_Data: Item Pair: %d  --  %f  (%s)\n", x_value, y_value, y_error)

			x_array = append(x_array, x_value)
			y_array = append(y_array, y_value)
			text_array = append(text_array, text_value)
		}

		// Build the data_item map
		data_item := map[string]interface{}{
			"type": "scatter",
			"mode": "line",
			"name": fmt.Sprintf("%s", metric_map["__name__"]),
			"x": x_array,			// Times - Array of Series(Array)
			"y": y_array,			// Values - Array of Series(Array)

			//"text": text_array,		// Text - Array of Series(Array)

			//"mode": "markers",
			//"marker": map[string]interface{}{
			//	"size": 12,
			//},
		}

		// Append the item
		json_data_array = append(json_data_array, data_item)
	}

	data["json_data"] = json_data_array
	//data["debug_alert"] = true

	return data
}

func GetMapKeysAsSelector(data map[string]interface{}, prefix string) ([]string, []interface{}) {
	keys := make([]string, 0)
	values := make([]interface{}, 0)

	for key, key_value := range data {

		switch key_value.(type) {
		case map[string]interface{}:
			key_prefix := fmt.Sprintf("%s%s.", prefix, key)

			new_keys, new_values := GetMapKeysAsSelector(key_value.(map[string]interface{}), key_prefix)
			for _, new_key := range new_keys {
				keys = append(keys, new_key)
			}
			for _, new_value := range new_values {
				values = append(values, new_value)
			}

			break
		default:
			key_str := fmt.Sprintf("%s%s", prefix, key)
			keys = append(keys, key_str)

			values = append(values, key_value)
		}
	}

	sort.Strings(keys)

	return keys, values
}

func DashboardItemGetFieldOptions(field_selector string, time_series_metric map[string]interface{}) []map[string]interface{} {
	field_options := make([]map[string]interface{}, 0)

	// Get all the names
	prefix := ""
	field_names, field_values := GetMapKeysAsSelector(time_series_metric, prefix)

	UdnLogLevel(nil, log_trace, "DashboardItemGetFieldOptions: Field Names: %v\n", field_names)

	for index, name := range field_names {
		field_item := make(map[string]interface{})

		//label=name,name=name,value=_id,selected=selected
		name_html := fmt.Sprintf("%s  [%T]", name, field_values[index])
		field_item["name"] = HtmlClean(name_html)
		field_item["value"] = name

		if name == field_selector {
			field_item["selected"] = "selected"
		}

		field_options = append(field_options, field_item)
	}

	UdnLogLevel(nil, log_trace, "DashboardItemGetFieldOptions: Field Options: %v\n", field_options)

	return field_options
}

//TODO(g): Move this to utility so it's generally accessible
//TODO(g): Repalce inside of UDN_ArrayMapToSeries() with call to this to reduce duplicate and single-purpose code, this is a general solution
func ArrayMapToSeries(array_map []map[string]interface{}, map_key string) []interface{} {
	// Get the remapping information
	map_key_parts := strings.Split(map_key, ".")

	result_array := make([]interface{}, 0)

	UdnLogLevel(nil, log_trace, "ArrayMapToSeries: %s in %d Record(s)\n", map_key, len(array_map))

	for _, item := range array_map {
		var item_value interface{}

		item_value = item

		for _, part := range map_key_parts {
			//UdnLogLevel(udn_schema, log_trace, "Array Map To Series: Step In: %d: %s: %v\n", index, part, SnippetData(item_value, 300))

			item_value = item_value.(map[string]interface{})[part]
		}

		result_array = append(result_array, item_value)
	}

	return result_array
}

func UDN_Custom_Dataman_Create_Filter_Html(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	field_label := GetResult(args[1], type_string).(string)
	filter_array := GetResult(args[2], type_array).([]interface{})
	input_map := GetResult(args[3], type_map).(map[string]interface{})

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	html := DatamanCreateFilterHtml(internal_database_name, field_label, filter_array, input_map)

	result := UdnResult{}
	result.Result = html

	return result
}

func DatamanCreateFilterHtml(internal_database_name string, field_label string, filter_array []interface{}, input_map map[string]interface{}) string {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	UdnLogLevel(nil, log_trace, "DatamanCreateFilterHtml: field_label: %s: %s\n", field_label, JsonDump(filter_array))

	filter := map[string]interface{}{}
	options["sort"] = []string{"alias"}
	compare_array := DatamanFilter("type_compare", filter, options)
	compare_map := MapArrayToMap(compare_array, "alias")
	options["sort"] = nil

	html_field_array := make([]map[string]interface{}, 0)

	for index, outer_map := range filter_array {
		for field, inner_map := range outer_map.(map[string]interface{}) {
			for operator, value := range inner_map.(map[string]interface{}) {

				item_field_label := fmt.Sprintf("%s||%d||%s||%s", field_label, index, field, operator)

				UdnLogLevel(nil, log_trace, "DatamanCreateFilterHtml: %s: item: %s %s %s\n", item_field_label, field, operator, JsonDump(value))

				web_widget_name := compare_map[operator].(map[string]interface{})["web_widget_name"].(string)
				web_widget_html := GetWebWidgetHtml(web_widget_name)

				// Use the input_map
				data := MapCopy(input_map)
				data["_field_label"] = item_field_label
				data["value"] = value
				data["label"] = fmt.Sprintf("%s %s", field, operator)
				data["color"] = "primary"
				data["onclick"] = fmt.Sprintf("RPC('api/delete_field_in_record_json', {data: JSON.stringify({delete_record_field: '%s||%d'})})", field_label, index)

				data["name"] = fmt.Sprintf("%s_%d", input_map["name"], index)


				// Template
				data["_output"] = TemplateFromMap(web_widget_html, data)

				UdnLogLevel(nil, log_trace, "DatamanCreateFilterHtml: Field Array: %s: %s\n", item_field_label, JsonDump(data))

				html_field_array = append(html_field_array, data)
			}
		}
	}

	core_table := GetWebWidgetHtml("core_table_simple")
	core_icon_list := GetWebWidgetHtml("core_icon_list")
	core_button := GetWebWidgetHtml("core_button")


	for _, html_field_item := range html_field_array {
		icon_map_delete := map[string]interface{}{"icon": " icon-trash-alt", "color": "primary", "onclick": html_field_item["onclick"]}
		icon_map_array := []interface{}{icon_map_delete}
		icon_map_array_map := map[string]interface{}{
			"item": icon_map_array,
		}

		icon_list := TemplateFromMap(core_icon_list, icon_map_array_map)

		html_field_item["icons"] = icon_list
	}

	// Add final row to the html_field_array that has a button for adding new Rules
	onclick_string := fmt.Sprintf("GridRenderWidget_%s('dialog_add_rule', 'dialog_target', 'field_label', '%s')", input_map["uuid"], field_label)
	button_item := map[string]interface{}{
		"icons": "",
		"_output": TemplateFromMap(core_button, map[string]interface{}{"value": "Add New Rule", "icon": "icon-add", "color": "primary", "onclick": onclick_string}),
	}
	html_field_array = append(html_field_array, button_item)

	table_data := map[string]interface{}{
		"headers": []string{"", "Filter Rule"},
		"columns": []string{"icons", "_output"},
		"widths": map[string]interface{}{
			"icons": "40",
			"_output": "",
		},
		"items": html_field_array,
	}

	html := TemplateFromMap(core_table, table_data)

	return html
}

func GetWebWidgetHtml(name string) string {
	options := make(map[string]interface{})

	filter := map[string]interface{}{
		"name": []interface{}{"=", name},
	}
	web_widget_array := DatamanFilter("web_widget", filter, options)

	//UdnLogLevel(nil, log_trace, "GetWebWidgetHtml: %s: %v\n", name, web_widget_array)

	html := ""

	if len(web_widget_array) > 0 {
		html = web_widget_array[0]["html"].(string)
	}

	return html
}

func UDN_Custom_Dataman_Add_Rule(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	input_map := GetResult(args[0], type_map).(map[string]interface{})

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	html := DatamanAddRule(input_map)

	result := UdnResult{}
	result.Result = html

	return result
}

func DatamanAddRule(input_map map[string]interface{}) string {
	UdnLogLevel(nil, log_trace, "DatamanAddRule: %s\n", JsonDump(input_map))

	data_map := input_map["data"].(map[string]interface{})

	database, collection, record_pkey, field := ParseFieldLabel(data_map["field_label"].(string))


	options := make(map[string]interface{})
	options["db"] = database

	record_id, _ := strconv.ParseInt(record_pkey, 10, 64)
	record := DatamanGet(collection, int(record_id), options)

	data_operator := map[string]interface{}{}
	data_operator[data_map["operator"].(string)] = ""

	data := map[string]interface{}{}
	data[data_map["name"].(string)] = data_operator

	fields := strings.Split(field, "||")
	field_array := GetResult(fields, type_array).([]interface{})

	current_value := MapGet(field_array, record)
	if current_value != nil {
		current_value = append(current_value.([]interface{}), data)
	} else {
		current_value = []interface{}{data}
	}

	UdnLogLevel(nil, log_trace, "DatamanAddRule: Current: %s\n", JsonDump(current_value))

	//UdnLogLevel(nil, log_trace, "DataFieldMapDelete: Set Directly: %s\n", part_array)
	Direct_MapSet(field_array, current_value, record)

	// Save it
	DatamanSet(collection, record, options)

	UdnLogLevel(nil, log_trace, "DatamanAddRule: Result: %s\n", JsonDump(record))

	//html := TemplateFromMap(core_table, table_data)

	html := ""

	return html
}

func UDN_Custom_API_Business_Update(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	internal_database_name := GetResult(args[0], type_string).(string)
	api_server_connection_table := GetResult(args[1], type_string).(string)
	api_server_connection_id := GetResult(args[2], type_int).(int64)

	// Do all the work here, so I can call it from Go as well as UDN.  Need to cover the complex ground outside of UDN for now.
	API_BusinessUpdate(internal_database_name, api_server_connection_table, api_server_connection_id)

	result := UdnResult{}
	result.Result = nil

	return result
}

func API_BusinessUpdate(internal_database_name string, api_server_connection_table string, api_server_connection_id int64) {
	options := make(map[string]interface{})
	options["db"] = internal_database_name

	//TODO(g): Get this from the user login info...
	business_id := 1

	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: %d\n", business_id)

	business := DatamanGet("business", business_id, options)
	robot_user := DatamanGet("business_user", int(business["api_robot_business_user_id"].(int64)), options)

	api_server := DatamanGet(api_server_connection_table, int(api_server_connection_id), options)

	// Business
	data := make(map[string]interface{})
	data["name"] = fmt.Sprintf("%d", business_id)

	http_result := HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), "v1/business", "POST", JsonDump(data), "", "")

	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Business: %s\n", http_result)

	// Get the list of businesses
	http_result = HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), "v1/business", "GET", JsonDump(make(map[string]interface{})), "", "")

	var api_business_id int
	business_array, err := JsonLoadArray(string(http_result))
	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Business GET: %s (%v)\n", business_array, err)
	for _, business_item := range business_array {
		business_map := business_item.(map[string]interface{})
		if business_map["name"] == data["name"] {
			api_business_id = int(business_map["id"].(float64))
			break
		}
	}

	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: Found API Business: %d\n", api_business_id)


	// Add Business User
	data = make(map[string]interface{})
	data["name"] = robot_user["name"]
	data["business_id"] = api_business_id
	data["secret"] = robot_user["password_digest"]
	data["permissions"] = []interface{}{map[string]interface{}{
		"source": map[string]interface{}{
			"type": "=~",
			"value": ".*",
		},
		"environment": map[string]interface{}{
			"type": "=~",
			"value": ".*",
		},
		"namespace": map[string]interface{}{
			"type": "=~",
			"value": ".*",
		},
	}}

	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Insert User Payload: %s\n", JsonDump(data))


	path := fmt.Sprintf("v1/business/%d/user", api_business_id)

	http_result = HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "POST", JsonDump(data), "", "")

	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Business User POST: %s\n", http_result)

	http_result = HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "GET", JsonDump(make(map[string]interface{})), "", "")
	business_user_array, err := JsonLoadArray(string(http_result))
	UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Business User GET: %s (%v)\n", business_user_array, err)


	// Add Environment
	filter := map[string]interface{}{"business_id": []interface{}{"=", business_id}}
	environment_array := DatamanFilter("environment", filter, options)

	for _, environment := range environment_array {
		data = make(map[string]interface{})
		data["name"] = environment["api_name"]

		path := fmt.Sprintf("v1/business/%d/environment", api_business_id)

		http_result = HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "POST", JsonDump(data), "", "")
		UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Business Environment Insert: %s\n", http_result)


		http_result = HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "GET", JsonDump(data), "", "")
		environment_array, err := JsonLoadArray(string(http_result))
		UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Environment GET: %s (%v)\n", environment_array, err)

		for _, environment_item := range environment_array {
			environment_map := environment_item.(map[string]interface{})
			// Find the environment, and then add all it's namespaces
			if environment_map["name"] == environment["api_name"] {
				filter := map[string]interface{}{
					"business_id": []interface{}{"=", business_id},
					"environment_id": []interface{}{"=", environment["_id"]},
				}
				namespace_array := DatamanFilter("business_environment_namespace", filter, options)
				for _, namespace := range namespace_array {
					data = make(map[string]interface{})
					data["name"] = namespace["api_name"]
					data["environment_id"] = environment_map["id"]
					data["source"] = "mm"
					//data["retention_duration"] = "5s"	//TODO(g): Is this the total retention time?

					UdnLogLevel(nil, log_trace, "API_BusinessUpdate: Data: Business Namespace Insert: %s\n", JsonDump(data))

					path := fmt.Sprintf("v1/business/%d/namespace", api_business_id)

					http_result = HttpRequest(api_server["host"].(string), int(api_server["port"].(int64)), path, "POST", JsonDump(data), "", "")
					UdnLogLevel(nil, log_trace, "API_BusinessUpdate: HTTP Result: Business Namespace Insert: %s\n", http_result)
				}
			}
		}
	}
}

func UDN_Custom_Login(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result := UdnResult{}

	UdnLogLevel(udn_schema, log_error, "Custom Login: %v\n\n", args)

	//business_name := GetResult(args[0], type_string).(string)
	internal_database_name := GetResult(args[0], type_string).(string)
	business_name := GetResult(args[1], type_string).(string)
	username := GetResult(args[2], type_string).(string)
	password := GetResult(args[3], type_string).(string)

	options := map[string]interface{}{"db": internal_database_name}

	// Get the user (if it exists)
	filter := map[string]interface{}{}
	filter["alias"] = []interface{}{"=", business_name}
	business_array := DatamanFilter("business", filter, options)

	if len(business_array) == 0 {
		UdnLogLevel(udn_schema, log_debug, "Custom Login: Unknown business: %s: %v\n", business_name, business_array)
		result.Result = "invalid"
		return result
	}

	business := business_array[0]

	filter = map[string]interface{}{}
	filter["business_id"] = []interface{}{"=", business["_id"]}
	filter["name"] = []interface{}{"=", username}

	user_array := DatamanFilter("business_user", filter, options)

	UdnLogLevel(udn_schema, log_debug, "Custom Login: User Array: %v\n", user_array)

	if len(user_array) == 0 {
		UdnLogLevel(udn_schema, log_debug, "Custom Login: Unknown user: %v\n", username)
		result.Result = "invalid"
		return result
	} else {
		user := user_array[0]

		// Test the password
		hasher := crypto.SHA512.New()

		salted_password := fmt.Sprintf("%s.%s", user["password_salt"], password)

		UdnLogLevel(udn_schema, log_debug, "Custom Login: Password Salt: %s\n", salted_password)

		hasher.Write([]byte(salted_password))
		password_digest := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

		UdnLogLevel(udn_schema, log_debug, "Custom Login: Password Digest: %s\n", password_digest)

		// If the password is correct
		if password_digest == user["password_digest"] {
			filter = map[string]interface{}{}
			filter["business_user_id"] = []interface{}{"=", user["_id"]}
			user_session_array := DatamanFilter("business_user_session", filter, options)

			// If we dont already have a user session, make one
			if len(user_session_array) == 0 {
				// Create a new session token
				hasher := crypto.SHA512.New()

				session_hash_string := fmt.Sprintf("%s.%s.%s", username, password, time.Now().Format(time_format_db))
				hasher.Write([]byte(session_hash_string))
				session_hash := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

				// Create a user session
				user_session := map[string]interface{}{
					"business_user_id": user["_id"],
					"value": session_hash,
					"created": time.Now(),
				}

				user_session_result := DatamanInsert("business_user_session", user_session, options)

				UdnLogLevel(udn_schema, log_debug, "Custom Login: User Session Result: %v\n", user_session_result)

				result.Result = user_session_result["value"]
			} else {
				// Return their user session name
				result.Result = user_session_array[0]["value"]
			}
		} else {
			UdnLogLevel(udn_schema, log_debug, "Custom Login: Passwords do not match: %s != %s\n", password_digest, user["password_digest"])
		}
	}

	return result
}

func UDN_Custom_Auth(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	result_map := map[string]interface{}{}
	result := UdnResult{}
	result.Result = result_map

	UdnLogLevel(udn_schema, log_error, "Custom Auth: %v\n\n", args)

	//business_name := GetResult(args[0], type_string).(string)
	internal_database_name := GetResult(args[0], type_string).(string)
	session := GetResult(args[1], type_string).(string)

	result_map["session"] = session

	options := map[string]interface{}{"db": internal_database_name}

	// Get the user (if it exists)
	filter := map[string]interface{}{}
	filter["value"] = []interface{}{"=", session}
	business_user_session_array := DatamanFilter("business_user_session", filter, options)

	if len(business_user_session_array) == 0 {
		UdnLogLevel(udn_schema, log_debug, "Custom Auth: Unknown session: %s\n", session)
		return result
	}

	business_user_session := business_user_session_array[0]

	filter = map[string]interface{}{}
	filter["_id"] = []interface{}{"=", business_user_session["business_user_id"]}

	user_array := DatamanFilter("business_user", filter, options)

	UdnLogLevel(udn_schema, log_debug, "Custom Login: User Array: %v\n", user_array)

	if len(user_array) == 0 {
		UdnLogLevel(udn_schema, log_debug, "Custom Auth: Unknown user: %v\n", business_user_session)
		return result
	} else {
		user := user_array[0]

		result_map["user"] = user

		// Get any other data we want, and put it into the result map
		business := DatamanGet("business", int(user["business_id"].(int64)), options)
		result_map["business"] = business
	}

	return result
}

