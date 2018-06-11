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
	ao_database_name := GetResult(args[0], type_string).(string)
	uuid := GetResult(args[1], type_string).(string)
	executor := GetResult(args[2], type_string).(string)
	monitor_protocol := GetResult(args[3], type_string).(string)
	interval := GetResult(args[4], type_int).(int64)
	monitor_url := GetResult(args[5], type_string).(string)

	tablename := GetResult(args[6], type_string).(string)
	fieldname_customer_service_id := GetResult(args[7], type_string).(string)

	fieldname_created := "created"
	fieldname_data_json := "data_json"

	result := UdnResult{}
	result.Result = nil

	options := make(map[string]interface{})
	options["db"] = "ao"

	// Get the Roster Users ordered by priority
	options["sort"] = []string{"priority"}
	filter := map[string]interface{}{
		"name": []interface{}{"=", ao_database_name},
	}
	ao_database_array := DatamanFilter("ao_database", filter, options)
	if len(ao_database_array) == 0 {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: Error getting AO Database: %d\n", len(ao_database_array))
		return result
	}
	ao_database := ao_database_array[0]
	UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: Error getting AO Database: %v\n", ao_database)

	data := make(map[string]interface{})
	data["uuid"] = uuid
	data["executor"] = executor
	executor_args := make(map[string]interface{})
	data_returner_args := make(map[string]interface{})
	data_returner_args["type"] = ao_database["database_type"]
	data_returner_args["info"] = ao_database["database_connect_string"]
	data_returner_args["tablename"] = tablename
	data_returner_args["fieldname_customer_service_id"] = fieldname_customer_service_id
	data_returner_args["fieldname_created"] = fieldname_created
	data_returner_args["fieldname_data_json"] = fieldname_data_json
	executor_args["data_returner_args"] = data_returner_args
	executor_args["interval"] = fmt.Sprintf("%ds", interval)
	executor_args["monitor"] = monitor_protocol
	monitor_args := make(map[string]interface{})
	monitor_args["url"] = monitor_url
	executor_args["monitor_args"] = monitor_args
	data["executor_args"] = executor_args


	UdnLogLevel(udn_schema, log_debug, "CUSTOM: TaskMan: Add Task: %s\n", JsonDump(data))

	return result
}

func HttpsRequest(hostname string, port int, uri string, client_cert string, client_key string) {

}

