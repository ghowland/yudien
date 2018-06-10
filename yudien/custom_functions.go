package yudien

import (
	"database/sql"
	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudiendata"
	. "github.com/ghowland/yudien/yudienutil"
	"time"
	"strings"
	"strconv"
)

func UDN_Customer_PopulateScheduleDutyResponsibility(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	database := GetResult(args[0], type_string).(string)
	responsibility_id := GetResult(args[1], type_int).(int64)
	start_populating := GetResult(args[2], type_string).(string)
	//business_user_id := GetResult(args[3], type_string).(int64)

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


	EvaluateShiftTimes(shifts, start_time)


	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Result: %v\n", result.Result)

	return result
}

func EvaluateShiftTimes(shifts []map[string]interface{}, start_time time.Time) {
	UdnLogLevel(nil, log_debug, "Evaluate Shift Times: %v\n", shifts)
/*
	UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Start Weekday: %v\n", start_time.Weekday())

	year, month, day := start_time.Date()

	UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Start Weekday: %v %v %v\n", year, month, day)

	start_day_of_week := int(start_time.Weekday())

	UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Start Weekday: %v\n", start_day_of_week)
*/

	for _, shift := range shifts {
		shift_start := GetShiftTimeStart(start_time, shift, shifts)
		UdnLogLevel(nil, log_debug, "Evaluate Shift Times: %s: %v\n", shift["name"], shift_start)

		/*
		//shift_start := start_time.AddDate(0, 0, 0 - start_day_of_week + int(shift["start_day_of_week"].(int64)) )
		shift_start := start_time.AddDate(0, 0, 0 - start_day_of_week)
		UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Shift Start: %v\n", shift_start)

		start_hour, start_minute, start_second := shift_start.Clock()
		start_hour_duration := GetTimeOfDayDuration(start_hour, start_minute, start_second)

		shift_start_zero := shift_start.Add(-start_hour_duration)

		UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Shift Start Zero: %v\n", shift_start_zero)

		UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Clock: %d %d %d\n", start_hour, start_minute, start_second)

		hour, minute, second := GetTimeOfDayFromString(shift["start_time_of_day"].(string))
		time_seconds_duration := GetTimeOfDayDuration(hour, minute, second)

		UdnLogLevel(nil, log_debug, "Evaluate Shift Times: Duration: %v\n", time_seconds_duration)

		shirt_start_added := shift_start_zero.Add(time_seconds_duration)

		//UdnLogLevel(udn_schema, log_debug, "Evaluate Shift Times:: %s: %d: %v: %v\n", shift["name"], shift["start_day_of_week"], shift_start_zero, shirt_start_added)
		UdnLogLevel(nil, log_debug, "Evaluate Shift Times: %s: %d: %v\n", shift["name"], shift["start_day_of_week"], shirt_start_added)
*/
	}
}

func GetShiftTimeStart(start_time time.Time, shift map[string]interface{}, shifts []map[string]interface{}) time.Time {
	start_day_of_week := int(start_time.Weekday())
	shift_start := start_time.AddDate(0, 0, 0 - start_day_of_week)

	start_hour, start_minute, start_second := shift_start.Clock()
	start_hour_duration := GetTimeOfDayDuration(start_hour, start_minute, start_second)
	shift_start_zero_day := shift_start.Add(-start_hour_duration)

	hour, minute, second := GetTimeOfDayFromString(shift["start_time_of_day"].(string))
	time_seconds_duration := GetTimeOfDayDuration(hour, minute, second)
	shift_start_zero := shift_start_zero_day.Add(time_seconds_duration)

	return shift_start_zero
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
	time_seconds_duration := time.Duration(time_seconds * 1000000000)

	return time_seconds_duration
}