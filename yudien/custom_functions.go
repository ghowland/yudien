package yudien

import (
	"database/sql"
	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudiendata"
	. "github.com/ghowland/yudien/yudienutil"
)

func UDN_Customer_PopulateScheduleDutyResponsibility(db *sql.DB, udn_schema map[string]interface{}, udn_start *UdnPart, args []interface{}, input interface{}, udn_data map[string]interface{}) UdnResult {
	database := GetResult(args[0], type_string).(string)
	responsibility_id := GetResult(args[1], type_int).(int64)
	//start_populating := GetResult(args[2], type_string).(string)

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: %v", input)

	options := make(map[string]interface{})
	options["db"] = database
	responsibility := DatamanGet("duty_responsibility", int(responsibility_id), options)

	result := UdnResult{}
	result.Result = nil

	// If we didnt get an error
	if responsibility["_error"] != nil {
		UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Error getting responsibility: %v", responsibility["_error"])
		return result
	}

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Responsibility: %v", responsibility)

	UdnLogLevel(udn_schema, log_debug, "CUSTOM: Populate Schedule: Duty Responsibility: Result: %v", result.Result)

	return result
}