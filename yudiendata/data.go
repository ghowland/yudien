package yudiendata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	. "github.com/ghowland/yudien/yudiencore"
	. "github.com/ghowland/yudien/yudienutil"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/junhsieh/goexamples/fieldbinding/fieldbinding"
	"github.com/jacksontj/dataman/src/datamantype"
	"time"
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

const (
	time_format_db = "2006-01-02 15:04:05"
	time_format_go = "2006-01-02T15:04:05"
	time_format_date = "2006-01-02"
)

type DatabaseConfig struct {
	Name string `json:"name"`
	Database string `json:"database"`
	Schema string `json:"schema"`
	User string `json:"user"`
	Password string `json:"password"`
	Host string `json:"host"`
	ConnectOptionsTemplate string `json:"connect_template"`
	ConnectOptions string `json:"connect_opts"`
}

var DefaultDatabase *DatabaseConfig
var AllDatabaseConfig = map[string]DatabaseConfig{}


var DatasourceInstance = map[string]*storagenode.DatasourceInstance{}
var DatasourceConfig = map[string]*storagenode.DatasourceInstanceConfig{}
var DatasourceDatabase = map[string]string{}

var DefaultDatabaseTarget string

// Maps database names to their Datasource Instance
var DatabaseToDatasourceInstance = map[string]*storagenode.DatasourceInstance{}


func Query(db *sql.DB, sql string) []map[string]interface{} {
	UdnLogLevel(nil, log_debug,"Query: %s\n", sql)

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
	outArr := []map[string]interface{}{}

	for rs.Next() {
		if err := rs.Scan(fb.GetFieldPtrArr()...); err != nil {
			log.Panic(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
		}

		template_map := make(map[string]interface{})

		for key, value := range fb.GetFieldArr() {
			//fmt.Printf("Found value: %s = %s\n", key, value)

			switch value.(type) {
			case []byte:
				template_map[key] = fmt.Sprintf("%s", value)
			default:
				template_map[key] = value
			}
		}

		outArr = append(outArr, template_map)
	}

	if err := rs.Err(); err != nil {
		log.Panic(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}

	return outArr
}

// Returns a DatasourceInstance.  If name is "" or not found, it starts with the lowest DB and finds the first collection/table that matches
func GetDatasourceInstance(options map[string]interface{}) (*storagenode.DatasourceInstance, string, string) {
	datasource_instance := DatasourceInstance["_default"]
	datasource_database := DatasourceDatabase["_default"]

	selected_db := "_default"

	// If there is a specified option to select an explicit Database
	if options["db"] != nil {
		if DatasourceInstance[options["db"].(string)] != nil {
			datasource_instance = DatasourceInstance[options["db"].(string)]
			datasource_database = DatasourceDatabase[options["db"].(string)]

			selected_db = options["db"].(string)
		}
	}

	//UdnLogLevel(nil, log_trace, "Data Source Connection: %s\n", selected_db)

	return datasource_instance, datasource_database, selected_db
}

func GetRecordLabel(datasource_database string, collection_name string, record_id int) string {
	record_label := fmt.Sprintf("%s.%s.%d", datasource_database, collection_name, record_id)

	return record_label
}

func AddJoinAsFlatNamespace(record map[string]interface{}, join_array []interface{}) {
	//TODO(g): Handle multiple depths of joins.  Currently I dont check if they are dotted for more depth in joins...
	for _, join_name := range join_array {
		join_string := join_name.(string)

		if record[join_string] != nil {
			join_record := record[join_string].(map[string]interface{})

			for field_name, value := range join_record {
				// Use dots to reach joined data in flat namespace
				field_key := fmt.Sprintf("%s.%s", join_name, field_name)
				record[field_key] = value

				// Use double-underscore too, sometimes we cant use dots
				field_key = fmt.Sprintf("%s__%s", join_name, field_name)
				record[field_key] = value

				//UdnLogLevel(nil, log_trace, "AddJoinAsFlatNamespace: %s: %v\n", field_key, value)
			}
		}
	}
}

func DatamanGetByLabel(record_label string) map[string]interface{} {
	UdnLogLevel(nil, log_debug, "Dataman GET By Label: %s\n", record_label)

	parts := strings.Split(record_label, ".")

	database := parts[0]
	table := parts[1]
	record_pkey := GetResult(parts[2], type_int).(int64)

	options := make(map[string]interface{})
	options["db"] = database

	record := DatamanGet(table, int(record_pkey), options)

	return record
}

func DatamanSetByLabel(record_label string, record map[string]interface{}) map[string]interface{} {
	UdnLogLevel(nil, log_debug, "Dataman SET By Label: %s: %v\n", record_label, record)

	parts := strings.Split(record_label, ".")

	database := parts[0]
	table := parts[1]

	options := make(map[string]interface{})
	options["db"] = database

	record_result := DatamanSet(table, record, options)

	return record_result
}

func DatamanGet(collection_name string, record_id int, options map[string]interface{}) map[string]interface{} {
	//fmt.Printf("DatamanGet: %s: %d\n", collection_name, record_id)

	datasource_instance, datasource_database, selected_db := GetDatasourceInstance(options)

	get_map := map[string]interface{} {
		"db":             datasource_database,
		"shard_instance": "public",
		"collection":     collection_name,
		//"_id":            record_id,
		"pkey": map[string]interface{}{"_id": record_id},
		"join": options["join"],
	}

	UdnLogLevel(nil, log_debug, "Dataman Get: %s: %v\n\n", datasource_database, get_map)

	dataman_query := &query.Query{query.Get, get_map}

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman GET: %s: ERRORS: %v\n", datasource_database, result.Error)
	}

	UdnLogLevel(nil, log_debug, "Dataman GET: %s: %v\n", datasource_database, result.Return[0])

	record := result.Return[0]
	if record != nil {
		record["_record_label"] = GetRecordLabel(selected_db, collection_name, record_id)

		// If we have an Active Tombstone record, and we arent ignoring it
		if record["_is_deleted"] != nil && options["ignore_tombstones"] != nil && record["_is_deleted"] == true && options["ignore_tombstones"] != true {
			record["_error"] = "This record has been deleted with a Tombstone: _is_deleted = true"
			UdnLogLevel(nil, log_error, "Dataman GET: %s: ERRORS: %s\n", record["_record_label"], record["_error"])
		}
	}

	// Add all the joined fields as a flat namespace to the original table
	if options["join"] != nil {
		AddJoinAsFlatNamespace(record, options["join"].([]interface{}))
	}

	return record
}

func DatamanSet(collection_name string, record map[string]interface{}, options map[string]interface{}) map[string]interface{} {
	UdnLogLevel(nil, log_trace, "Dataman SET: %s: %v\n", collection_name, record)

	// Duplicate this map, because we are messing with a live map, that we dont expect to change in this function...
	//TODO(g):REMOVE: Once I dont need to manipulate the map in this function anymore...
	record = MapCopy(record)

	datasource_instance, datasource_database, selected_db := GetDatasourceInstance(options)

	// Remove the _id field, if it is nil.  This means it should be new/insert
	if record["_id"] == nil || record["_id"] == "<nil>" || record["_id"] == "\u003cnil\u003e" || record["_id"] == "" {
		//fmt.Printf("DatamanSet: Removing _id key: %s\n", record["_id"])
		delete(record, "_id")
	} else {
		// If this a new record (negative ID), clear the negative value out so it is not submitted
		record_id := GetResult(record["_id"], type_int).(int64)

		if record_id < 0 {
			UdnLogLevel(nil, log_trace, "DatamanSet: New Record: Removing _id: %d\n", record_id)
			delete(record, "_id")
		} else {
			//UdnLogLevel(nil, log_trace, "DatamanSet: Not Removing _id: %s\n", record["_id"])
		}
	}

	// Delete the defaults base64 encoded map, it is never part of a record, it is to ensure we keep our defaults
	if record["_defaults"] != nil {
		delete(record, "_defaults")
	}

	// Delete the record label
	if record["_record_label"] != nil {
		delete(record, "_record_label")
	}


	// Put the deep fields into their containers
	for k, v := range record {
		if strings.Contains(k, "__") {
			parts := strings.Split(k, "__")
			part_array := GetResult(parts, type_array).([]interface{})

			// Update the record
			UdnLogLevel(nil, log_trace, "DatamanSet: %s: %s: %v\n", collection_name, k, part_array)
			Direct_MapSet(part_array, v, record)
		}
	}

	// Fix data manually, for now
	for k, v := range record {
		if v == "true" {
			record[k] = true
		} else if v == "false" {
			record[k] = false
		}

		//TODO(g): I shouldnt have to hard-code this, but just building the editors now, so I will need to find a better solution than this.  Can do it by schema_table_field information, which would be fine to assert types there dynamically
		if strings.Contains(k, "data_json") {
			switch v.(type) {
			case string:
				valid_map, err := JsonLoadMap(v.(string))

				if err == nil {
					record[k] = valid_map
				} else {
					valid_array, err := JsonLoadArray(v.(string))
					if err == nil {
						record[k] = valid_array
					}
				}
			}
		}

		// If we have any different trimming the key, then trim and remove the old one.  Spaces are not allowed
		trimmed_key := strings.TrimSpace(k)
		if k != trimmed_key {
			record[trimmed_key] = record[k]
			delete(record, k)
		}
	}


	// Fixup the record, if its not a new one, by getti
	// ng any values
	//TODO(g):REMOVE: This is fixing up implementation problems in Dataman, which Thomas will fix...
	if record["_id"] != nil && record["_id"] != "" {
		UdnLogLevel(nil, log_error, "Ensuring all fields are present (HACK): %s: %v\n", collection_name, record["_id"])

		// Record ID will be an integer
		var record_id int64
		var err interface{}
		switch record["_id"].(type) {
		case string:
			record_id, err = strconv.ParseInt(record["_id"].(string), 10, 32)
			if err != nil {
				UdnLogLevel(nil, log_error,"Record _id is not an integer: %s: %v\n", collection_name, record)
				delete(record, "_id")
			}
		default:
			UdnLogLevel(nil, log_error,"Record _id type: %T\n", record["_id"])
			record_id = GetResult(record["_id"], type_int).(int64)
		}

		record["_id"] = record_id

		UdnLogLevel(nil, log_trace,"record_id type: %T\n", record_id)

		get_options := make(map[string]interface{})
		get_options["db"] = options["db"]

		record_current := DatamanGet(collection_name, int(record_id), get_options)

		//// Set all the fields we have in the existing record, into our new record, if they dont exist, which defeats Thomas' current bug not allowing me to save data unless all fields are present
		//for k, v := range record_current {
		//	if record[k] == nil {
		//		record[k] = v
		//		fmt.Printf("Adding field: %s: %s: %v\n", collection_name, k, v)
		//	}
		//}

		// Remove any fields that arent present in the record_current
		for k, _ := range record {
			if _, has_key := record_current[k]; !has_key {
				UdnLogLevel(nil, log_debug, "Removing field: %s: %s: %v\n", collection_name, k, record[k])
				delete(record, k)
			}
		}
	} else {
		// This is a new record, we just tested for it above, remove empty string _id, if it exists
		if _, ok := record["_id"]; ok {
			UdnLogLevel(nil, log_debug, "Removing _id: %s: %v\n", collection_name, record)
			delete(record, "_id")
		}
	}

	// Remove fields I know I put in here, that I dont want to go in
	//TODO(g):REMOVE: Same as the others
	delete(record, "_table")
	delete(record, "_web_data_widget_instance_id")

	var dataman_query *query.Query

	// Insert if this doesnt have the _id PKEY, otherwise SET (update)
	if record["_id"] == nil {
		// Form the Dataman query -- INSERT
		dataman_query = &query.Query{
			query.Insert,
			map[string]interface{} {
				"db":             datasource_database,
				"shard_instance": "public",
				"collection":     collection_name,
				"record":         record,
			},
		}

	} else {
		// Form the Dataman query -- SET
		dataman_query = &query.Query{
			query.Set,
			map[string]interface{} {
				"db":             datasource_database,
				"shard_instance": "public",
				"collection":     collection_name,
				"record":         record,
			},
		}
	}

	//UdnLogLevel(nil, log_debug,"Dataman SET: Record: %v\n", record)
	UdnLogLevel(nil, log_trace, "Dataman SET: Query: JSON: %v\n", JsonDump(dataman_query))

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)


	if result.ValidationError != nil {
		UdnLogLevel(nil, log_error, "Dataman SET: Validation ERROR: %s\n", JsonDump(result.ValidationError))
	}


	//result_bytes, _ := json.Marshal(result)
	//UdnLogLevel(nil, log_trace, "Dataman SET: %s\n", result_bytes)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman SET: ERROR: %v\n", result.Error)
	}

	if result.Return != nil {
		record := result.Return[0]
		record["_record_label"] = GetRecordLabel(selected_db, collection_name, int(record["_id"].(int64)))

		UdnLogLevel(nil, log_trace, "Dataman SET: Result Record: JSON: %v\n", record)

		return record
	} else {
		UdnLogLevel(nil, log_trace, "Dataman SET: Failed Result: nil: %v\n", result.Error)
		record := make(map[string]interface{})
		record["_error"] = result.ValidationError
		return record
	}
}


func DatamanInsert(collection_name string, record map[string]interface{}, options map[string]interface{}) map[string]interface{} {
	UdnLogLevel(nil, log_trace, "Dataman INSERT: %s: %v\n", collection_name, record)

	// Duplicate this map, because we are messing with a live map, that we dont expect to change in this function...
	//TODO(g):REMOVE: Once I dont need to manipulate the map in this function anymore...
	record = MapCopy(record)

	datasource_instance, datasource_database, selected_db := GetDatasourceInstance(options)

	// Remove fields I know I put in here, that I dont want to go in
	//TODO(g):REMOVE: Same as the others
	delete(record, "_table")
	delete(record, "_web_data_widget_instance_id")

	// Form the Dataman query
	dataman_query := &query.Query{
		query.Insert,
		map[string]interface{} {
			"db":             datasource_database,
			"shard_instance": "public",
			"collection":     collection_name,
			"record":         record,
		},
	}

	//UdnLogLevel(nil, log_debug,"Dataman SET: Record: %v\n", record)
	UdnLogLevel(nil, log_trace, "Dataman INSERT: Query: JSON: %v\n", JsonDump(dataman_query))

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)


	if result.ValidationError != nil {
		UdnLogLevel(nil, log_error, "Dataman INSERT: Validation ERROR: %s\n", JsonDump(result.ValidationError))
	}


	//result_bytes, _ := json.Marshal(result)
	//UdnLogLevel(nil, log_trace, "Dataman SET: %s\n", result_bytes)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman INSERT: ERROR: %v\n", result.Error)
	}

	if result.Return != nil {
		record := result.Return[0]
		record["_record_label"] = GetRecordLabel(selected_db, collection_name, int(record["_id"].(int64)))

		UdnLogLevel(nil, log_trace, "Dataman INSERT: Result Record: JSON: %v\n", record)

		return record
	} else {
		UdnLogLevel(nil, log_trace, "Dataman INSERT: Failed Result: nil: %v\n", result.Error)
		record := make(map[string]interface{})
		record["_error"] = result.ValidationError
		return record
	}
}

func DatamanFilter(collection_name string, filter_input_map map[string]interface{}, options map[string]interface{}) []map[string]interface{} {

	//fmt.Printf("DatamanFilter: %s:  Filter: %v  Join: %v\n\n", collection_name, filter, options["join"])
	//fmt.Printf("Sort: %v\n", options["sort"])		//TODO(g): Sorting

	datasource_instance, datasource_database, selected_db := GetDatasourceInstance(options)

	filter_input_map = MapCopy(filter_input_map)

	for k, v := range filter_input_map {
		switch v.(type) {
		case string:
			filter_input_map[k] = []string{"=", v.(string)}
		case int64:
			filter_input_map[k] = []string{"=", fmt.Sprintf("%d", v)}
		}
	}

	var filter interface{}
	filter = filter_input_map

	// If we were given Time Range options to filter on, handle these
	if options["time_range"] != nil && options["time_range_fields"] != nil {
		// Make a full filter out of this, as we need multiple of the same fields to AND together, so incorporate the filter_map we got as an arg
		filter_full := make([]interface{}, 0)
		filter_full = append(filter_full, filter_input_map)

		UdnLogLevel(nil, log_trace, "Dataman Filter: Time Range 001: %s\n\n", JsonDump(filter_full))

		//TODO(g): Handle errors from parsing
		time_range_part := strings.Split(options["time_range"].(string), " - ")
		time_range_start, _ := time.Parse(time_format_db, time_range_part[0])
		time_range_stop, _ := time.Parse(time_format_db, time_range_part[1])

		for _, field := range options["time_range_fields"].([]interface{}) {
			field_str := field.(string)

			filter_item_start := map[string]interface{}{
				field_str: []interface{}{">=", time_range_start},
			}
			filter_item_stop := map[string]interface{}{
				field_str: []interface{}{"<", time_range_stop},
			}

			filter_item := []interface{}{filter_item_start, "AND", filter_item_stop}

			// Join this item and previous together logically with AND
			filter_full = append(filter_full, "AND")
			filter_full = append(filter_full, filter_item)
		}

		// Update the filter to the full filter we made here
		filter = filter_full
	}

	filter_map := map[string]interface{} {
		"db":             datasource_database,
		"shard_instance": "public",
		"collection":     collection_name,
		"filter":         filter,
		"join":           options["join"],
		"sort":           options["sort"],
		"limit":           options["limit"],
		//"sort_reverse":	  []bool{true},
	}

	//fmt.Printf("Dataman Filter: %s\n\n", JsonDump(filter_map))
	//fmt.Printf("Dataman Filter Map Filter: %s\n\n", SnippetData(filter_map["filter"], 120))
	//fmt.Printf("Dataman Filter Map Filter Array: %s\n\n", SnippetData(filter_map["filter"].(map[string]interface{})["name"], 120))
	UdnLogLevel(nil, log_trace, "Dataman Filter: %s\n\n", JsonDump(filter_map))
	//UdnLogLevel(nil, log_trace, "Dataman Filter Map Filter: %s\n\n", SnippetData(filter_map["filter"], 120))
	//UdnLogLevel(nil, log_trace, "Dataman Filter Map Filter Array: %s\n\n", SnippetData(filter_map["filter"].(map[string]interface{})["name"], 120))

	dataman_query := &query.Query{query.Filter, filter_map}


	result := datasource_instance.HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman ERROR: %v\n", result.Error)
	} else {
		//fmt.Printf("Dataman FILTER: %v\n", result.Return)
	}

	// Add all the joined fields as a flat namespace to the original table
	for _, record := range result.Return {
		field_id := "_id"
		if record[field_id] == nil {
			field_id = "id"
		}

		record["_record_label"] = GetRecordLabel(selected_db, collection_name, int(record[field_id].(int64)))
		if options["join"] != nil {
			AddJoinAsFlatNamespace(record, options["join"].([]interface{}))
		}
	}

	// Filter any Tombstone Deleted records
	final_record_array := make([]map[string]interface{}, 0)

	for _, record := range result.Return {
		// Ensure we remove any records with _is_deleted==true unless options.ignore_tombstones==true
		// If we have an Active Tombstone record, and we arent ignoring it
		if record["_is_deleted"] != nil && record["_is_deleted"] == true {
			if  options["ignore_tombstones"] != nil && options["ignore_tombstones"] == true {
				final_record_array = append(final_record_array, record)
			} else {
				// Not adding this record to final_record_array, because it was deleted by tombstone, and we arent ignoring that with options flag
			}
		} else {
			// This wasnt removed by Tombstone, so add it to our final results
			final_record_array = append(final_record_array, record)
		}
	}

	return final_record_array
}

func DatamanFilterFull(collection_name string, filter interface{}, options map[string]interface{}) []map[string]interface{} {
	// Contains updated functionality of DatamanFilter where multiple constraints can be used as per dataman specs

	datasource_instance, datasource_database, selected_db := GetDatasourceInstance(options)

	// filter should be a map[string]interface{} for single filters and []interface{} for multi-filters
	// dataman handles all cases so it is fine for filter to be interface{}
	// ex: single filter:
	//     {field1=value1}  (type: map[string]interface{})
	// ex: multi filter:
	//     [{field1=value1}, "AND", {field2=value2}] (type: []interface{})
	//     [{field1=value1}, "AND", [{field2=value2}, "AND", {field3=value3}]]
	UdnLogLevel(nil, log_debug, "DatamanFilter: %s:  Filter: %v  Join: %v\n\n", collection_name, filter, options["join"])
	//fmt.Printf("Sort: %v\n", options["sort"])		//TODO(g): Sorting

	filter_map := map[string]interface{} {
		"db":             datasource_database,
		"shard_instance": "public",
		"collection":     collection_name,
		"filter":         filter,
		"join":           options["join"],
		"sort":           options["sort"],
		//"sort_reverse":	  []bool{true},
	}

	UdnLogLevel(nil, log_debug, "Dataman Filter: %v\n\n", filter_map)
	UdnLogLevel(nil, log_debug, "Dataman Filter Map Filter: %s\n\n", SnippetData(filter_map["filter"], 120))


	dataman_query := &query.Query{query.Filter, filter_map}

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman ERROR: %v\n", result.Error)
	} else {
		UdnLogLevel(nil, log_debug, "Dataman FILTER: %v\n", result.Return)
	}


	// Add all the joined fields as a flat namespace to the original table
	for _, record := range result.Return {
		record["_record_label"] = GetRecordLabel(selected_db, collection_name, int(record["_id"].(int64)))
		if options["join"] != nil {
			AddJoinAsFlatNamespace(record, options["join"].([]interface{}))
		}
	}

	// Filter any Tombstone Deleted records
	final_record_array := make([]map[string]interface{}, 0)

	for _, record := range result.Return {
		// Ensure we remove any records with _is_deleted==true unless options.ignore_tombstones==true
		// If we have an Active Tombstone record, and we arent ignoring it
		if record["_is_deleted"] != nil && record["_is_deleted"] == true {
			if  options["ignore_tombstones"] != nil && options["ignore_tombstones"] == true {
				final_record_array = append(final_record_array, record)
			} else {
				// Not adding this record to final_record_array, because it was deleted by tombstone, and we arent ignoring that with options flag
			}
		} else {
			// This wasnt removed by Tombstone, so add it to our final results
			final_record_array = append(final_record_array, record)
		}
	}

	return final_record_array
}

func DatamanFormat(filter_string string, args ...interface{}) interface{} {
	// Given a json formatted string (similar to what is used in __data_filter, decode it into a dataman compatible structure)
	filter_string = strings.Replace(filter_string, "'", "\"", -1)
	filter_string = fmt.Sprintf(filter_string, args...)

	filter_string_bytes := []byte(filter_string)

	var filter interface{}

	if err := json.Unmarshal(filter_string_bytes, &filter); err != nil {
		log.Panic(err)
	}

	return filter
}

func DatamanDelete(collection_name string, record_id int64, options map[string]interface{}) map[string]interface{} {
	// The delete function looks at the table schema_table_delete_dependency to determine
	// Whether to delete or NULL FK dependencies on the deleted entry (recursively)
	UdnLogLevel(nil, log_trace, "Delete entry: collection name: %v, record_id: %v, options: %v\n", collection_name, record_id, options)

	_, datasource_database, _ := GetDatasourceInstance(options)

	var record map[string]interface{}

	// Find the schema_id
	schema_filter := DatamanFormat("{'name':['=', '%s']}", datasource_database)
	schema_result := DatamanFilterFull("schema", schema_filter, make(map[string]interface{}))
	var schema_id int64

	if len(schema_result) > 0 {
		schema_id = schema_result[0]["_id"].(int64)
	} else {
		UdnLogLevel(nil, log_error, "Cannot find schema in database for delete. Aborting delete.\n")
		return record
	}

	// Find the table_id
	table_filter := DatamanFormat("[{'name':['=', '%s']}, 'AND', {'schema_id':['=', '%d']}]", collection_name, schema_id)
	table_result := DatamanFilterFull("schema_table", table_filter, make(map[string]interface{}))
	var table_id int64

	if len(table_result) > 0 {
		table_id = table_result[0]["_id"].(int64)
	} else {
		UdnLogLevel(nil, log_error, "Cannot find table_id in schema_table for delete. Aborting delete.\n")
		return record
	}

	// For the given entry, check if there are any dependencies
	dependency_list := make([]map[string]interface{}, 0, 10)
	FindDeleteDependency(schema_id, table_id, record_id, &dependency_list)

	UdnLogLevel(nil, log_debug, "\nDependency List: %v\n\n", dependency_list)

	if len(dependency_list) > 0 {
		// Go through the list of dependencies backwards and perform NULL or delete as needed
		for i := len(dependency_list) - 1; i >= 0; i-- {
			dependent_table_name := dependency_list[i]["table_name"].(string)

			if (dependency_list[i]["delete"].(bool)) {
				// Delete the dependent entry
				DatamanDeleteRaw(dependent_table_name, dependency_list[i]["record_id"].(int64), make(map[string]interface{}))
			} else {
				// NULL the entry's FK reference
				dependent_table_field_name := dependency_list[i]["table_field_name"].(string)
				dependent_record_id := dependency_list[i]["record_id"].(int64)

				DatamanNullRaw(dependent_table_name, dependent_table_field_name, dependent_record_id, make(map[string]interface{}))
			}
		}
	}

	// After resolving all dependencies, delete the current entry
	//TODO(z):Return all deleted/NULLed dependencies instead of only the specified one (needed?)
	record = DatamanDeleteRaw(collection_name, record_id,  options)
	return record
}

//func DatamanDeleteFilter(collection_name string, filter interface{}, options map[string]interface{}) []map[string]interface{} {
// Add function when necessary - currently UDN_DataDeleteFilter runs DatamanDelete on each entry in the filtered list
//}

func FindDeleteDependency(schema_id int64, schema_table_id int64, record_id int64, dependency_list *[]map[string]interface{}) {
	// Dependencies are formatted as a array of map[string]interface{}
	// Fields in the map
	// "schema_name" - string
	// "table_name" - string
	// "table_field_name" - string
	// "record_id" - int64/pkey
	// "delete" - bool (True = delete, False = NULL)

	// Recursively find dependencies and add them to dependency_list

	// Find the _id for schema_table_field PK
	table_field_filter := DatamanFormat("[{'schema_table_id':['=', '%d']}, 'AND', {'name': ['=', '_id']}]", schema_table_id)
	table_field_result := DatamanFilterFull("schema_table_field", table_field_filter, make(map[string]interface{}))
	var table_field_id int64

	// If the field exists, look for dependent fields
	if len(table_field_result) > 0 {
		table_field_id = table_field_result[0]["_id"].(int64)

		// Find all dependent fields from foreign_key_schema_table_field_id
		dependent_table_field_filter := DatamanFormat("{'foreign_key_schema_table_field_id':['=', '%d']}", table_field_id)
		dependent_table_field_result := DatamanFilterFull("schema_table_field", dependent_table_field_filter, make(map[string]interface{}))

		// For each dependent field - find the dependent table, look for dependent entries, and add them to the dependency list
		for _, dependent_table_field := range dependent_table_field_result {
			dependent_table_id := dependent_table_field["schema_table_id"].(int64)
			dependent_table_field_name := dependent_table_field["name"].(string)
			dependent_entry_name := dependent_table_field["name"]

			dependent_table_filter := DatamanFormat("{'_id':['=', '%d']}", dependent_table_id)
			dependent_table_result := DatamanFilterFull("schema_table", dependent_table_filter, make(map[string]interface{}))
			dependent_table_name := ""

			if len(dependent_table_result) > 0 {
				dependent_table_name = dependent_table_result[0]["name"].(string)
			}

			// Look in the dependent table and look for dependent entries that need to be NULL/deleted
			dependent_entry_filter := DatamanFormat("{'%s':['=', '%d']}", dependent_entry_name, record_id)
			dependent_entry_result := DatamanFilterFull(dependent_table_name, dependent_entry_filter, make(map[string]interface{}))

			// Check the delete_dependency table for the action to be performed on the dependent entries (NULL/deleted)
			delete_dependency_filter := DatamanFormat("[{'delete_schema_table_field_id':['=', '%d']}, 'AND', {'schema_table_id':['=','%d']}]", table_field_id, dependent_table_id)
			delete_dependency_result := DatamanFilterFull("schema_table_delete_dependency", delete_dependency_filter, make(map[string]interface{}))

			// Set the flag for either delete or NULL dependent entries
			delete := len(delete_dependency_result) > 0

			// Find the schema name
			schema_filter := DatamanFormat("{'_id':['=', '%d']}", schema_id)
			schema_result := DatamanFilterFull("schema", schema_filter, make(map[string]interface{}))
			schema_name := ""

			if len(schema_result) > 0 {
				schema_name = schema_result[0]["name"].(string)
			}

			// Used so that only one call to IsUniqueDependency is needed as we need to make a second pass for the recursion
			seen_record_id := make(map[int64]bool)

			// Go through each entry and store it in the dependency list (if it is uniqeu)
			for _, dependent_entry := range dependent_entry_result {
				// Check if the entry is unique

				// Add each dependent entry to the dependency list
				dependency := make(map[string]interface{})
				dependency["schema_name"] = schema_name
				dependency["table_name"] = dependent_table_name
				dependency["record_id"] = dependent_entry["_id"]
				dependency["table_field_name"] = dependent_table_field_name
				dependency["delete"] = delete

				if IsUniqueDependency(dependency, *dependency_list) {
					*dependency_list = AppendArrayMap(*dependency_list, dependency)
					seen_record_id[dependency["record_id"].(int64)] = false
				} else {
					seen_record_id[dependency["record_id"].(int64)] = true
				}
			}

			// if dependent entries marked for delete, recursively call FindDeleteDependency on each dependent entry
			if delete {
				for _, dependent_entry := range dependent_entry_result {
					dependent_record_id := dependent_entry["_id"].(int64)

					if !seen_record_id[dependent_record_id] {
						FindDeleteDependency(schema_id, dependent_table_id, dependent_record_id, dependency_list)
					}
				}
			}
		}
	}
}

func DatamanDeleteRaw(collection_name string, record_id int64, options map[string]interface{}) map[string]interface{}{
	// Used for deleting the a single entry with no other FK dependent on the deleted entry
	// If there are other FK(s) dependent on the deleted entry, please use DatamanDelete
	datasource_instance, datasource_database, selected_db := GetDatasourceInstance(options)

	delete_map := map[string]interface{} {
		"db":             datasource_database,
		"shard_instance": "public",
		"collection":     collection_name,
		"pkey": map[string]interface{}{"_id": record_id},
	}

	UdnLogLevel(nil, log_debug, "Dataman Delete: %s: %v\n\n", datasource_database, delete_map)

	dataman_query := &query.Query{query.Delete, delete_map}

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)

	record := make(map[string]interface{})

	if result.Error == "" {
		UdnLogLevel(nil, log_debug, "Dataman Delete: %s: %v\n", datasource_database, result.Return[0])

		record = result.Return[0]

		if record != nil {
			record["__record_label"] = GetRecordLabel(selected_db, collection_name, int(record_id))
		}
	} else {
		// Send the error & msg back to the UDN caller
		record["error"] = "Dataman Delete: Could not delete entry"
		record["error_message"] = result.Error
		UdnLogLevel(nil, log_error, "Dataman Delete: %s: ERRORS: %v\n", datasource_database, result.Error)
	}

	return record
}

func DatamanNullRaw(collection_name string, field_name string, record_id int64, options map[string]interface{}) map[string]interface{}{
	// Used for NULLing the a single entry with a FK dependency
	record := DatamanFormat("{'_id': '%d', '%s': 'null'}", record_id, field_name)

	record.(map[string]interface{})[field_name] = nil
	result_map := DatamanSet(collection_name, record.(map[string]interface{}), options)

	UdnLogLevel(nil, log_trace,"Set dependent entry to null: %v\n", result_map)

	return result_map
}

func IsUniqueDependency(dependency map[string]interface{}, dependency_list []map[string]interface{}) bool{
	// Check if dependency exists in dependency_list
	// TODO(z): make more efficient - currently O(n) time where n = number of items in dependency_list - however there are not many items in most cases
	// ^ think of a way to create maps for O(1) avg lookup time?
	for _, cur_dependency := range dependency_list {
		if cur_dependency["schema_name"] == dependency["schema_name"] &&
			cur_dependency["table_name"] == dependency["table_name"] &&
			cur_dependency["table_field_name"] == dependency["table_field_name"] &&
			cur_dependency["record_id"] == dependency["record_id"] {
			return false
		}
	}

	return true
}

/*

Redo DatamanEnsureDatabases to use https://github.com/jacksontj/dataman/blob/master/src/storage_node/datasource/interface.go:

Provision States:

-3 = Deleted from Real Schema.  Needs to be removed from the Database (purge/obliterate) -- Change Management pre-removal phase
-2 = In process of Deletion
-1 = Tagged for Deletion
NULL = Unknown, test for existence and update.  Assume it is desired to be created if it doesnt exist.
0 = In Process of Provisioning
1 = Provisioned

 */


func DatamanEnsureDatabases(database_config DatabaseConfig, new_path interface{}) {
	UdnLogLevel(nil, log_info, "Dataman Ensure Database: Database Config: %v\n\n", database_config)

	// Get the Hard coded OpsDB record from the database `schema` table
	option_map := make(map[string]interface{})
	filter_map := make(map[string]interface{})
	filter_map["name"] = database_config.Name
	schema_result := DatamanFilter("schema", filter_map, option_map)
	schema_map := schema_result[0]

	UdnLogLevel(nil, log_info, "Schema: %v\n\n", schema_map)


	datasource := DatasourceInstance[database_config.Name].StoreSchema

	//TODO(g): Remove when we have the Dataman capability to ALTER tables to set PKEYs
	UdnLogLevel(nil, log_info, "Direct Database Connect: \"%s\"\n\n", database_config.ConnectOptions)
	db, err := sql.Open("postgres", database_config.ConnectOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//// Make sure we see all the DBs in the Instance
	//db_list := datasource.ListDatabase(context.Background())
	//UdnLogLevel(nil, log_info, "Schema DB List: %s\n\n", JsonDump(db_list))



	//UdnLogLevel(nil, log_info, "\nList DB Start: %v\n\n", time.Now().String())
	//db_list := datasource.ListDatabase(context.Background())
	//UdnLogLevel(nil, log_info, "\nList DB Stop: %v\n\n", time.Now().String())

	UdnLogLevel(nil, log_info, "Get Database: %s (%s)\n\n", database_config.Name, database_config.Database)
	db_item := datasource.GetDatabase(context.Background(), database_config.Database)

	UdnLogLevel(nil, log_info, "\n\nFound DB Item: %v\n\n", db_item.Name)
	UdnLogLevel(nil, log_info, "\n\nFound DB Total: %v\n\n", JsonDump(db_item))

	//shard_instance := datasource.ListShardInstance(context.Background(), db_item.Name)


	//TODO(g): Make an empty list of the collections, so we can track what we have, to find missing ones
	collection_array := make([]string, 0)
	missing_collection_array := make([]string, 0)


	//fmt.Printf("\n\nFound DB Shard Instance: %v -- %v\n\n", shard_instance, db_item.ShardInstances["public"])

	//collections := datasource.ListCollection(context.Background(), db_item.Name, "public")

	for _, collection := range db_item.ShardInstances["public"].Collections {
		UdnLogLevel(nil, log_info, "\n\n%s: Found DB Collections: %s\n", db_item.Name, collection.Name)

		option_map := make(map[string]interface{})
		filter_map := make(map[string]interface{})
		filter_map["name"] = collection.Name
		filter_map["schema_id"] = schema_map["_id"]
		collection_result := DatamanFilter("schema_table", filter_map, option_map)

		collection_map := make(map[string]interface{})
		if len(collection_result) > 0 {
			collection_map = collection_result[0]
			UdnLogLevel(nil, log_info, "%s: Collection: %v\n\n", db_item.Name, collection_map)

			// Add collection to our tracking array
			collection_array = append(collection_array, collection.Name)


			//TODO(g): Make an empty list of the collection fields, so we can track what we have, to find missing ones
			collection_field_array := make([]string, 0)

			// Check: Index, Primary Index

			for _, field := range collection.Fields {
				UdnLogLevel(nil, log_info, "\n\n%s: Found DB Collections: %s  Field: %s  Type: %s   (Default: %v -- Not Null: %v -- Relation: %v)\n", db_item.Name, collection.Name, field.Name, field.FieldType.Name, field.Default, field.NotNull, field.Relation)

				// Check: Not Null, Relation, Default, Type

				option_map := make(map[string]interface{})
				filter_map := make(map[string]interface{})
				filter_map["name"] = field.Name
				filter_map["schema_table_id"] = collection_map["_id"]
				collection_field_result := DatamanFilter("schema_table_field", filter_map, option_map)

				collection_field_map := make(map[string]interface{})
				if len(collection_field_result) > 0 {
					collection_field_map = collection_field_result[0]
					UdnLogLevel(nil, log_info, "%s: Collection Field: %v\n\n", db_item.Name, collection_field_map)

					// Add collection to our tracking array
					collection_field_array = append(collection_field_array, field.Name)


				} else {
					UdnLogLevel(nil, log_info, "%s: Collection Field: MISSING: %s\n\n", db_item.Name, field.Name)

				}
			}
		} else {
			UdnLogLevel(nil, log_info, "%s: Collection: MISSING: %s\n\n", db_item.Name, collection.Name)

			missing_collection_array = append(missing_collection_array, collection.Name)

			option_map := make(map[string]interface{})
			record_map := make(map[string]interface{})
			record_map["name"] = collection.Name
			record_map["schema_id"] = schema_map["_id"]
			record_map["data_json"] = JsonDump(collection)

			DatamanSet("schema_table", record_map, option_map)
		}
	}

	UdnLogLevel(nil, log_info, "%s: Missing Collections: %v\n\n", db_item.Name, missing_collection_array)


	// Loop over all collections and see if there are any we have in the `schema` table, that we dont have accounted for in the actual database
	option_map = make(map[string]interface{})
	filter_map = make(map[string]interface{})
	argument_type_result := DatamanFilter("argument_type", filter_map, option_map)
	argument_type_map := ConvertMapArrayToMap(argument_type_result, "_id")

	// Loop over all collections and see if there are any we have in the `schema` table, that we dont have accounted for in the actual database
	option_map = make(map[string]interface{})
	filter_map = make(map[string]interface{})
	all_collection_result := DatamanFilter("schema_table", filter_map, option_map)

	for _, collection_record := range all_collection_result {
		if ok, _ := InArray(collection_record["name"].(string), collection_array) ; !ok {
			UdnLogLevel(nil, log_info, "%s: Not Found collection: %s\n\n", db_item.Name, collection_record["name"])


			new_collection := metadata.Collection{}

			new_collection.Name = collection_record["name"].(string)
			new_collection.Fields = make(map[string]*metadata.CollectionField)

			// Get all the fields for this collection
			option_map := make(map[string]interface{})
			filter_map := make(map[string]interface{})
			filter_map["schema_table_id"] = collection_record["_id"]
			all_collection_field_result := DatamanFilter("schema_table_field", filter_map, option_map)


			// Create the new collection
			err := datasource.AddCollection(context.Background(), db_item, db_item.ShardInstances["public"], &new_collection)
			UdnLogLevel(nil, log_info, "%s: Add New Collection: %s: ERROR: %s\n\n", db_item.Name, new_collection.Name, err)

			for _, field_map := range all_collection_field_result {
				// Create fields we need to populate this table
				new_field := metadata.CollectionField{}
				new_field.Name = field_map["name"].(string)
				argument_type_map_key := fmt.Sprintf("%v", field_map["argument_type_id"])
				new_field.Type = argument_type_map[argument_type_map_key].(map[string]interface{})["schema_type_name"].(string)
				new_field.NotNull = !field_map["allow_null"].(bool)
				new_field.Default = field_map["default_value"]
				new_field.FieldType = &metadata.FieldType{}
				new_field.FieldType.Name = argument_type_map[argument_type_map_key].(map[string]interface{})["schema_type_name"].(string)
				new_field.FieldType.DatamanType = datamantype.DatamanType(new_field.FieldType.Name)

				new_collection.Fields[new_field.Name] = &new_field

				// Create the new collection field
				err := datasource.AddCollectionField(context.Background(), db_item, db_item.ShardInstances["public"], &new_collection, &new_field)
				UdnLogLevel(nil, log_info, "%s: Add New Collection Field: %s: %s: ERROR: %s\n\n", db_item.Name, new_collection.Name, new_field.Name, err)

				if field_map["is_primary_key"] == true {
					new_index := metadata.CollectionIndex{}

					new_index.Name = fmt.Sprintf("pkey_%s", new_field.Name)
					new_index.Primary = true
					new_index.Unique = true

					new_index.Fields = make([]string, 0)
					new_index.Fields = append(new_index.Fields, new_field.Name)

					// Create the new collection field index
//					err := datasource.AddCollectionIndex(context.Background(), db_item, db_item.ShardInstances["public"], &new_collection, &new_index)

					// Perform an ALTER table through SQL here, as dataman doesnt allow it
					//TODO(g)...
					//
					sql := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)", new_collection.Name, new_field.Name)
					Query(db, sql)
					UdnLogLevel(nil, log_info, "%s: Add New Collection Field PKEY: %s: %s: ERROR: %s\n\n", db_item.Name, new_collection.Name, new_index.Name, err)

				}

				//TODO: Foreign Key




			}


			//TODO: Indices

			//TODO: Sequences


		}
	}



	// Returns a JSON-styled map which contains all the data needed to enact this change, and is what we store in version_pending/version_commit
	//		We need to be able to do this comparison at 2 different times.
	//		Should I do this through 2 diffs of the data?  Then I just pull in the current data, and compare it?
	//		Yes, we need to be able to get data from 2 different databases.  So I will make the comparison work as it sending the data from one to the other.
	//			The same will have to be done with the data.
	//
	//		For data, we need to get all the data sometimes, but othertimes, we want partial data.  Maybe last 2 weeks, or only by users with X.
	//			Often this is based on how big the tables.  Small we take it all, big we want to dig.
	//
	//		Make this work as just sending the data packs across, either with everything, or with a sort.
	//
	//		If it's creating things with everything, it will have to respect FOREIGN KEY constraints, because otherwise we will corrupt the dataset.
	//		But the FOREIGN KEY we protect only needs to be the shorter tables, our config tables?  Some tables we dont protect:  LOGGING TABLES.  Something like that name.  Non-core data tables.  Non-essential data.  Non-critical.  Supporting data.
	//
	//		This way we dont purge existing data, so it doesnt break anything, but UPDATES existing, and adds new data we need for testing.  They can purge on their own, as they wish.
	//
	//		This way, can request the schema from other machines.  Can share your schema, and data information, and then others can request your data.  They can get your schema all the time.
	//
	//	Uploading data dumps, we would always have the data.  Should I just have a method of restoring from a dump, and having everyone's DBs in a centralized location this way?
	//
	//		It does make sense to just constantly sync their dumps, because then we already have the data to sync it.  A staging server becomes a "hub" for people to sync against, and keeps all the DBs for the backups and such.
	//
	//			In this way, we can also test everyone's code and features, because we have all their code, so we basically have an instance of their opsdb, available for testing.  Make it an N-OpsDB Hub.

	//				-- Does this system need a separate DB to track all these DBs?  How do we know about them, if we change the default?  Just copy the data from the old to the new!!! --


	/*
	for _, db_item := range db_list {
		if db_item.Name == limited_database_search {
			fmt.Printf("\n\nFound DB Item: %v\n\n", db_item.Name)

		}

	}*/



	//result_list := DatamanFilter(collection_name, filter, options)


	// Look through all our Schemas, can we connect.  Do they exist?  Skip some of this for now, OpsDB is enough.

	// Look through single schema, for all tables/collections.
	//		Any assertions on the Table level?  Sequences?  Indexes???????///////???

	// Look through all fields in all tables, ensure they exist or should be removed.  Check for Alter, new foreign keys, etc


	// The result of this should be a Change Management record, which can be applied, or replicated
	//		Run Apply Change Management pending record, to actually apply the changes, and save them.  Storing original data too.


	/*
	fmt.Printf("Starting ensure DB processs...\n")

	config := storagenode.DatasourceInstanceConfig{
		StorageNodeType: "postgres",
		StorageConfig: map[string]interface{}{
			"pg_string": pgconnect,
		},
	}

	DatabaseTarget = database

	configfile := "./data/schema.json"
	schema_str, err := ioutil.ReadFile(configfile)
	if err != nil {
		configfile = "/etc/web6/schema.json"
		schema_str, err = ioutil.ReadFile(configfile)
		if err != nil {
			panic(fmt.Sprintf("Load schema configuration data: %s: %s", configfile, err.Error()))
		}
	}

	//fmt.Printf("Schema STR: %s\n\n", schema_str)

	var meta metadata.Meta
	err = json.Unmarshal(schema_str, &meta)
	if err != nil {
		panic("Cannot parse JSON config data: " + err.Error())
	}

	fmt.Printf("Creating new data source instances...\n")


	//if datasource, err := storagenode.NewLocalDatasourceInstance(&config, &meta); err == nil {
	if datasource, err := storagenode.NewDatasourceInstanceDefault(&config); err == nil {
		DatasourceInstance["opsdb"] = datasource
		DatasourceConfig["opsdb"] = &config
	} else {
		panic("Cannot open primary database connection: " + err.Error())
	}


	//DatasourceInstance["opsdb"] = datasource
	//DatasourceConfig["opsdb"] = &config

	fmt.Printf("Importing current database info...\n")


	// Load meta
	meta2 := &metadata.Meta{}
	metaBytes, err := ioutil.ReadFile(current_path)
	if err != nil {
		panic(fmt.Sprintf("Error loading schema: %v", err))
	}
	err = json.Unmarshal([]byte(metaBytes), meta2)
	if err != nil {
		panic(fmt.Sprintf("Error loading meta: %v", err))
	}

	fmt.Printf("Creating the current DB as ensure...\n")

	for _, db := range meta.Databases {
		//name := db.Name	//TODO(g): Make the names dynamic, with the DBs too, need to make it all line up
		fmt.Printf("Ensuring DB: %s\n", db.Name)
		err := DatasourceInstance["opsdb"].MutableMetaStore.EnsureExistsDatabase(context.Background(), db)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("Creating the NEW DB as ensure...\n")

	// Load meta
	meta2 = &metadata.Meta{}
	metaBytes, err = ioutil.ReadFile(new_path)
	if err != nil {
		panic(fmt.Sprintf("Error loading schema: %v", err))
	}
	err = json.Unmarshal([]byte(metaBytes), meta2)
	if err != nil {
		panic(fmt.Sprintf("Error loading meta: %v", err))
	}

	for _, db := range meta.Databases {
		//name := db.Name	//TODO(g): Make the names dynamic, with the DBs too, need to make it all line up
		fmt.Printf("Ensuring DB: %s\n", db.Name)
		err := DatasourceInstance["opsdb"].EnsureExistsDatabase(context.Background(), db)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("Finished Creating the NEW DB as ensure...\n")
	*/



	/*
Geoffs-MacBook-Pro:web6.0 geoff$ cat ../../jacksontj/dataman/src//client/example_usage/datasourceinstance.yaml

storage_type: postgres
storage_config:
  pg_string: user=postgres password=password sslmode=disable
	 */

/*
	// Create the meta store
	metaStore, err := NewMetadataStore(config)
	if err != nil {
		return nil, err
	}
	return NewDatasourceInstance(config, metaStore)

	for _, db := range OLD_DBs {
		if err := metaStore.EnsureExistsDatabase(context.Background(), db); err != nil {
			panic(err)
		}
	}
*/
}

//func (s *DatasourceInstance) EnsureExistsDatabase(ctx context.Context, db *metadata.Database) error {


func SanitizeSQL(text string) string {
	text = strings.Replace(text, "'", "''", -1)

	return text
}

func InitDataman(database_config DatabaseConfig, databases map[string]DatabaseConfig) {
	configfile := database_config.Schema

	datasource, config, err := InitDatamanDatabase(database_config)
	if err != nil {
		panic(fmt.Sprintf("Load schema configuration data: %s: %s", configfile, err.Error()))
	}

	//TODO(g): Fix this, as this hardcodes everything to one.  Simple in the beginning, but maybe not useful now.  Maybe just the default?
	DefaultDatabaseTarget = database_config.Database

	// Add this DB as the _default, because it is our default
	DatasourceInstance["_default"] = datasource
	DatasourceConfig["_default"] = config
	DatasourceDatabase["_default"] = database_config.Database
	AllDatabaseConfig["_default"] = database_config

	// Also add this DB under it's own name, so that we can access it both ways
	DatasourceInstance[database_config.Name] = datasource
	DatasourceConfig[database_config.Name] = config
	DatasourceDatabase[database_config.Name] = database_config.Database
	AllDatabaseConfig[database_config.Name] = database_config


	// Initialize all our secondary databases
	for _, database_data := range databases {
		datasource, config, err = InitDatamanDatabase(database_data)

		if err != nil {
			panic(fmt.Sprintf("Load schema configuration data: %s: %s", database_data.Schema, err.Error()))
		}

		DatasourceInstance[database_data.Name] = datasource
		DatasourceConfig[database_data.Name] = config
		DatasourceDatabase[database_data.Name] = database_data.Database
		AllDatabaseConfig[database_data.Name] = database_data
	}

}


func InitDatamanDatabase(database_config DatabaseConfig) (*storagenode.DatasourceInstance, *storagenode.DatasourceInstanceConfig, error) {
	// Initialize the DefaultDatabase
	config := storagenode.DatasourceInstanceConfig{
		StorageNodeType: "postgres",
		StorageConfig: map[string]interface{}{
			"pg_string": database_config.ConnectOptions,
		},
	}

	// This is the development location
	schema_str, err := ioutil.ReadFile(database_config.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot read database config file: %s", database_config.Schema)
	}

	var meta metadata.Meta
	err = json.Unmarshal(schema_str, &meta)
	if err != nil {
		panic(fmt.Sprintf("Cannot parse JSON config data: %s: %s", database_config.Schema, err.Error()))
	}

	datasource, err := storagenode.NewLocalDatasourceInstance(&config, &meta)

	return datasource, &config, err
}

func ValidateField(database string, table string, record_pkey string, field_name string, value interface{}, field_map map[string]interface{}) string {
	error := ""

	UdnLogLevel(nil, log_trace, "Validation Field: %s.%s.%s.%s: %v\n", database, table, record_pkey, field_name, value)

	// String
	if field_map["argument_type_id"].(int64) == 2 {
		value_str := GetResult(value, type_string).(string)

		if field_map["length_minimum"] != nil && int64(len([]rune(value_str))) < field_map["length_minimum"].(int64) {
			error = fmt.Sprintf("Must be longer than %d character(s)", field_map["length_minimum"])
		}

	}

	if error != "" {
		UdnLogLevel(nil, log_trace, "Validation Field: %s.%s.%s.%s: %v: ERROR: %s\n", database, table, record_pkey, field_name, value, error)
	}

	return error
}



func GetDatasource(record_id int64) map[string]interface{} {
	options := make(map[string]interface{})

	//TODO(g): Cache these
	result_map := DatamanGet("datasource", int(record_id), options)

	return result_map
}

func GetSchema(record_id int64) map[string]interface{} {
	options := make(map[string]interface{})

	//TODO(g): Cache these
	result_map := DatamanGet("schema", int(record_id), options)

	return result_map
}

func GetSchemaTable(record_id int64) map[string]interface{} {
	options := make(map[string]interface{})

	//TODO(g): Cache these
	result_map := DatamanGet("schema_table", int(record_id), options)

	return result_map
}


func GetSchemaTableField(record_id int64) map[string]interface{} {
	options := make(map[string]interface{})

	//TODO(g): Cache these
	result_map := DatamanGet("schema_table_field", int(record_id), options)

	return result_map
}

