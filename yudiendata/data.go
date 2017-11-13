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

	. "github.com/ghowland/yudien/yudienutil"
	"github.com/jacksontj/dataman/src/query"
	"github.com/jacksontj/dataman/src/storage_node"
	"github.com/jacksontj/dataman/src/storage_node/metadata"
	"github.com/junhsieh/goexamples/fieldbinding/fieldbinding"
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
	type_string_force = iota // This forces it to a string, even if it will be ugly, will print the type of the non-string data too.  Testing this to see if splitting these into 2 yields better results.
	type_array        = iota // []interface{} - takes: lists, arrays, maps (key/value tuple array, strings (single element array), ints (single), floats (single)
	type_map          = iota // map[string]interface{}
)

var DatasourceInstance = map[string]*storagenode.DatasourceInstance{}

func GetSelectedDb(db_web *sql.DB, db *sql.DB, db_id int64) *sql.DB {
	// Assume we are using the non-web DB
	selected_db := db

	if db_id == 1 {
		selected_db = db_web
	} else if db_id == 2 {
		selected_db = db
	}

	return selected_db
}

func Query(db *sql.DB, sql string) []map[string]interface{} {
	fmt.Printf("Query: %s\n", sql)

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
	outArr := []map[string]interface{}{}

	for rs.Next() {
		if err := rs.Scan(fb.GetFieldPtrArr()...); err != nil {
			log.Fatal(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
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
		log.Fatal(fmt.Sprintf("SQL: %s\nError: %s\n", sql, err))
	}

	return outArr
}

func DatamanGet(collection_name string, record_id int, options map[string]interface{}) map[string]interface{} {
	fmt.Printf("DatamanGet: %s: %d\n", collection_name, record_id)

	get_map := map[string]interface{}{
		"db":             "opsdb",
		"shard_instance": "public",
		"collection":     collection_name,
		//"_id":            record_id,
		"pkey": map[string]interface{}{"_id": record_id},
		"join": options["join"],
	}

	//fmt.Printf("Dataman Get: %v\n\n", get_map)

	dataman_query := &query.Query{query.Get, get_map}

	result := DatasourceInstance["opsdb"].HandleQuery(context.Background(), dataman_query)

	//fmt.Printf("Dataman GET: %v\n", result.Return[0])
	if result.Error != "" {
		fmt.Printf("Dataman GET: ERRORS: %v\n", result.Error)
	}

	return result.Return[0]
}

func DatamanSet(collection_name string, record map[string]interface{}) map[string]interface{} {
	// Duplicate this map, because we are messing with a live map, that we dont expect to change in this function...
	//TODO(g):REMOVE: Once I dont need to manipulate the map in this function anymore...
	record = MapCopy(record)

	// Remove the _id field, if it is nil.  This means it should be new/insert
	if record["_id"] == nil || record["_id"] == "<nil>" || record["_id"] == "\u003cnil\u003e" || record["_id"] == "" {
		fmt.Printf("DatamanSet: Removing _id key: %s\n", record["_id"])
		delete(record, "_id")
	} else {
		fmt.Printf("DatamanSet: Not Removing _id: %s\n", record["_id"])
	}

	// Fix data manually, for now
	for k, v := range record {
		if v == "true" {
			record[k] = true
		} else if v == "false" {
			record[k] = false
		}
	}

	// Fixup the record, if its not a new one, by getti
	// ng any values
	//TODO(g):REMOVE: This is fixing up implementation problems in Dataman, which Thomas will fix...
	if record["_id"] != nil && record["_id"] != "" {
		//fmt.Printf("Ensuring all fields are present (HACK): %s: %v\n", collection_name, record["_id"])

		// Record ID will be an integer
		var record_id int64
		var err interface{}
		switch record["_id"].(type) {
		case string:
			record_id, err = strconv.ParseInt(record["_id"].(string), 10, 32)
			if err != nil {
				panic(err)
			}
		default:
			record_id = GetResult(record["_id"], type_int).(int64)
		}

		options := make(map[string]interface{})

		record_current := DatamanGet(collection_name, int(record_id), options)

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
				fmt.Printf("Removing field: %s: %s: %v\n", collection_name, k, record[k])
				delete(record, k)
			}
		}
	} else {
		// This is a new record, we just tested for it above, remove empty string _id
		delete(record, "_id")
	}

	// Remove fields I know I put in here, that I dont want to go in
	//TODO(g):REMOVE: Same as the others
	delete(record, "_table")
	delete(record, "_web_data_widget_instance_id")

	// Form the Dataman query
	dataman_query := &query.Query{
		query.Set,
		map[string]interface{}{
			"db":             "opsdb",
			"shard_instance": "public",
			"collection":     collection_name,
			"record":         record,
		},
	}

	//fmt.Printf("Dataman SET: Record: %v\n", record)
	fmt.Printf("Dataman SET: Record: JSON: %v\n", JsonDump(record))

	result := DatasourceInstance["opsdb"].HandleQuery(context.Background(), dataman_query)

	//result_bytes, _ := json.Marshal(result)
	//fmt.Printf("Dataman SET: %s\n", result_bytes)

	if result.Error != "" {
		fmt.Printf("Dataman SET: ERROR: %v\n", result.Error)
	}

	return result.Return[0]
}

func DatamanFilter(collection_name string, filter map[string]interface{}, options map[string]interface{}) []map[string]interface{} {

	fmt.Printf("DatamanFilter: %s:  Filter: %v  Join: %v\n\n", collection_name, filter, options["join"])
	//fmt.Printf("Sort: %v\n", options["sort"])		//TODO(g): Sorting

	for k, v := range filter {
		switch v.(type) {
		case string:
			filter[k] = []string{"=", v.(string)}
		}
	}

	filter_map := map[string]interface{}{
		"db":             "opsdb",
		"shard_instance": "public",
		"collection":     collection_name,
		"filter":         filter,
		"join":           options["join"],
		"sort":           options["sort"],
		//"sort_reverse":	  []bool{true},
	}

	fmt.Printf("Dataman Filter: %v\n\n", filter_map)
	fmt.Printf("Dataman Filter Map Filter: %s\n\n", SnippetData(filter_map["filter"], 120))
	fmt.Printf("Dataman Filter Map Filter Array: %s\n\n", SnippetData(filter_map["filter"].(map[string]interface{})["name"], 120))

	dataman_query := &query.Query{query.Filter, filter_map}

	result := DatasourceInstance["opsdb"].HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		fmt.Printf("Dataman ERROR: %v\n", result.Error)
	} else {
		fmt.Printf("Dataman FILTER: %v\n", result.Return)
	}

	return result.Return
}

func SanitizeSQL(text string) string {
	text = strings.Replace(text, "'", "''", -1)

	return text
}

func InitDataman(pgconnect string) {
	config := storagenode.DatasourceInstanceConfig{
		StorageNodeType: "postgres",
		StorageConfig: map[string]interface{}{
			"pg_string": pgconnect,
		},
	}

	schema_str, err := ioutil.ReadFile("./data/schema.json")
	if err != nil {
		log.Panic(err)
	}

	//fmt.Printf("Schema STR: %s\n\n", schema_str)

	var meta metadata.Meta
	err = json.Unmarshal(schema_str, &meta)
	if err != nil {
		panic(err)
	}

	if datasource, err := storagenode.NewLocalDatasourceInstance(&config, &meta); err == nil {
		DatasourceInstance["opsdb"] = datasource
	} else {
		panic(err)
	}
}
