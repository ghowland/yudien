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

type OpsdbConfig struct {
	ConnectOptions string `json:"connect_opts"`
	Database string `json:"database"`
}

var Opsdb *OpsdbConfig


var DatasourceInstance = map[string]*storagenode.DatasourceInstance{}
var DatasourceConfig = map[string]*storagenode.DatasourceInstanceConfig{}

var DatabaseTarget string

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
	//fmt.Printf("DatamanGet: %s: %d\n", collection_name, record_id)

	get_map := map[string]interface{}{
		"db":             DatabaseTarget,
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
		//fmt.Printf("DatamanSet: Removing _id key: %s\n", record["_id"])
		delete(record, "_id")
	} else {
		//fmt.Printf("DatamanSet: Not Removing _id: %s\n", record["_id"])
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
				UdnError(nil,"Record _id is not an integer: %s: %s", collection_name, record)
				delete(record, "_id")
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
		map[string]interface{} {
			"db":             DatabaseTarget,
			"shard_instance": "public",
			"collection":     collection_name,
			"record":         record,
		},
	}

	//fmt.Printf("Dataman SET: Record: %v\n", record)
	//fmt.Printf("Dataman SET: Record: JSON: %v\n", JsonDump(record))
	//fmt.Printf("Dataman SET: Query: JSON: %v\n", JsonDump(dataman_query))

	result := DatasourceInstance["opsdb"].HandleQuery(context.Background(), dataman_query)

	//result_bytes, _ := json.Marshal(result)
	//fmt.Printf("Dataman SET: %s\n", result_bytes)

	if result.Error != "" {
		fmt.Printf("Dataman SET: ERROR: %v\n", result.Error)
	}

	if result.Return != nil {
		return result.Return[0]
	} else {
		return nil
	}
}

func DatamanFilter(collection_name string, filter map[string]interface{}, options map[string]interface{}) []map[string]interface{} {

	//fmt.Printf("DatamanFilter: %s:  Filter: %v  Join: %v\n\n", collection_name, filter, options["join"])
	//fmt.Printf("Sort: %v\n", options["sort"])		//TODO(g): Sorting

	filter = MapCopy(filter)

	for k, v := range filter {
		switch v.(type) {
		case string:
			filter[k] = []string{"=", v.(string)}
		case int64:
			filter[k] = []string{"=", fmt.Sprintf("%d", v)}
		}
	}


	filter_map := map[string]interface{}{
		"db":             DatabaseTarget,
		"shard_instance": "public",
		"collection":     collection_name,
		"filter":         filter,
		"join":           options["join"],
		"sort":           options["sort"],
		//"sort_reverse":	  []bool{true},
	}

	//fmt.Printf("Dataman Filter: %s\n\n", JsonDump(filter_map))
	//fmt.Printf("Dataman Filter Map Filter: %s\n\n", SnippetData(filter_map["filter"], 120))
	//fmt.Printf("Dataman Filter Map Filter Array: %s\n\n", SnippetData(filter_map["filter"].(map[string]interface{})["name"], 120))

	dataman_query := &query.Query{query.Filter, filter_map}

	result := DatasourceInstance["opsdb"].HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		fmt.Printf("Dataman ERROR: %v\n", result.Error)
	} else {
		//fmt.Printf("Dataman FILTER: %v\n", result.Return)
	}

	return result.Return
}

func DatamanFilterFull(collection_name string, filter_json string, options map[string]interface{}) []map[string]interface{} {
	// Contains updated functionality of DatamanFilter where multiple constraints can be used as per dataman specs

	fmt.Printf("DatamanFilter: %s:  Filter: %v  Join: %v\n\n", collection_name, filter_json, options["join"])
	//fmt.Printf("Sort: %v\n", options["sort"])		//TODO(g): Sorting

	var filter interface{}

	err := json.Unmarshal([]byte(filter_json), &filter)

	if err != nil {
		filter = nil // set filter to nil if JSON string cannot be parsed
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


	dataman_query := &query.Query{query.Filter, filter_map}

	result := DatasourceInstance["opsdb"].HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		fmt.Printf("Dataman ERROR: %v\n", result.Error)
	} else {
		fmt.Printf("Dataman FILTER: %v\n", result.Return)
	}

	return result.Return
}


func DatamanEnsureDatabases(pgconnect string, database string, current_path string, new_path string) {

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

/*
	// Create a metaStore so we can mutate this DB schema
	metaStore, err := storagenode.NewMetadataStore(DatasourceConfig["opsdb"])
	if err != nil {
		panic(err)
	}

	// To prep the current configuration, scans current schema
	DatasourceInstance["opsdb"], err = storagenode.NewDatasourceInstance(DatasourceConfig["opsdb"], metaStore)
	if err != nil {
		panic(err)
	}
*/

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

func InitDataman(pgconnect string, database string) {
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



	if datasource, err := storagenode.NewLocalDatasourceInstance(&config, &meta); err == nil {
	//if datasource, err := storagenode.NewDatasourceInstanceDefault(&config); err == nil {
		DatasourceInstance["opsdb"] = datasource
		DatasourceConfig["opsdb"] = &config
	} else {
		panic("Cannot open primary database connection: " + err.Error())
	}
}

