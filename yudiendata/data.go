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
	"time"
	"github.com/jacksontj/dataman/src/datamantype"
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

type DatabaseConfig struct {
	ConnectOptions string `json:"connect_opts"`
	Database string `json:"database"`
	Schema string `json:"schema"`
}

var DefaultDatabase *DatabaseConfig


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
func GetDatasourceInstance(options map[string]interface{}) (*storagenode.DatasourceInstance, string) {
	datasource_instance := DatasourceInstance["_default"]
	datasource_database := DatasourceDatabase["_default"]

	// If there is a specified option to select an explicit Database
	if options["db"] != nil {
		if DatasourceInstance[options["db"].(string)] != nil {
			datasource_instance = DatasourceInstance[options["db"].(string)]
			datasource_database = DatasourceDatabase[options["db"].(string)]
		}
	}

	return datasource_instance, datasource_database
}

func GetRecordLabel(datasource_database string, collection_name string, record_id int) string {
	record_label := fmt.Sprintf("%s.%s.%d", datasource_database, collection_name, record_id)

	return record_label
}

func AddJoinAsFlatNamespace(record map[string]interface{}, join_array []interface{}) {
	//TODO(g): Handle multiple depths of joins.  Currently I dont check if they are dotted for more depth in joins...
	for _, join_name := range join_array {
		join_string := join_name.(string)

		join_record := record[join_string].(map[string]interface{})

		for field_name, value := range join_record {
			field_key := fmt.Sprintf("%s.%s", join_name, field_name)
			record[field_key] = value
		}
	}
}

func DatamanGet(collection_name string, record_id int, options map[string]interface{}) map[string]interface{} {
	//fmt.Printf("DatamanGet: %s: %d\n", collection_name, record_id)

	datasource_instance, datasource_database := GetDatasourceInstance(options)

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

	UdnLogLevel(nil, log_debug, "Dataman GET: %s: %v\n", datasource_database, result.Return[0])

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman GET: %s: ERRORS: %v\n", datasource_database, result.Error)
	}

	record := result.Return[0]
	if record != nil {
		record["__record_label"] = GetRecordLabel(datasource_database, collection_name, record_id)
	}

	// Add all the joined fields as a flat namespace to the original table
	if options["join"] != nil {
		AddJoinAsFlatNamespace(record, options["join"].([]interface{}))
	}

	return record
}

func DatamanSet(collection_name string, record map[string]interface{}, options map[string]interface{}) map[string]interface{} {
	// Duplicate this map, because we are messing with a live map, that we dont expect to change in this function...
	//TODO(g):REMOVE: Once I dont need to manipulate the map in this function anymore...
	record = MapCopy(record)

	datasource_instance, datasource_database := GetDatasourceInstance(options)

	// Remove the _id field, if it is nil.  This means it should be new/insert
	if record["_id"] == nil || record["_id"] == "<nil>" || record["_id"] == "\u003cnil\u003e" || record["_id"] == "" {
		//fmt.Printf("DatamanSet: Removing _id key: %s\n", record["_id"])
		delete(record, "_id")
	} else {
		//fmt.Printf("DatamanSet: Not Removing _id: %s\n", record["_id"])
	}

	// Delete the defaults base64 encoded map, it is never part of a record, it is to ensure we keep our defaults
	if record["_defaults"] != nil {
		delete(record, "_defaults")
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
				UdnLogLevel(nil, log_error,"Record _id is not an integer: %s: %s", collection_name, record)
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
				UdnLogLevel(nil, log_debug, "Removing field: %s: %s: %v\n", collection_name, k, record[k])
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
			"db":             datasource_database,
			"shard_instance": "public",
			"collection":     collection_name,
			"record":         record,
		},
	}

	UdnLogLevel(nil, log_debug,"Dataman SET: Record: %v\n", record)
	//fmt.Printf("Dataman SET: Record: JSON: %v\n", JsonDump(record))
	//fmt.Printf("Dataman SET: Query: JSON: %v\n", JsonDump(dataman_query))

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)

	//result_bytes, _ := json.Marshal(result)
	//fmt.Printf("Dataman SET: %s\n", result_bytes)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman SET: ERROR: %v\n", result.Error)
	}

	if result.Return != nil {
		record := result.Return[0]
		record["__record_label"] = GetRecordLabel(datasource_database, collection_name, int(record["_id"].(int64)))
		UdnLogLevel(nil, log_trace, "Dataman SET: Result Record: JSON: %v\n", record)

		return record
	} else {
		return nil
	}
}

func DatamanFilter(collection_name string, filter map[string]interface{}, options map[string]interface{}) []map[string]interface{} {

	//fmt.Printf("DatamanFilter: %s:  Filter: %v  Join: %v\n\n", collection_name, filter, options["join"])
	//fmt.Printf("Sort: %v\n", options["sort"])		//TODO(g): Sorting

	datasource_instance, datasource_database := GetDatasourceInstance(options)

	filter = MapCopy(filter)

	for k, v := range filter {
		switch v.(type) {
		case string:
			filter[k] = []string{"=", v.(string)}
		case int64:
			filter[k] = []string{"=", fmt.Sprintf("%d", v)}
		}
	}


	filter_map := map[string]interface{} {
		"db":             datasource_database,
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
	UdnLogLevel(nil, log_trace, "Dataman Filter: %s\n\n", JsonDump(filter_map))
	UdnLogLevel(nil, log_trace, "Dataman Filter Map Filter: %s\n\n", SnippetData(filter_map["filter"], 120))
	UdnLogLevel(nil, log_trace, "Dataman Filter Map Filter Array: %s\n\n", SnippetData(filter_map["filter"].(map[string]interface{})["name"], 120))

	dataman_query := &query.Query{query.Filter, filter_map}

	result := datasource_instance.HandleQuery(context.Background(), dataman_query)

	if result.Error != "" {
		UdnLogLevel(nil, log_error, "Dataman ERROR: %v\n", result.Error)
	} else {
		//fmt.Printf("Dataman FILTER: %v\n", result.Return)
	}

	// Add all the joined fields as a flat namespace to the original table
	for _, record := range result.Return {
		if options["join"] != nil {
			AddJoinAsFlatNamespace(record, options["join"].([]interface{}))
		}
	}

	return result.Return
}

func DatamanFilterFull(collection_name string, filter interface{}, options map[string]interface{}) []map[string]interface{} {
	// Contains updated functionality of DatamanFilter where multiple constraints can be used as per dataman specs

	datasource_instance, datasource_database := GetDatasourceInstance(options)

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
		if options["join"] != nil {
			AddJoinAsFlatNamespace(record, options["join"].([]interface{}))
		}
	}

	return result.Return
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


func DatamanEnsureDatabases(pgconnect string, database string, current_path string, new_path string) {

	//TODO(g): Do multiple DBs in the future (schema), for now just limit it to opsdb, because thats all we need now
	limited_database_search := "_default"


	// Get the Hard coded OpsDB record from the database `schema` table
	option_map := make(map[string]interface{})
	filter_map := make(map[string]interface{})
	filter_map["name"] = limited_database_search
	schema_result := DatamanFilter("schema", filter_map, option_map)
	schema_map := schema_result[0]

	UdnLogLevel(nil, log_info, "Schema: %v\n\n", schema_map)

	default_schema := DatasourceInstance["_default"].StoreSchema

	//TODO(g): Remove when we have the Dataman capability to ALTER tables to set PKEYs
	db, err := sql.Open("postgres", pgconnect)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()



	UdnLogLevel(nil, log_info, "\nList DB Start: %v\n\n", time.Now().String())
	//db_list := default_schema.ListDatabase(context.Background())
	db_item := default_schema.GetDatabase(context.Background(), limited_database_search)
	UdnLogLevel(nil, log_info, "\nList DB Stop: %v\n\n", time.Now().String())

	UdnLogLevel(nil, log_info, "\n\nFound DB Item: %v\n\n", db_item.Name)

	//shard_instance := default_schema.ListShardInstance(context.Background(), db_item.Name)


	//TODO(g): Make an empty list of the collections, so we can track what we have, to find missing ones
	collection_array := make([]string, 0)


	//fmt.Printf("\n\nFound DB Shard Instance: %v -- %v\n\n", shard_instance, db_item.ShardInstances["public"])

	//collections := default_schema.ListCollection(context.Background(), db_item.Name, "public")

	for _, collection := range db_item.ShardInstances["public"].Collections {
		UdnLogLevel(nil, log_info, "\n\nFound DB Collections: %s\n", collection.Name)

		option_map := make(map[string]interface{})
		filter_map := make(map[string]interface{})
		filter_map["name"] = collection.Name
		filter_map["schema_id"] = schema_map["_id"]
		collection_result := DatamanFilter("schema_table", filter_map, option_map)

		collection_map := make(map[string]interface{})
		if len(collection_result) > 0 {
			collection_map = collection_result[0]
			UdnLogLevel(nil, log_info, "OpsDB Collection: %v\n\n", collection_map)

			// Add collection to our tracking array
			collection_array = append(collection_array, collection.Name)


			//TODO(g): Make an empty list of the collection fields, so we can track what we have, to find missing ones
			collection_field_array := make([]string, 0)

			// Check: Index, Primary Index

			for _, field := range collection.Fields {
				UdnLogLevel(nil, log_info, "\n\nFound DB Collections: %s  Field: %s  Type: %s   (Default: %v -- Not Null: %v -- Relation: %v)\n", collection.Name, field.Name, field.FieldType.Name, field.Default, field.NotNull, field.Relation)

				// Check: Not Null, Relation, Default, Type

				option_map := make(map[string]interface{})
				filter_map := make(map[string]interface{})
				filter_map["name"] = field.Name
				filter_map["schema_table_id"] = collection_map["_id"]
				collection_field_result := DatamanFilter("schema_table_field", filter_map, option_map)

				collection_field_map := make(map[string]interface{})
				if len(collection_field_result) > 0 {
					collection_field_map = collection_field_result[0]
					UdnLogLevel(nil, log_info, "OpsDB Collection Field: %v\n\n", collection_field_map)

					// Add collection to our tracking array
					collection_field_array = append(collection_field_array, field.Name)


				} else {
					UdnLogLevel(nil, log_info, "OpsDB Collection Field: MISSING: %s\n\n", field.Name)

				}
			}
		} else {
			UdnLogLevel(nil, log_info, "OpsDB Collection: MISSING: %s\n\n", collection.Name)

		}
	}


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
			UdnLogLevel(nil, log_info, "Not Found collection: %s\n\n", collection_record["name"])


			new_collection := metadata.Collection{}

			new_collection.Name = collection_record["name"].(string)
			new_collection.Fields = make(map[string]*metadata.CollectionField)

			// Get all the fields for this collection
			option_map := make(map[string]interface{})
			filter_map := make(map[string]interface{})
			filter_map["schema_table_id"] = collection_record["_id"]
			all_collection_field_result := DatamanFilter("schema_table_field", filter_map, option_map)


			// Create the new collection
			err := default_schema.AddCollection(context.Background(), db_item, db_item.ShardInstances["public"], &new_collection)
			UdnLogLevel(nil, log_info, "Add New Collection: %s: ERROR: %s\n\n", new_collection.Name, err)

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
				err := default_schema.AddCollectionField(context.Background(), db_item, db_item.ShardInstances["public"], &new_collection, &new_field)
				UdnLogLevel(nil, log_info, "Add New Collection Field: %s: %s: ERROR: %s\n\n", new_collection.Name, new_field.Name, err)

				if field_map["is_primary_key"] == true {
					new_index := metadata.CollectionIndex{}

					new_index.Name = fmt.Sprintf("pkey_%s", new_field.Name)
					new_index.Primary = true
					new_index.Unique = true

					new_index.Fields = make([]string, 0)
					new_index.Fields = append(new_index.Fields, new_field.Name)

					// Create the new collection field index
//					err := default_schema.AddCollectionIndex(context.Background(), db_item, db_item.ShardInstances["public"], &new_collection, &new_index)

					// Perform an ALTER table through SQL here, as dataman doesnt allow it
					//TODO(g)...
					//
					sql := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)", new_collection.Name, new_field.Name)
					Query(db, sql)
					UdnLogLevel(nil, log_info, "Add New Collection Field PKEY: %s: %s: ERROR: %s\n\n", new_collection.Name, new_index.Name, err)

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

func InitDataman(pgconnect string, database string, configfile string, databases map[string]DatabaseConfig) {

	datasource, config, err := InitDatamanDatabase(database, pgconnect, configfile)
	if err != nil {
		datasource, config, err = InitDatamanDatabase(database, pgconnect, "/etc/web6/schema.json")
		if err != nil {
			panic(fmt.Sprintf("Load schema configuration data: %s: %s", configfile, err.Error()))
		}
	}

	//TODO(g): Fix this, as this hardcodes everything to one.  Simple in the beginning, but maybe not useful now.  Maybe just the default?
	DefaultDatabaseTarget = database

	DatasourceInstance["_default"] = datasource
	DatasourceConfig["_default"] = config
	DatasourceDatabase["_default"] = database

	for database_name, database_data := range databases {
		datasource, config, err = InitDatamanDatabase(database_data.Database, database_data.ConnectOptions, database_data.Schema)

		if err != nil {
			panic(fmt.Sprintf("Load schema configuration data: %s: %s", database_data.Schema, err.Error()))
		}

		DatasourceInstance[database_name] = datasource
		DatasourceConfig[database_name] = config
		DatasourceDatabase[database_name] = database_data.Database
	}

}

func InitDatamanDatabase(database string, connect_string string, configfile string) (*storagenode.DatasourceInstance, *storagenode.DatasourceInstanceConfig, error) {
	// Initialize the DefaultDatabase
	config := storagenode.DatasourceInstanceConfig{
		StorageNodeType: "postgres",
		StorageConfig: map[string]interface{}{
			"pg_string": connect_string,
		},
	}

	// This is the development location
	schema_str, err := ioutil.ReadFile(configfile)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot read database config file: %s", configfile)
	}

	var meta metadata.Meta
	err = json.Unmarshal(schema_str, &meta)
	if err != nil {
		panic(fmt.Sprintf("Cannot parse JSON config data: %s: %s", configfile, err.Error()))
	}

	datasource, err := storagenode.NewLocalDatasourceInstance(&config, &meta)

	return datasource, &config, err
}
