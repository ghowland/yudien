package yudiendata

import (
	"fmt"
	. "github.com/ghowland/yudien/yudienutil"
	"strconv"
)

func GenerateSchemaJson(path string) {
	fmt.Printf("Generate Schema JSON: %s\n", path)

	data := make(map[string]interface{})
	data["databases"] = make(map[string]interface{})
	databases := data["databases"].(map[string]interface{})
	data["field_types"] = make(map[string]interface{})
	field_types := data["field_types"].(map[string]interface{})

	// Get Type Map map
	type_map_options := make(map[string]interface{})
	type_map_filter := make(map[string]interface{})
	type_map := make(map[int64]map[string]interface{})
	type_map_filter["schema_type_map_id"] = "1"	//TODO(g): Only works as a string.  Int value doesnt work.
	type_map_result := DatamanFilter("schema_type_map_item", type_map_filter, type_map_options)
	for _, item := range type_map_result {
		type_map[item["_id"].(int64)] = item

		field_types[item["name"].(string)] = make(map[string]interface{})
		field_types[item["name"].(string)].(map[string]interface{})["name"] = item["name"]
		field_types[item["name"].(string)].(map[string]interface{})["dataman_type"] = item["target_name"]
	}
	fmt.Printf("Import Schema JSON: Type Map: %v\n", type_map)

	map_options := make(map[string]interface{})
	map_filter := make(map[string]interface{})
	schema_result := DatamanFilter("schema", map_filter, map_options)

	for _, item := range schema_result {
		databases[item["name"].(string)] = make(map[string]interface{})
		databases[item["name"].(string)].(map[string]interface{})["name"] = item["name"].(string)
		databases[item["name"].(string)].(map[string]interface{})["shard_instances"] = make(map[string]interface{})
		databases[item["name"].(string)].(map[string]interface{})["provision_state"] = 3

		shard_instances := databases[item["name"].(string)].(map[string]interface{})["shard_instances"].(map[string]interface{})
		shard_instances["public"] = make(map[string]interface{})
		public_shard := shard_instances["public"].(map[string]interface{})
		public_shard["name"] = "public"
		public_shard["count"] = 1
		public_shard["instance"] = 1
		public_shard["provision_state"] = 3 //TODO(g): Should these be put in here manually?
		public_shard["collections"] = make(map[string]interface{})
		collections := public_shard["collections"].(map[string]interface{})

		map_filter = make(map[string]interface{})
		map_filter["schema_id"] = fmt.Sprintf("%d", item["_id"])
		schema_table_result := DatamanFilter("schema_table", map_filter, map_options)

		for _, table_item := range schema_table_result {
			collections[table_item["name"].(string)] = make(map[string]interface{})
			table := collections[table_item["name"].(string)].(map[string]interface{})

			table["name"] = table_item["name"]
			table["fields"] = make(map[string]interface{})
			table["provision_state"] = 3

			fields := table["fields"].(map[string]interface{})

			// Add Indexes, if we have them
			if table_item["data_json"] != nil && table_item["data_json"].(map[string]interface{})["indexes"] != nil {
				table["indexes"] = table_item["data_json"].(map[string]interface{})["indexes"]
			}

			map_filter = make(map[string]interface{})
			map_filter["schema_table_id"] = fmt.Sprintf("%d", table_item["_id"])
			schema_table_field_result := DatamanFilter("schema_table_field", map_filter, map_options)

			for _, field_item := range schema_table_field_result {
				fields[field_item["name"].(string)] = make(map[string]interface{})
				field := fields[field_item["name"].(string)].(map[string]interface{})

				field["name"] = field_item["name"]
				field["provision_state"] = 3 //TODO(g): Should these be put in here manually?
				field["field_type"] = type_map[field_item["schema_type_map_item_id"].(int64)]["name"]

				if field_item["allow_null"] == false {
					field["not_null"] = true
				}

				if field_item["default_value"] != nil {
					default_value, err := strconv.Atoi(field_item["default_value"].(string))
					if err == nil {
						field["default"] = default_value
					} else {
						if field_item["default_value"] == "true!" {
							field["default"] = true
						} else if field_item["default_value"] == "false!" {
							field["default"] = false
						} else {
							field["default"] = field_item["default_value"]
						}
					}
				}

				// Add Relations, if we have them
				if field_item["data_json"] != nil && field_item["data_json"].(map[string]interface{})["relation"] != nil {
					field["relation"] = field_item["data_json"].(map[string]interface{})["relation"]
				}
			}
		}
	}

	output := JsonDump(data)

	WritePathData(path, output)
}

func ImportSchemaJson(path string) map[string]interface{} {
	fmt.Printf("Import Schema JSON: Started\n")

	text := ReadPathData(path)

	data, err := JsonLoadMap(text)
	if err != nil {
		panic(err)
	}


	// Get Type Map map
	type_map_options := make(map[string]interface{})
	type_map_filter := make(map[string]interface{})
	type_map := make(map[string]map[string]interface{})

	//TODO(g): Should pull this from the path name, so that we can pull in multiple at a time
	type_map_filter["schema_type_map_id"] = "1"	//TODO(g): Only works as a string.  Int value doesnt work.


	type_map_result := DatamanFilter("schema_type_map_item", type_map_filter, type_map_options)
	for _, item := range type_map_result {
		type_map[item["name"].(string)] = item
	}
	fmt.Printf("Import Schema JSON: Type Map: %v\n", type_map)


	// Import databases
	databases := data["databases"].(map[string]interface{})
	for key, value := range databases {
		fmt.Printf("  Database: %s\n", key)

		database := value.(map[string]interface{})
		shard_instances := database["shard_instances"].(map[string]interface{})

		// Import this database into the `schema` table
		database_record := Import_Database(database["name"].(string), database)

		for shard_key, shard_value := range shard_instances {
			fmt.Printf("    Shard Instance: %s\n", shard_key)

			shard_instance := shard_value.(map[string]interface{})
			for collection_key, collection_value := range shard_instance["collections"].(map[string]interface{}) {
				fmt.Printf("      Collection: %s\n", collection_key)

				collection := collection_value.(map[string]interface{})

				collection_record := Import_Collection(database_record, collection_key, collection)

				for field_key, field_value := range collection["fields"].(map[string]interface{}) {
					field := field_value.(map[string]interface{})
					fmt.Printf("        Field: %s == %v\n", field_key, field)

					Import_CollectionField(collection_record, field_key, field, type_map, collection)
				}
			}
		}

	}


	//TODO(g): Remove tables/fields we didnt encounter, so we can clean things up as well as adding them
	//



	//TODO(g): Ive just added these to the DB manually in `schema_type_map` table.  Later import them.  How to name for upsert?
	/*
	// Import field types
	field_types := data["field_types"].(map[string]interface{})
	for key, value := range field_types {
		fmt.Printf("  Field Type: %s == %v\n", key, value)
	}*/

	fmt.Printf("Parse Schema JSON: Finished\n")

	return data
}


func Import_Database(name string, data map[string]interface{}) map[string]interface{} {
	fmt.Printf("Import: Database: %s\n", name)

	options_map := make(map[string]interface{})

	record := make(map[string]interface{})
	record["name"] = name

	result := DatamanFilter("schema", record, options_map)

	fmt.Printf("Import: Database: Filter RESULT: %v\n", result)

	// We dont have this table field, so add it
	if len(result) != 0 {
		record = result[0]
		fmt.Printf("      Import: Database: Updating: %s (%v)\n", name, record)
	} else {
		fmt.Printf("      Import: Database: Adding: %s (%v)\n", name, record)
	}

	// Update the record
	record = DatamanSet("schema", record)

	return record
}

func Import_Collection(database map[string]interface{}, name string, data map[string]interface{}) map[string]interface{} {
	fmt.Printf("      Import: Collection: %s   DB: %v\n", name, database)
	//fmt.Printf("Import: Collection: DATA: %s\n", JsonDump(data))

	options_map := make(map[string]interface{})

	record := make(map[string]interface{})
	record["name"] = name
	record["schema_id"] = database["_id"].(int64)

	result := DatamanFilter("schema_table", record, options_map)

	fmt.Printf("      Import: Collection: Filter RESULT: %v\n", result)

	// We dont have this table field, so add it
	if len(result) != 0 {
		record = result[0]
		fmt.Printf("      Import: Collection: Updating: %s (%v)\n", name, record)
	} else {
		fmt.Printf("      Import: Collection: Adding: %s (%v)\n", name, record)
	}

	// Ensure we have JSON data available
	if record["data_json"] == nil {
		record["data_json"] = make(map[string]interface{})
	}

	// If we have Indexes, capture them
	if data["indexes"] != nil {
		record["data_json"].(map[string]interface{})["indexes"] = data["indexes"]
	}

	// Update the record
	record = DatamanSet("schema_table", record)

	return record
}


func Import_CollectionField(collection map[string]interface{}, name string, data map[string]interface{}, type_map map[string]map[string]interface{}, collection_data map[string]interface{}) map[string]interface{} {
	fmt.Printf("        Import: Collection Field: %s\n", name)

	options_map := make(map[string]interface{})

	record := make(map[string]interface{})
	record["name"] = name
	record["schema_table_id"] = collection["_id"]

	result := DatamanFilter("schema_table_field", record, options_map)

	fmt.Printf("      Import: Collection Field: Filter RESULT: %v\n", result)

	// We dont have this table field, so add it
	if len(result) != 0 {
		record = result[0]
		fmt.Printf("      Import: Collection Field: Updating: %s (%v)\n", name, record)
	} else {
		fmt.Printf("      Import: Collection Field: Adding: %s (%v)\n", name, record)
	}

	fmt.Printf("      Import: Collection Field: DATA: \n%s\n", JsonDump(data))
	//fmt.Printf("      Import: Collection Field: COLLECTION: \n%s\n", JsonDump(collection_data))

	// Set record values

	// Allow null?
	if data["not_null"] != nil {
		record["allow_null"] = !data["not_null"].(bool)
	} else {
		record["allow_null"] = true
	}

	// Field Type
	//fmt.Printf("      Import: Collection Field: Field Type: %s: %d\n", data["field_type"], type_map[data["field_type"].(string)])
	record["schema_type_map_item_id"] = type_map[data["field_type"].(string)]["_id"].(int64)

	// Argument Type
	record["argument_type_id"] = type_map[data["field_type"].(string)]["argument_type_id"].(int64)

	// Default Value
	if data["default"] != nil {
		fmt.Printf("      Import: Collection Field: %s:  Default: %s\n", name, data["default"])

		if data["default"] == true {
			record["default_value"] = "true!"
			fmt.Printf("        Import: Collection Field: %s:  Set Default: %s\n", name, record["default_value"])
		} else if data["default"] == false {
			record["default_value"] = "false!"
			fmt.Printf("        Import: Collection Field: %s:  Set Default: %s\n", name, record["default_value"])
		} else {
			record["default_value"] = fmt.Sprintf("%v", data["default"])
			fmt.Printf("        Import: Collection Field: %s:  Set Default: %s\n", name, record["default_value"])
		}
	} else {
		record["default_value"] = nil
	}

	// If our Collection has indexes, we might be a PKEY
	record["is_primary_key"] = false
	if collection_data["indexes"] != nil {
		for _, value := range collection_data["indexes"].(map[string]interface{}) {
			index_item := value.(map[string]interface{})
			if index_item["primary"] == true {
				for _, field_name := range index_item["fields"].([]interface{}) {
					if field_name.(string) == name {
						record["is_primary_key"] = true
					}
				}
			}
		}
	}

	// Ensure we have JSON data available
	if record["data_json"] == nil {
		record["data_json"] = make(map[string]interface{})
	}

	if data["relation"] != nil {
		record["data_json"].(map[string]interface{})["relation"] = data["relation"]
	}

	fmt.Printf("Saved Table Field: BEFORE: %s: %s\n\n", name, JsonDump(record))

	// Update the record
	record = DatamanSet("schema_table_field", record)

	fmt.Printf("Saved Table Field: AFTER: %s: %s\n\n", name, JsonDump(record))

	return record
}

