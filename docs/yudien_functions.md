# Yudien (UDN) Functions

1. [Data Access](#data_access)
    1. [__get - Get Global Data](#__get)
    2. [__set - Set Global Data](#__set)
    3. [__get_index - Get data from input](#__get_index)
    4. [__set_index - Set data to output](#__set_index)
    5. [__get_first - Get First non-nil Data](#__get_first)
    6. [__get_temp - Get Temp Data](#__get_temp)
    7. [__set_temp - Set Temp Data](#__set_temp)
2. [Database](#database)
    1. [__data_get - Dataman Get](#__data_get)
    2. [__data_set - Dataman Set](#__data_set)
    3. [__data_filter - Dataman Filter](#__data_filter)
    4. [__query - Stored SQL Querying](#__query)
3. [Conditions and Looping](#looping)
    1. [__if - If](#__if)
    2. [__else_if  - Debug Output](#__else_if)
    3. [__end_if - Debug Output](#__end_if)
    4. [__not - Not](#__not)
    5. [__not_nil - Not Nil](#__not_nil)
    6. [__iterate - Iterate](#__iterate)
    7. [__end_iterate - End Iterate](#__end_iterate)
    8. [compare_equal - Compare Equal](#compare_equal)
    9. [compare_not_equal - Compare Not Equal](#compare_not_equal)
4. [Execution Control](#execution)
    1. [__input - Input](#__input)
    2. [__input_get - Input Get](#__input_get)
    3. [__function - Call Function](#__function)
    4. [__execute  - Execute UDN](#__execute)
3. [Text](#text)
    1. [__template - String Template from Value](#__template)
    2. [__template_wrap - TBD](#__template_wrap)
    3. [__format - Format Strings from Map](#__format)
    4. [__template_short - String Template from Value](#__template_short)
    5. [__string_append - String Append](#__string_append)
    6. [__string_clear - String Clear](#__string_clear)
    7. [__concat - String Concatenate](#__concat)
    8. [__upper - String Uppercase](#__upper)
    9. [__lower - String Lowercase](#__lower)
    10. [__split - String Split](#__split)
    11. [__json_decode - JSON Decode](#__json_decode)
    12. [__json_encode - JSON Encode](#__json_encode)
    13. [__html_encode - HTML Encode](#__html_encode)
6. [Maps](#map)
    1. [__map_key_set - Map Key Set](#__map_key_set)
    2. [__map_key_delete - Map Key Delete](#__map_key_delete)
    3. [__map_copy - Map Copy](#__map_copy)
    4. [__map_update - Map Update](#__map_update)
7. [Array](#array)
    1. [__array_append - Array Append](#__array_append)
    2. [__array_slice - Array Slice](#__array_slice)
    3. [__array_map_remap - Array Map Remap](#__array_map_remap)
    4. [__array_divide - Array Divide](#__array_divide)
5. [Rendering](#rendering)
    1. [__widget - Render Widget](#__widget)
    2. [__render_data - Render Data Widget](#__render_data)
8. [User](#user)
    1. [__login- LDAP Login](#__login)
9. [Special](#special)
    1. [__ddd_render - Render DDD Dialog Editor](#__ddd_render)
10. [Debugging](#debugging)
    1. [__debug_output - Debug Output](#__debug_output)
11. [Comments](#comments)
    1. [__comment - UDN Comment](#__comment)




## Data Access <a name="data_access"></a>


### __get ::: Get Global Data <a name="__get"></a>

**Go:** UDN_Get

**Input:** Ignored

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings

**Output:** list of maps :: []map[string]interface

**Example:**

```
__input.Testing123.__set.temp.testing.__get.temp.testing
```

**Result:**

```
Testing123
```

Alternate Example, single dotted string uses the same Global Data:

```
__input.Testing123.__set.temp.testing.__get.'temp.testing'
```

**Side Effect:** None

**Related Functions:** [__set](#__set)

### __set ::: Set Global Data <a name="__set"></a>

**Go:** UDN_Set

**Input:** Ignored

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings
  2. Any :: The final data can be any value, and is set into the location

**Output:** list of maps :: []map[string]interface

**Example:**

```
__input.Testing123.__set.temp.testing.__get.temp.testing
```

**Result:**

```
Testing123
```


Alternate Example, single dotted string uses the same Global Data:

```
__input.Testing123.__set.'temp.testing'.__get.temp.testing
```


**Side Effect:** None

**Related Functions:** [__get](#__get)


### __get_index ::: Get Input Data <a name="__get_index"></a>

**Go:** UDN_GetIndex

Note: similar to __get, however the global udn_data is not used. Data comes directly from input

**Input:** Any

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the input data
  1. string (optional, variadic) :: Any number of args can be provided, all strings

**Output:** Any depending on what is specified

**Example:**

```
__input.{key1=value1}.__get_index.key1
```

**Result:**

```
value1
```

Alternate Example:

```
__input.[{key1=value1}, {key2=value2}].__get_index.1.key2
```

**Result:**

```
value2
```

**Side Effect:** None

**Related Functions:** [__set_index](#__set_index)


### __set_index ::: Set Input Data <a name="__set_index"></a>

**Go:** UDN_SetIndex

Note: similar to __set, however data is not stored in the global udn_data. Output is directly piped out to result and not stored. Data comes directly from input and is modified and piped out.

**Input:** Any

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the input data
  1. string :: The last argument is the value that is set to the specified location in the input string

**Output:** The updated input string with the specified updated value

**Example:**

```
__input.{key1=value1}.__set_index.key1.value2
```

**Result:**

```
{key1: value2}
```

Alternate Example:

```
__input.[{key1=value1}, {key2=value2}].__set_index.1.key2.value3
```

**Result:**

```
[{key1=value1, key2=value3}]
```

**Side Effect:** None

**Related Functions:** [__get_index](#__get_index)


### __get_first ::: Get first non-nil Global Data  <a name="__get_first"></a>

Takes an array of N strings, which are dotted for udn_data accessing.  The first value that isnt nil is returned.  nil is returned if they all are.

**Go:** UDN_Get

**Input:** Ignored

**Args:**

  0. string :: Dotted string ('location.goes.here')
  1. string (optional, variadic) :: Any number of args can be provided, same as the first argument

**Output:** Any

**Example:**

```
__input.'Hello World'.__set.special.field.__get_first.'not.a.real.place'.'special.field'
```

**Result:**

```
Hello World
```

**Side Effect:** None

### __get_temp ::: Get Temporary Data  <a name="__get_temp"></a>

Just like __get, except uses a portion of the Global Data space behind a UUID for this ProcessSchemaUDNSet() or __function call.  It allows names to be re-used, which they cannot be in the normal Global Data space, as it is global.

**Go:** UDN_GetTemp

**Input:** Ignored

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings

**Output:** Any

**Example:**

```
__input.Testing123.__set_temp.temp.testing.__get_temp.temp.testing
```

**Result:**

```
Testing123
```


Alternate Example, single dotted string uses the same Global Data:

```
__input.Testing123.__set_temp.'temp.testing'.__get_temp.temp.testing
```


**Side Effect:** None

**Related Functions:** [__set_temp](#__set_temp)


### __set_temp ::: Set Global Data  <a name="__set_temp"></a>

Just like __set, except uses a portion of the Global Data space behind a UUID for this ProcessSchemaUDNSet() or __function call.  It allows names to be re-used, which they cannot be in the normal Global Data space, as it is global.

**Go:** UDN_SetTemp

**Input:** Ignored

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings
  2. Any :: The final data can be any value, and is set into the location

**Output:** list of maps :: []map[string]interface

**Example:**

```
__input.Testing123.__set_temp.testing.__get_temp.testing
```

**Result:**

```
Testing123
```


Alternate Example, single dotted string uses the same Global Data:

```
__input.Testing123.__set_'temp.testing'.__get_temp.testing
```


**Side Effect:** None

**Related Functions:** [__get_temp](#__get_temp)

## Database  <a name="database"></a>

See docs on Dataman for more details: https://docs.google.com/document/d/1YDbwuQPpObAK06nnDd-v-rded3ma25AUToZ5iuewTiU/edit#

### __data_get ::: Dataman Get <a name="__data_get"></a>

Just like __set, except uses a portion of the Global Data space behind a UUID for this ProcessSchemaUDNSet() or __function call.  It allows names to be re-used, which they cannot be in the normal Global Data space, as it is global.

**Go:** UDN_DataGet

**Input:** Ignored

**Args:**

  0. string :: Table/Collection name
  1. int :: Record ID.  Primary key.

**Output:** Map :: map[string]interface

**Example:**

```
__data_get.web_widget_type.1
```

**Result:**

```
{_id: 1, name: "Base Page"}
```

**Side Effect:** None

**Related Functions:** [__data_set](#__data_set), [__data_filter](#__data_filter), [__query](#__query)


### __data_set ::: Dataman Set <a name="__data_set"></a>

Just like __set, except uses a portion of the Global Data space behind a UUID for this ProcessSchemaUDNSet() or __function call.  It allows names to be re-used, which they cannot be in the normal Global Data space, as it is global.

**Go:** UDN_DataSet

**Input:** Ignored

**Args:**

  0. string :: Table/Collection name
  1. map :: Record field data to put back in

**Output:** Map :: map[string]interface

**Example:**

```
__data_set.web_widget_type.{_id=1,name='Base Page'}
```

**Result:**

```
{_id: 1, name: "Base Page"}
```

**Side Effect:** None

**Related Functions:** [__data_get](#__data_get), [__data_filter](#__data_filter)

### __data_filter ::: Dataman Filter <a name="__data_filter"></a>

Just like __set, except uses a portion of the Global Data space behind a UUID for this ProcessSchemaUDNSet() or __function call.  It allows names to be re-used, which they cannot be in the normal Global Data space, as it is global.

**Go:** UDN_DataFilter

**Input:** Ignored

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings
  2. Any :: The final data can be any value, and is set into the location

**Output:** list of maps :: []map[string]interface

**Example:**

```
__data_filter.web_widget_type.{name=(__input.['=', 'Base Page'])}
```

* Note that it is necessary to create a list first to adhere to the dataman requirements

**Result:**

```
[{_id: 1, name: "Base Page"}]
```

**Side Effect:** None

**Related Functions:** [__data_get](#__data_get), [__data_set](#__data_set)

### __query ::: Stored SQL Querying  <a name="__query"></a>

*PARTIALLY DEPRICATED:* Only use `__query` when `__data_get` and `__data_filter` absolutely wont work.  Dataman makes working with data much more consistent and also takes care of integrite problems.  Especially only use Dataman for writing data, as there are additional constraints.

**Go:** UDN_QueryById

**Input:** Ignored

**Args:**

  0. int :: datasource_query.id record primary key
  1. map (optional) :: data arguments for the query, are short templated into the stored SQL

**Output:** list of maps :: []map[string]interface

**Example:**

```
__query.25
```

**Side Effect:** None

**Related Functions:** [__data_get](#__data_get), [__data_filter](#__data_filter)

## Conditions and Looping <a name="looping"></a>

### __if :: Conditional If  <a name="__if"></a>

**Go:** UDN_IfCondition

**Input:** Any

**Args:** None

**Output:** Last Output Function Result

**Example:**

```
__if.1.__debug_output.__end_if
```

**Related Functions:** [__else_if](#__else_if)

**End Block:** [__end_if](#__end_if)

**Side Effect:** Loops over all functions in the block (between __if and matching __end_if)

### __else_if :: Conditional Else, If  <a name="__else_if"></a>

**Go:** UDN_ElseCondition

**Input:** Any

**Args:** None

**Output:** Last Output Function Result

**Example:**

```
__if.0.__debug_output.__else_if.__debug_output.__end_if
```

**Side Effect:** Loops over all functions in the block (between __else_if and matching __end_if or next __else_if)

### __end_if :: End If/ElseIf Block  <a name="__end_if"></a>

**Go:** nil

**Input:** Any

**Args:** None

**Output:** Last Output Function Result

**Example:**

```
__if.1.__debug_output.__end_if
```

**Side Effect:** None

**Related Functions:** [__if](#__if)


### __not :: Not - Reverses boolean test (1, "1", true)  <a name="__not"></a>

**Go:** UDN_Not

**Input:** Boolean value: true, 1, "1", false, 0, "0"

**Args:**
  - Boolean, String, Integer: true, false, "1", "0", 1, 0

**Output:** Boolean: "1", "0"

**Example:**

```
__if.(__not.0).__debug_output.__end_if
```

**Side Effect:** None

**Related Functions:** [__not_nil](#__not_nil), [__if](#__if)

### __not_nil :: Not Nil - Returns "1" (true) if not nil  <a name="__not_nil"></a>

**Go:** UDN_NotNil

**Input:** nil or Not

**Args:** None

**Output:** Boolean: "1", "0"

**Example:**

```
__if.(__not.0).__debug_output.__end_if
```

**Side Effect:** None

**Related Functions:** [__not](#__not), [__if](#__if)


### __iterate :: Iterate  <a name="__iterate"></a>

**Go:** UDN_Iterate

**Input:** Any

**Args:** None

**Output:** First Element of Array

**Example:**

```
__iterate.__debug_output.__end_iterate
```

**End Block:** [__end_iterate](#__end_iterate)

**Side Effect:** Loops over all functions in the block (between __iterate and matching __end_iterate)


### __end_iterate :: End Iterate  <a name="__end_iterate"></a>

**Go:** nil

**Input:** Any

**Args:** None

**Output:** Array of All iterate block runs

**Example:**

```
__input.[1,2,3].__iterate.__debug_output.__end_iterate
```

****Returns:****

```
[1,2,3]
```

**Side Effect:** None

**Related Functions:** [__iterate](#__iterate)

### __compare_equal :: Conditon to Check for Equality  <a name="__compare_equal"></a>

**Go:** UDN_CompareEqual

**Input:** Ignored

**Args:**

  0. Any :: Converted to a string for comparison
  1. Any :: Converted to a string for comparison

**Output:** Boolean: "1", "0"

**Example:**

```
__if.(__compare_equal.Tom.Jerry).__input.1.__else.__input.0.__end_if
```

**Returns:**

```
0
```

**Related Functions:** [__compare_not_equal](#__compare_not_equal), [__if](#__if)

**Side Effect:** None

### __compare_not_equal :: Conditon to Check for Non-Equality  <a name="__compare_not_equal"></a>

**Go:** UDN_CompareNotEqual

**Input:** Ignored

**Args:**

  0. Any :: Converted to a string for comparison
  1. Any :: Converted to a string for comparison

**Output:** Boolean: "1", "0"

**Example:**

```
__if.(__compare_not_equal.Tom.Jerry).__input.1.__else.__input.0.__end_if
```

**Returns:**

```
1
```

**Related Functions:** [__compare_equal](#__compare_equal), [__if](#__if)

**Side Effect:** None



## Execution Control <a name="execution"></a>

### __input ::: Input <a name="__input"></a>

**Go:** UDN_Input

**Input:** Any

**Args:**

  0. Any (optional) :: This overrides the Input coming into this function

**Output:** Any.  Passes through Input or Arg[0]

**Example:**

```
__input.Testing123.__set.temp.testing.__get.temp.testing
```

**Result:**

```
Testing123
```

**Side Effect:** None


### __input_get ::: Retrieves field from current Input as Map <a name="__input_get"></a>

**Go:** UDN_InputGet

**Input:** Map ::: map[string]interface

**Args:**

  0. string :: Index of the field for the Input

**Output:** Any.  Passes through Input or Arg[0]

**Example:**

```
__input.{name=Bob}.__input_get.name
```

**Result:**

```
Bob
```

**Side Effect:** None


### __function ::: Calls a UDN Stored Function <a name="__function"></a>

This uses the udn_stored_function.name as the first argument, and then uses the current input to pass to the function, returning the final result of the function.		Uses the web_site.udn_stored_function_domain_id to determine the stored function

**Go:** UDN_StoredFunction

**Input:** Any

**Args:**

  0. string :: Index of the field for the Input
  1. Any (options, variadic) :: Any arguments from this point are stored as an Array in the Global Data location "function_arg"

**Output:** Any

**Example:**

```
__function.test_function.arg0.arg1.arg2
```

**Result:**

```
Anything!!!
```

**Side Effect:** Any

**Related Functions:** [__execute](#__execute)

### __execute ::: Execute UDN from String <a name="__execute"></a>

Execute a single UDN string.  Combines the 2-tuple normally used to a single string.  Also removes the concurrency blocks, making it a single string and not a next JSON array of 2-tuple strings.

**Go:** UDN_Execute

**Input:** Ignored

**Args:**

  0. string :: UDN code in a single string (Source/Target not separated)

**Output:** Any

**Example:**

```
__execute.'__input.Testing123'
```

**Result:**

```
Testing123
```

**Side Effect:** Any

**Related Functions:** [__function](#__function)



## Text  <a name="text"></a>

### __template :: String Template From Value  <a name="__template"></a>

**Go:** UDN_StringTemplateFromValue

**Input:** Map :: map[string]interface{}

**Args:**

  0. string :: Text to be templated, using Go's text/template function
  1. Map (optional) :: Overrides the Input map value, if present

**Output:** string

**Example:**

```
__input.{name="Bob"}.__template.'Name: {index .Map "name"}'
```

**Returns:**

```
"Name: Bob"
```

**Related Functions:** [__template_wrap](#__template_wrap), [__template_short](#__template_short), [__format](#__format), <a name="__template_map">__template_map</a>

**Side Effect:** None


### __template_wrap :: String Template From Value  <a name="__template_wrap"></a>

Takes N-2 tuple args, after 0th arg, which is the wrap_key, (also supports a single arg templating, like __template, but not the main purpose).  For each N-Tuple, the new map data gets "value" set by the previous output of the last template, creating a rolling "wrap" function.

NOTE(g): I dont know how this function is used at this point.  It was useful, but I dont see an example to explain it.  It's extremely overloaded, but powerful.

**Go:** UDN_StringTemplateMultiWrap

**Input:** Map :: map[string]interface{}

**Args:**

  0. string :: Text to be templated, using Go's text/template function
  1. Map (optional) :: Overrides the Input map value, if present

**Output:** string

**Example:**

```
__input.{name=Bob,job=Programmer}.__template_wrap.'Name: {index .Map "name"}'.{name=Bob}.'Job: {index .Map "job"}'.{job=Programmer}
```

**Returns:**

```
"Name: Bob"
```

**Related Functions:** [__template](#__template), [__template_short](#__template_short), [__format](#__format), <a name="__template_map">__template_map</a>

**Side Effect:** None

### __template_map :: String Template From Value  <a name="__template_map"></a>

Like format, for templating.  Takes 3*N **Args:** (key,text,map), any number of times.  Performs template and assigns key into the input map

**Go:** UDN_MapTemplate

**Input:** Ignored

**Args:**

  0. String :: Set key.  This is where we will set the value once templated.
  1. String :: Template text.  This is the text to be templated.
  2. Map :: This is the data to be templated into the 2nd arg.

**Output:** Passed Through Input

**Example:**

```
__template_map.'location.saved'.'Name: {index .Map "name"}'.{name=Bob}.__get.location.saved
```

**Returns:**

```
"Name: Bob"
```

**Related Functions:** [__template_wrap](#__template_wrap), [__template_short](#__template_short), [__format](#__format), [__template](#__template)

**Side Effect:** None


### __format :: Format Strings from Map  <a name="__format"></a>

Updates a map with keys and string formats.  Uses the map to format the strings.  Takes N args, doing each arg in sequence, for order control

**Go:** UDN_MapStringFormat

**Input:** Map :: map[string]interface

**Args:**

  0. String :: Set key.  This is where we will set the value once templated.
  1. Map :: This is the data to be templated into the 2nd arg.
  2. String (optional, variadic) :: Indefinite pairs of String/Map args
  3. Map (optional, variadic) :: Indefinite pairs of String/Map args

**Output:** Passed Through Input

**Example:**

```
__input.{name=Bob,job=Programmer}.__format.'location.saved.name'.'Name: {index .Map "name"}'.'location.saved.job'.'Job: {index .Map "job"}.__get.location.saved.name'
```

**Returns:**

```
"Name: Bob"
```

**Related Functions:** [__template_wrap](#__template_wrap), [__template_short](#__template_short), [__format](#__format), [__template](#__template)

**Side Effect:** None


### __template_short :: String Template From Value  <a name="__template_short"></a>

Like __template, but uses {{{name}} instead of {index .Map "name"}

**Go:** UDN_StringTemplateFromValueShort

**Input:** Map :: map[string]interface

**Args:**

  0. String :: Set key.  This is where we will set the value once templated.
  1. Map (optional) :: This overrides the Input, if present

**Output:** String

**Example:**

```
__input.{name=Bob,job=Programmer}.__template_short.'Name: {{{name}}}'
```

**Returns:**

```
"Name: Bob"
```

**Related Functions:** [__template_wrap](#__template_wrap), [__template_short](#__template_short), [__format](#__format), [__template](#__template)

**Side Effect:** None


### __string_append :: String Append  <a name="__string_append"></a>

Appends to an existing string, or creates a string if nil (not present in Global Data).  Args work like __get

**Go:** UDN_StringAppend

**Input:** String

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings

**Output:** String

**Example:**

```
__input.'The Quick '.__set.temp.test.__input.'Brown Fox'.__string_append.temp.test.__get.temp.test
```

**Returns:**

```
"The Quick Brown Fox"
```

**Related Functions:** [__string_clear](#__string_clear), [__concat](#__concat)

**Side Effect:** None


### __string_clear:: String Clear  <a name="__string_clear"></a>

This is only needed when re-using a Global Data label, you can start appending to an non-existent location and it will start it with an empty string.

**Go:** UDN_StringClear

**Input:** String

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings

**Output:** String

**Example:**

```
__string_clear.temp.test
```

**Related Functions:** [__string_append](#__string_append)

**Side Effect:** None


### __concat :: String Concatenate  <a name="__concat"></a>

TODO(g): Not Yet Implemented

**Go:** UDN_StringConcat

**Input:** String

**Args:**

  0. string :: If quoted, this can contain dots, of each arg will become part of a "dotted string" to access the global data
  1. string (optional, variadic) :: Any number of args can be provided, all strings

**Output:** String

**Example:**

```
```

**Returns:**

```
```

**Related Functions:**  [__string_clear](#__string_clear), [__string_append](#__string_append)

**Side Effect:** None


### __upper :: String Uppercase  <a name="__upper"></a>

**Go:** UDN_StringUpper

**Input:** String

**Args:**

  0. string :: string that will be set to uppercase

**Output:** String (upper case)

**Example:**

```
"__upper.hElLo"
```

**Returns:**

```
HELLO
```

**Related Functions:**  [__lower](#__lower)

**Side Effect:** None


### __lower :: String Lowercase  <a name="__lower"></a>

**Go:** UDN_StringLower

**Input:** String

**Args:**

  0. string :: string that will be set to lowercase

**Output:** String (lower case)

**Example:**

```
"__lower.hElLo"
```

**Returns:**

```
hello
```

**Related Functions:**  [__upper](#__upper)

**Side Effect:** None


### __split :: String Split  <a name="__split"></a>

**Go:** UDN_StringSplit

**Input:** String that will be split

**Args:**

  0. string :: string that is used as the separator

**Output:** List (of strings)

**Example:**

```
"__input.'hello.world.how.are.you'.__split.'.'"
```

**Returns:**

```
[hello, world, how, are, you]
```

**Related Functions:**  [__concat](#__concat), [__string_append](#__string_append)

**Side Effect:** None


### __json_decode :: JSON Decode  <a name="__json_decode"></a>

Decodes a string to Go data: map[string]interface is assumed if using Global Data

**Go:** UDN_JsonDecode

**Input:** String

**Args:** None

**Output:** Map :: map[string]interface

**Example:**

```
__input.'{"a": 1}'.__json_decode
```

**Returns:**

```
{a: 1}
```

**Related Functions:** [__json_encode](#__json_encode)

**Side Effect:** None


### __json_encode :: JSON Encode  <a name="__json_encode"></a>

Encodes Go data into a JSON string

**Go:** UDN_JsonDecode

**Input:** Any

**Args:** None

**Output:** String

**Example:**

```
__input.{a=1}.__json_encode
```

**Returns:**

```
{"a": "1"}
```

**Related Functions:** [__json_decode](#__json_decode)

**Side Effect:** None

### __html_encode :: HTML Encode  <a name="__html_encode"></a>

Escapes HTML characters

**Go:** UDN_HtmlEncode

**Input:** String

**Args:** None

**Output:** String

**Example:**

```
__input.'1 < 2'.__html_encode
```

**Returns:**

```
1 &lt; 2
```

**Side Effect:** None


## Map <a name="map"></a>


### __map_key_set ::: Map Key Set <a name="__map_key_set"></a>

Sets N keys, like __format, but with no formatting

**Go:** UDN_MapKeySet

**Input:** Map

**Args:**

  0. String (variadic) :: Key/field to set in the Map
  1. Any (variadic) :: Value to set in the Map key/field

**Output:** Map

**Example:**

```
__input.{name=Bob}.__map_key_set.job.Programmer
```

**Result:**

```
{name=Bob,job=Programmer}
```

**Side Effect:** None

**Related Functions:** [__map_key_delete](#__map_key_delete)

### __map_key_delete ::: Map Key Delete <a name="__map_key_delete"></a>

Deletes N keys

**Go:** UDN_MapKeySet

**Input:** Map

**Args:**

  0. String (variadic) :: Key/field to delete in the Map

**Output:** Map

**Example:**

```
__input.{name=Bob,job=Programming}.__map_key_delete.job
```

**Result:**

```
{name=Bob}
```

**Side Effect:** None

**Related Functions:** [__map_key_set](#__map_key_set)

### __map_copy ::: Map Copy <a name="__map_copy"></a>

Creates a new Map which is a copy/clone of the current one, so you can modify it without changing the original

**Go:** UDN_MapCopy

**Input:** Map

**Args:**

  0. String (variadic) :: Key/field to delete in the Map

**Output:** Map

**Example:**

```
__input.{name=Bob,job=Programming}.__map_copy
```

**Result:**

```
{name=Bob,job=Programming}
```

**Side Effect:** None


### __map_update ::: Map Update <a name="__map_update"></a>

Creates a new Map which is a copy/clone of the current one, so you can modify it without changing the original

**Go:** UDN_MapUpdate

**Input:** Map

**Args:**

  0. String (variadic) :: Key/field to delete in the Map

**Output:** Map

**Example:**

```
__input.{name=Bob}.__map_update.{job=Programming}
```

**Result:**

```
{name=Bob,job=Programming}
```

**Side Effect:** None

## Array <a name="array"></a>


### __array_append ::: Array Append <a name="__array_append"></a>

Appends the input into the specified target location (args)

**Go:** UDN_ArrayAppend

**Input:** Array

**Args:**

  0. Any :: Item to append into the array

**Output:** Array

**Example:**

```
__input.[1,2,3].__set.test
__input.4.__array_append.test
```

**Result:**

```
[1,2,3,4]
```

**Side Effect:** None


### __array_slice ::: Array Slice <a name="__array_slice"></a>

Splits the array based on the start and end index (args)

**Go:** UDN_ArraySlice

**Input:** Array

**Args:**

  0. Int :: Start index (can be positive or negative)
  1. Int :: End index (can be positive or negative) - if end index not provided then end index is assumed to be end of array
  
Note: for positive indices the end index is non-inclusive. For negative indices the start index is non inclusive. Also, for positive indices the first element of the array is at 0. For negative indices the last element is at -1.

**Output:** Array Slice based on start & end index

**Example:**

```
__input.[1,2,3,4,5,6].__array_slice.0.6
```

**Result:**

```
[1,2,3,4,5,6]
```

**Example 2:**

```
__input.[1,2,3,4,5,6].__array_slice.-2.-1
```

**Result:**

```
[6]
```

**Example 3:**

```
__input.[1,2,3,4,5,6].__array_slice.-7.-1
```

**Result:**

```
[1,2,3,4,5,6]
```

**Side Effect:** None


### __array_divide ::: Array Divide <a name="__array_divide"></a>

Breaks an array up into a set of arrays, based on a divisor.  Ex: divide=4, a 14 item array will be 4 arrays, of 4/4/4/2 items each.

**Go:** UDN_ArrayDivide

**Input:** Array

**Args:**

  0. Integer :: "Columns" to break up the "Row" of the Array, into many "Rows" of max "Column"

**Output:** Array

**Example:**

```
__input.[1,2,3,4].__array_divide.2
```

**Result:**

```
[[1,2],[3,4]]
```

**Side Effect:** None


### __array_map_remap ::: Array Map Remap <a name="__array_map_remap"></a>

Takes an array of maps, and makes a new array of maps, based on the arg[0] (map) mapping (key_new=key_old)

**Go:** UDN_ArrayMapRemap

**Input:** Array of Maps

**Args:**

  0. Map :: Keys of this map will be replaced in every Map in the Array with the value

**Output:** Array of Maps

**Example:**

```
__input.[{age=10},{age=20}].__array_map_remap.{age=8}
```

**Result:**

```
[{age=8},{age=8}]
```

**Side Effect:** None

## Rendering <a name="rendering"></a>

### __widget ::: Execute UDN from String <a name="__widget"></a>

All widgets are cached in memory, this just accesses that cache and returns the Widget string.

**Go:** UDN_Widget

**Input:** Ignored

**Args:**

  0. string :: Name of widget

**Output:** String

**Example:**

```
__widget.button
```

**Result:**

```
<button type="button" class="btn btn-{{index .Map "color"}}" onclick="{{index .Map "onclick"}}"><i class="{{index .Map "icon"}} position-left"></i> {{index .Map "value"}}</button>
```

**Side Effect:** Any


### __render_data ::: Render Data Widget <a name="__render_data"></a>

Renders a Data Widget Instance.  Performs all the operations needed to render a Data Widget Instance to a web page via an API call, or other accessing method.

**Go:** UDN_RenderDataWidgetInstance

**Input:** Ignored

**Args:**

  0. Integer :: web_data_widget_instance.id
  1. Map :: A map to update the "widget_instance" Global Data, to include external data in the rendering process

**Output:** String

**Example:**

```
__render_data.dialog_target.34.{control=(__get.param.data.__json_decode)}
```

**Result:**

```
...HTML/CSS/JS...
```

**Side Effect:** Any



## User <a name="user"></a>

### __login ::: LDAP User Login <a name="__login"></a>

Authenticates against LDAP server

**Go:** UDN_Login

**Input:** Ignored

**Args:**

  0. String :: User name
  1. String :: Password

**Output:** String

**Example:**

```
__login.bob.pass
```

**Result:**

```
0tE44fJhc8Ne81jsILc6TuUZCkX
```

**Side Effect:** None


## Special <a name="special"></a>

### __ddd_render ::: Render DDD Widget Editor Dialog <a name="__ddd_render"></a>

Returns HTML/CSS/JS necessary to render a dialog editing window for DDD spec data.

**Go:** UDN_RenderDataWidgetInstance

**Input:** Ignored

**Args:**

  0. String :: DOM Target ID
  1. Int64 :: web_data_widget_instance.id
  2. Map :: Widget Instance Update Map
  3. Map (optional):: UDN Update Map

**Output:** String

**Example:**

```
__ddd_render.'0'.0.0.0.(__get.temp.item.ddd_id).'temp.item.static_data_json'.(__get.temp.item.static_data_json).0
```

**Result:**

```
...HTML/CSS/JS...
```

**Side Effect:** None


## Debugging <a name="debugging"></a>

### ____debug_output ::: Debug Output Printing  <a name="__debug_output"></a>

**Go:** UDN_QueryById

**Input:** Any

**Args:** None

**Output:** Pass Through Input

**Example:**

```
__debug_output
```

**Side Effect:** Prints input to the debug log


## Comments <a name="comments"></a>

### __comments ::: UDN Comments <a name="__comment"></a>

**Go:** UDN_Comment

**Input:** Any

**Args:** Any

**Output:** Pass Through Input

**Example:**

```
__comment.hello.this is a comment
```

**Side Effect:** None
