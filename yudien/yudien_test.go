package yudien

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/ghowland/yudien/yudiencore"
)

var testDir = "data/udn_test_cases"

type udnTestCase struct {
	Func    string                 `json:"func"`
	Args    []interface{}          `json:"args"`
	Input   interface{}            `json:"input"`
	UdnData map[string]interface{} `json:"udn_data"`
}

func (u *udnTestCase) UnmarshalJSON(data []byte) error {
	type Alias udnTestCase
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(u),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if u.UdnData == nil {
		u.UdnData = map[string]interface{}{}
	}
	return nil
}

type udnTestCaseResult struct {
	UdnResult *yudiencore.UdnResult  `json:"udn_result"`
	Input     interface{}            `json:"input"`
	UdnData   map[string]interface{} `json:"udn_data"`
}

func TestUDN(t *testing.T) {
	// DB Web
	db_web, err := sql.Open("postgres", "user=postgres dbname=opsdb password='password' host=localhost sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db_web.Close()

	// Test the UDN Processor
	udn_schema := PrepareSchemaUDN(db_web)
	//fmt.Printf("\n\nUDN Schema: %v\n\n", udn_schema)

	// TODO: b etter
	udn_start := yudiencore.NewUdnPart()

	// Setup for the UDN stuff

	// Define function for walking over all the test cases
	walkFunc := func(fpath string, info os.FileInfo, err error) error {
		// If its a directory, skip it-- we'll let something else grab it
		if !info.IsDir() {
			return nil
		}

		testCase := &udnTestCase{}
		queryBytes, err := ioutil.ReadFile(path.Join(fpath, "input.json"))
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			t.Fatalf("Unable to read input for test %s.%s: %v", testDir, info.Name(), err)
		}
		if err := json.Unmarshal([]byte(queryBytes), testCase); err != nil {
			t.Fatalf("Unable to load queries for test %s.%s: %v", testDir, info.Name(), err)
		}

		relFilePath, err := filepath.Rel(testDir, fpath)
		if err != nil {
			t.Fatalf("Error getting relative path? Shouldn't be possible: %v", err)
		}

		t.Run(relFilePath, func(t *testing.T) {

			// Find the function
			f, ok := UdnFunctions[testCase.Func]
			if !ok {
				t.Fatalf("Unknown UDN function: %s", testCase.Func)
			}

			// Process args
			args := ProcessUdnArguments(db_web, udn_schema, &udn_start, testCase.Input, testCase.UdnData)

			ret := f(db_web, udn_schema, &udn_start, args, testCase.Input, testCase.UdnData)
			// Generate result
			result := &udnTestCaseResult{
				UdnResult: &ret,
				Input:     testCase.Input,
				UdnData:   testCase.UdnData,
			}

			// write out results
			resultPath := path.Join(fpath, "result.json")
			resultBytes, _ := json.MarshalIndent(result, "", "  ")
			ioutil.WriteFile(resultPath, resultBytes, 0644)

			// compare against baseline if it exists
			baselinePath := path.Join(fpath, "baseline.json")
			baselineResultBytes, err := ioutil.ReadFile(baselinePath)
			if err != nil {
				t.Skip("No baseline.json found, skipping comparison")
			} else {
				baselineResultBytes = bytes.TrimSpace(baselineResultBytes)
				resultBytes = bytes.TrimSpace(resultBytes)
				if !bytes.Equal(baselineResultBytes, resultBytes) {
					t.Fatalf("Mismatch of results and baseline!")
				}
			}

		})
		return nil
	}

	if err := filepath.Walk(testDir, walkFunc); err != nil {
		t.Errorf("Error walking: %v", err)
	}
}
