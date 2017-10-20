package yudien

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"testing"
)

var benchResult interface{}

func BenchmarkUDN(b *testing.B) {
	// DB Web
	db_web, err := sql.Open("postgres", "user=postgres dbname=opsdb password='password' host=localhost sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db_web.Close()

	// Test the UDN Processor
	udn_schema := PrepareSchemaUDN(db_web)
	//fmt.Printf("\n\nUDN Schema: %v\n\n", udn_schema)

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
			b.Fatalf("Unable to read input for test %s.%s: %v", testDir, info.Name(), err)
		}
		if err := json.Unmarshal([]byte(queryBytes), testCase); err != nil {
			b.Fatalf("Unable to load queries for test %s.%s: %v", testDir, info.Name(), err)
		}

		relFilePath, err := filepath.Rel(testDir, fpath)
		if err != nil {
			b.Fatalf("Error getting relative path? Shouldn't be possible: %v", err)
		}

		b.Run(relFilePath, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				benchResult = ProcessSingleUDNTarget(db_web, udn_schema, testCase.Statement, testCase.Input, testCase.UdnData)
			}
		})
		return nil
	}

	if err := filepath.Walk(testDir, walkFunc); err != nil {
		b.Errorf("Error walking: %v", err)
	}
}
