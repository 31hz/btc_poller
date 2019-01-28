package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// type definitions

type Poll struct {
	Uri         string `json:"uri"`
	IntervalSec int    `json:"interval_sec"`
}

type DB struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
}

type Config struct {
	Poll `json:"poll"`
	DB   `json:"DB"`
}

// global configs
var allConfig Config

func main() {

	// coordination between routines
	PollingFinishedChan := make(chan struct{}, 1)

	// process command line flags
	configPtr := flag.String("config", "poller.json", "path to config file")

	flag.Parse()

	log.Printf("Using config file %s\n", *configPtr)

	if !ReadConfig(&allConfig, configPtr) {
		log.Fatal("Failure reading config file, stopping")
	}

	log.Printf("Proceeding with config settings\n")

	// Define the connection string
	conn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		allConfig.DB.User,
		allConfig.DB.Password,
		allConfig.DB.Host,
		allConfig.DB.Port,
		allConfig.DB.DBName)

	// Initialize the connection
	db, err := sql.Open("mysql", conn)

	// Do this eventually
	defer db.Close()

	// So far so good?
	if err != nil {
		log.Println(err)
	} else {
		log.Println("DB init OK")

		go pollUntilDone(db, &allConfig.Poll, PollingFinishedChan)
	}

	// wait for polling to finish

	log.Printf("Main: waiting for poll to finish\n")
	<-PollingFinishedChan

	log.Printf("Exiting\n")
}

// pollUntilDone will repeatedly call doPoll() on the specified interval
func pollUntilDone(db *sql.DB, settings *Poll, closingChan chan struct{}) {

	// poll ticker
	pollTicker := time.NewTicker(time.Duration(allConfig.Poll.IntervalSec) * time.Second)

	for {
		doPoll(db, settings)
		<-pollTicker.C

		// TODO: add interrupt logic, then: close(closingChan)
	}
}

// doPoll does a single poll cycle to retrieve the samples and insert them into the DB
func doPoll(db *sql.DB, settings *Poll) {

	type unpackSample struct {
		Success bool `json:"success"`
		Ticker  struct {
			Base   string  `json:"base"`
			Target string  `json:"target"`
			Price  float64 `json:"price,string"`
			Volume float64 `json:"volume,string"`
			Change float64 `json:"change,string"`
		} `json:"ticker"`
	}

	var oneSample unpackSample

	log.Printf("polling with uri=%s\n", settings.Uri)

	request, _ := http.NewRequest("GET", settings.Uri, nil)

	response, _ := http.DefaultClient.Do(request)

	defer response.Body.Close()

	payload, _ := ioutil.ReadAll(response.Body)

	log.Printf("Received payload: %s\n", string(payload))

	err := json.Unmarshal(payload, &oneSample)
	if err != nil {
		log.Println(err)
	}

	if oneSample.Success {
		insertOneResult(db, oneSample.Ticker.Base, oneSample.Ticker.Target, oneSample.Ticker.Price, oneSample.Ticker.Volume, oneSample.Ticker.Change)
	}
}

// insertOneResult will add one record to the database
// returns true if successful, false otherwise
func insertOneResult(db *sql.DB, ticker string, currency string, price float64, volume float64, delta float64) bool {

	returnResult := false

	insertPrep, err := db.Prepare("insert into prices(ticker,currency,price,volume,delta) values (?,?,?,?,?)")

	if err != nil {
		log.Println(err)
	} else {
		log.Println("Prepared insert OK")
	}

	// Execute the insert
	result, err := insertPrep.Exec(ticker, currency, price, volume, delta)

	if err != nil {
		log.Println(err)
	} else {
		log.Println("DB exec OK")

		numAffected, errAffected := result.RowsAffected()

		if errAffected != nil {
			log.Println(err)
		} else {
			returnResult = true
			log.Printf("Affected rows=%d\n", numAffected)
		}
	}
	return returnResult
}

// ReadConfig reads the config file from path, parses
// the json and populates settings.
// returns true if all required settings were set
func ReadConfig(settings *Config, path *string) bool {
	result := false

	// slurp the whole file; it should be small
	jsonBytes, err := ioutil.ReadFile(*path)

	if err == nil {

		unmErr := json.Unmarshal(jsonBytes, settings)

		if unmErr != nil {
			log.Println(unmErr)
		}
		// set result if all required are present
		if (settings.DB.Host != "") && (settings.DB.Port > 0) && (settings.DB.User != "") && (settings.DB.Password != "") && (settings.DB.DBName != "") && (settings.Poll.IntervalSec > 0) && (settings.Poll.Uri != "") {
			result = true
		}
	}
	return result
}
