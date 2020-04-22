package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/artback/gtfs2postgis/config"
	"github.com/artback/gtfs2postgis/filehandling"
	"github.com/artback/gtfs2postgis/query"
	"log"
	"net/http"
	"os"
)

var (
	conf *config.Configuration
	repo *query.Repository
)

func init() {
	conf = new(config.Configuration)
	repo = new(query.Repository)
	err := config.Init(conf)
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := filehandling.DownloadFile("gtfs.zip", conf.Host.Url); err != nil {
		panic(err)
	}
	_, err := filehandling.Unzip("gtfs.zip", "./gtfs")
	if err != nil {
		panic(err)
	}
	fmt.Println("GTFS downloaded and unzipped")
	err = repo.Connect(conf.Database)
	var text string
	if err != nil {
		panic(err)
	}

	line, err := repo.PopulateTable("agency", "./gtfs/agency.txt")
	if err != nil {
		panic(err)
	}
	if line != nil {
		text = text + *line + "\n"
	}

	line, err = repo.PopulateTable("calendar_dates", "./gtfs/calendar_dates.txt")
	if line != nil {
		text = text + *line + "\n"
	}
	if err != nil {
		panic(err)
	}
	line, err = repo.PopulateTable("routes", "./gtfs/routes.txt")
	if err != nil {
		panic(err)
	}
	if line != nil {
		text = text + *line + "\n"
	}
	line, err = repo.PopulateTable("stops", "./gtfs/stops.txt")
	if err != nil {
		panic(err)
	}

	if line != nil {
		text = text + *line + "\n"
	}
	line, err = repo.PopulateTable("trips", "./gtfs/trips.txt")
	if err != nil {
		panic(err)
	}

	if line != nil {
		text = text + *line + "\n"
	}
	line, err = repo.PopulateTable("stop_times", "./gtfs/stop_times.txt")
	if err != nil {
		panic(err)
	}

	if line != nil {
		text = text + *line + "\n"
	}
	os.RemoveAll("./gtfs")
	os.Remove("./gtfs.zip")
	if os.Getenv("SLACK_URL") != "" {
		message := message{text}
		data, err := json.Marshal(message)
		if err != nil {
			panic(err)
		}
		resp, err := http.Post(os.Getenv("SLACK_URL"), "application/json", bytes.NewBuffer(data))
		if err != nil {
			panic(err)
		}
	}
}

type message struct {
	text string `json:"text"`
}
