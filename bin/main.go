package main

import (
	"fmt"
	"github.com/artback/gtfs2postgis/config"
	"github.com/artback/gtfs2postgis/filehandling"
	"github.com/artback/gtfs2postgis/query"
	"github.com/artback/gtfs2postgis/slack"
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
	if err := repo.Connect(conf.Database); err != nil {
		panic(err)
	}

	if err := filehandling.DownloadFile("gtfs.zip", conf.Host.Url); err != nil {
		panic(err)
	}
	if _, err := filehandling.Unzip("gtfs.zip", "./gtfs"); err != nil {
		panic(err)
	}
	fmt.Println("GTFS downloaded and unzipped")

	text := repo.PopulateTable("agency", "./gtfs/agency.txt") + "\n"

	text += repo.PopulateTable("calendar_dates", "./gtfs/calendar_dates.txt") + "\n"

	text += repo.PopulateTable("routes", "./gtfs/routes.txt") + "\n"

	text += repo.PopulateTable("stops", "./gtfs/stops.txt") + "\n"

	text += repo.PopulateTable("trips", "./gtfs/trips.txt") + "\n"

	text += repo.PopulateTable("stop_times", "./gtfs/stop_times.txt") + "\n"
	slack.SendMessage(text)

	os.RemoveAll("./gtfs")
	os.Remove("./gtfs.zip")
}
