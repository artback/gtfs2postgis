package importer

import (
	"fmt"
	"github.com/allbin/gtfs2postgis/config"
	"github.com/allbin/gtfs2postgis/filehandling"
	"github.com/allbin/gtfs2postgis/query"
	"github.com/allbin/gtfs2postgis/slack"
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

func Run() {
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
	_, err := repo.CreatePostgis()
	if err != nil {
		panic(err)
	}
	text := repo.PopulateTable("agency", "./gtfs/agency.txt") +
		repo.PopulateTable("calendar_dates", "./gtfs/calendar_dates.txt") +
		repo.PopulateTable("routes", "./gtfs/routes.txt") +
		repo.PopulateTable("stops", "./gtfs/stops.txt") +
		repo.PopulateTable("trips", "./gtfs/trips.txt") +
		repo.PopulateTable("stop_times", "./gtfs/stop_times.txt")

	slack.SendMessage(text)
	os.RemoveAll("./gtfs")
	os.Remove("./gtfs.zip")
}
