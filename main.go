package main

import (
	"fmt"
	"github.com/fdefabricio/gtfs2postgis/config"
	"github.com/fdefabricio/gtfs2postgis/query"
	"os"
	"path"
	"runtime"
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
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("No caller information")
	}
	dir := path.Dir(filename)

	if err := reader.DownloadFile("gtfs.zip", "https://transitfeeds.com/p/trafiklab/50/latest/download"); err != nil {
		panic(err)
	}
	Unzip("gtfs.zip", "./gtfs")

	err := repo.Connect(conf.Database)
	if err != nil {
		panic(err)
	}

	err = repo.PopulateTableGeom("agency", fmt.Sprintf("%s/gtfs/agency.txt", dir))
	if err != nil {
		panic(err)
	}
	err = repo.PopulateTableGeom("calendar_dates", fmt.Sprintf("%s/gtfs/calendar_dates.txt", dir))
	if err != nil {
		panic(err)
	}
	err = repo.PopulateTableGeom("routes", fmt.Sprintf("%s/gtfs/routes.txt", dir))
	if err != nil {
		panic(err)
	}
	err = repo.PopulateTableGeom("stops", fmt.Sprintf("%s/gtfs/stops.txt", dir))
	if err != nil {
		panic(err)
	}

	err = repo.PopulateTable("trips", fmt.Sprintf("%s/gtfs/trips.txt", dir))
	if err != nil {
		panic(err)
	}

	err = repo.PopulateTable("stop_times", fmt.Sprintf("%s/gtfs/stop_times.txt", dir))
	if err != nil {
		panic(err)
	}
	err = os.RemoveAll("./gtfs")
	err = os.Remove("./gtfs.zip")
	if err != nil {
		panic(err)
	}

}
