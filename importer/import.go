package importer

import (
	"fmt"
	"github.com/allbin/gtfs2postgis/config"
	"github.com/allbin/gtfs2postgis/filehandling"
	"github.com/allbin/gtfs2postgis/message"
	"github.com/allbin/gtfs2postgis/query"
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
	text := repo.PopulateTable("./gtfs/agency.txt") +
		repo.PopulateTable("./gtfs/calendar_dates.txt") +
		repo.PopulateTable("./gtfs/routes.txt") +
		repo.PopulateTable("./gtfs/stops.txt") +
		repo.PopulateTable("./gtfs/trips.txt") +
		repo.PopulateTable("./gtfs/stop_times.txt")

	s := message.Service{Url: conf.Slack.Url}
	m := message.SlackMessage{Text: text}
	s.Send(m)

	os.RemoveAll("./gtfs")
	os.Remove("./gtfs.zip")
}
