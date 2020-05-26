package importer

import (
	"fmt"
	"github.com/allbin/gtfs2postgis/config"
	"github.com/allbin/gtfs2postgis/filehandling"
	"github.com/allbin/gtfs2postgis/message"
	"github.com/allbin/gtfs2postgis/query"
	"os"
)

const gtfsDest = "./gtfs/"
const gtfsZip = "gtfs.zip"

var fileNames = []string{
	"agency.txt",
	"calendar_dates.txt",
	"routes.txt",
	"stops.txt",
	"trips.txt",
	"stop_times.txt",
}

func Run() {
	conf := new(config.Configuration)
	repo := new(query.Repository)
	if err := config.Init(conf); err != nil {
		panic(err)
	}
	if err := repo.Connect(conf.Database); err != nil {
		panic(err)
	}

	fmt.Printf("Downloading from %s\n", conf.Host.Url)
	file := filehandling.File{
		Path: gtfsZip,
	}
	if err := file.LoadDataFrom(conf.Host.Url); err != nil {
		panic(err)
	}
	defer os.Remove(gtfsZip)

	if _, err := file.Unzip(gtfsDest); err != nil {
		panic(err)
	}
	defer os.RemoveAll(gtfsDest)

	fmt.Println("GTFS downloaded and unzipped")

	if _, err := repo.CreatePostgis(); err != nil {
		panic(err)
	}

	var text string
	for _, file := range fileNames {
		if t, err := repo.PopulateTable(gtfsDest + file); err != nil {
			panic(err)
		} else if t != nil {
			text += *t
			fmt.Println(*t)
		}
	}

	s := message.Service{Url: conf.Slack.Url}
	m := message.SlackMessage{Text: text}
	s.Send(m)
}
