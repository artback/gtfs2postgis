package main

import (
	"github.com/allbin/gtfs2postgis/importer"
	"github.com/jasonlvhit/gocron"
	"log"
)

func main() {
	importer.Run()
	gocron.Every(1).Day().At("23:00").Do(importer.Run)
	_, t := gocron.NextRun()
	gocron.Start()
	log.Printf("Next run scheduled at %s.", t.Format("15:04:05"))
	select {}
}
