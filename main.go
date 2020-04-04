package main

import (
	"archive/zip"
	"fmt"
	"github.com/artback/gtfs2postgis/config"
	"github.com/artback/gtfs2postgis/query"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
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

	if err := DownloadFile("gtfs.zip", "https://transitfeeds.com/p/trafiklab/50/latest/download"); err != nil {
		panic(err)
	}
	Unzip("gtfs.zip", "./gtfs")

	err := repo.Connect(conf.Database)
	if err != nil {
		panic(err)
	}

	err = repo.PopulateTable("agency", fmt.Sprintf("%s/gtfs/agency.txt", dir))
	if err != nil {
		panic(err)
	}
	err = repo.PopulateTable("calendar_dates", fmt.Sprintf("%s/gtfs/calendar_dates.txt", dir))
	if err != nil {
		panic(err)
	}
	err = repo.PopulateTable("routes", fmt.Sprintf("%s/gtfs/routes.txt", dir))
	if err != nil {
		panic(err)
	}
	err = repo.PopulateTable("stops", fmt.Sprintf("%s/gtfs/stops.txt", dir))
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

func DownloadFile(filepath string, url string) error {

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}
func Unzip(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fpath)
		}

		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return filenames, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, err
		}
	}
	return filenames, nil
}
