package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

const filePath = "config/config.yaml"

type Configuration struct {
	Database DatabaseConfiguration `yaml:"database"`
	Host     HostConfiguration     `yaml:host`
}
type HostConfiguration struct {
	Url string `yaml:url`
}

type DatabaseConfiguration struct {
	Driver   string `yaml:"driver"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"dbname"`
}

var c *Configuration

func Init(co *Configuration) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(data, &co)

	return err
}
