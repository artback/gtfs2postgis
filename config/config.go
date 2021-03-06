package config

import (
	"github.com/ilyakaznacheev/cleanenv"
)

const filePath = "config/config.yaml"

type Configuration struct {
	Database DatabaseConfiguration `yaml:"database"`
	Host     HostConfiguration     `yaml:"host"`
	Slack    SlackConfiguration    `yaml:"slack"`
}
type HostConfiguration struct {
	Url string `yaml:"url"`
}

type DatabaseConfiguration struct {
	Driver   string `yaml:"driver" env-default:"postgres"`
	Host     string `yaml:"host" env:"POSTGRES_HOST" `
	Port     int    `yaml:"port" env:"POSTGRES_PORT" `
	User     string `yaml:"user" env:"POSTGRES_USER" `
	Password string `yaml:"password" env:"POSTGRES_PASSWORD" `
	Database string `yaml:"dbname" env:"POSTGRES_DB" `
}
type SlackConfiguration struct {
	Url string `yaml:"url" env:"SLACK_URL"`
}

func Init(co *Configuration) error {
	return cleanenv.ReadConfig(filePath, co)
}
