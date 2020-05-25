package message

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type Service struct {
	Url string
}

func (s Service) Send(message interface{}) (*http.Response, error) {
	data, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return http.Post(s.Url, "application/json", bytes.NewBuffer(data))
}

type SlackMessage struct {
	Text string `json:"text"`
}
