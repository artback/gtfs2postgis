package slack

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type Slack struct {
	Url string
}

func (s Slack) Send(text string) (*http.Response, error) {
	message := Message{text}
	data, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return http.Post(s.Url, "application/json", bytes.NewBuffer(data))
}

type Message struct {
	Text string `json:"text"`
}
