package slack

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
)

func SendMessage(text string) {
	if os.Getenv("SLACK_URL") != "" {
		message := Message{text}
		data, err := json.Marshal(message)
		if err != nil {
			panic(err)
		}
		_, err = http.Post(os.Getenv("SLACK_URL"), "application/json", bytes.NewBuffer(data))
		if err != nil {
			panic(err)
		}
	}
}

type Message struct {
	Text string `json:"text"`
}
