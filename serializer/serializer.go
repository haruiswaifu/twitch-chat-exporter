package serializer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gempir/go-twitch-irc/v2"
)

type messageToSave struct {
	Text     string `json:"message"`
	Time     string `json:"time"`
	Username string `json:"username"`
	Channel  string `json:"channel"`
}

func ToLine(m twitch.PrivateMessage) (string, error) {
	time, channel := m.Time, m.Channel

	mess := messageToSave{
		Text:     m.Message,
		Time:     time.Format("02/01/06 15:04:05 MST"),
		Username: m.User.Name,
		Channel:  channel,
	}
	marshalledMessage, err := json.Marshal(mess)
	if err != nil {
		errMessage := fmt.Sprintf("failed to marshal message: %s", err)
		return "", errors.New(errMessage)
	}
	return string(marshalledMessage), nil
}
