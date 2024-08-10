package kafka

import (
	"encoding/json"
	"fmt"
	"log"
)

type KafkaMessage struct {
	Action string `json:"action"`
	IP     string `json:"ip"`
	Topic  string `json:"topic"`
}

const (
	ACTION_ADD    = "Add"
	ACTION_DELETE = "Delete"
)

func Consume(msgByte []byte) {
	msg := &KafkaMessage{}
	err := json.Unmarshal(msgByte, msg)
	if err != nil {
		log.Println(err)
	}
	switch msg.Action {
	case ACTION_ADD:
		fmt.Println("msg.Action ADD", msg)
	case ACTION_DELETE:
		fmt.Println("msg.Action DELETE", msg)
	default:
		fmt.Println("msg type is wrong", msg)
	}

}
