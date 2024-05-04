package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"math/rand"
	"time"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		rand.NewSource(time.Now().UnixNano())
		// Update the message type to return back.
		body["type"] = "generate_ok"
		body["in_reply_to"] = body["msg_id"]
		body["id"] = fmt.Sprintf("%v-%v", time.Now().UnixNano(), rand.Float64())

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if n.Run() != nil {
		log.Fatal(n.Run().Error())
	}
}
