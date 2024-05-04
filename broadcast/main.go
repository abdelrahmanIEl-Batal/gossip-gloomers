package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type Server struct {
	node     *maelstrom.Node
	messages []int
	topology map[string][]string
}

func InitServer() *Server {
	s := &Server{
		node:     maelstrom.NewNode(),
		messages: make([]int, 0),
		topology: make(map[string][]string),
	}

	s.node.Handle("read", s.Read)
	s.node.Handle("topology", s.Topology)
	s.node.Handle("broadcast", s.Broadcast)
	s.node.Handle("broadcast_ok", s.BroadcastOk)
	return s
}

func (s *Server) Read(request maelstrom.Message) error {

	type Reply struct {
		Type     string `json:"type"`
		Messages []int  `json:"messages"`
	}

	// Echo the original message back with the updated message type.
	return s.node.Reply(request, Reply{
		Type:     "read_ok",
		Messages: s.messages,
	})
}

func (s *Server) Topology(request maelstrom.Message) error {

	type Body struct {
		Topology map[string][]string `json:"topology"`
	}

	var reqBody Body

	if err := json.Unmarshal(request.Body, &reqBody); err != nil {
		return err
	}

	s.topology = reqBody.Topology

	type Reply struct {
		Type string `json:"type"`
	}

	return s.node.Reply(request, Reply{
		Type: "topology_ok",
	})
}

func (s *Server) BroadcastOk(request maelstrom.Message) error {
	// when someone replies they got message then do nth
	return nil
}

func (s *Server) Broadcast(request maelstrom.Message) error {

	type Body struct {
		MessageID int `json:"msg_id"`
		Message   int `json:"message"`
	}

	var reqBody Body

	if err := json.Unmarshal(request.Body, &reqBody); err != nil {
		return nil
	}

	// if sent before do nth
	if s.sentBefore(reqBody.Message) {
		return nil
	}

	s.messages = append(s.messages, reqBody.Message)
	// make sure we don't sent message more than one and avoid loops
	// fmt.Printf("node ids are %v", s.node.NodeIDs())
	// fmt.Printf("node ids are %v", s.topology)
	for _, neighbour := range s.topology[s.node.ID()] {
		// test without handling err
		if neighbour == request.Src {
			continue
		}

		s.node.Send(neighbour, struct {
			Type    string `json:"type"`
			Message int    `json:"message"`
		}{
			Type:    "broadcast",
			Message: reqBody.Message,
		})

	}

	return s.node.Reply(request, struct {
		Type string `json:"type"`
	}{
		Type: "broadcast_ok",
	})
}

func (s *Server) sentBefore(message int) bool {
	for _, v := range s.messages {
		if v == message {
			return true
		}
	}
	return false
}

func main() {

	server := InitServer()

	err := server.node.Run()

	if err != nil {
		log.Fatal(err)
	}
}
