package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
)

type Service struct {
	store    *Store
	listener net.Listener
}

func (s *Service) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	if strings.HasPrefix(path, "/get") {
		parts := strings.Split(path, "/get/")
		key := parts[len(parts)-1]

		val, err := s.GetItem(key)
		if err != nil {
			res.Write([]byte(err.Error()))
		}
		res.Write([]byte(val))

	} else if strings.HasPrefix(path, "/set") {
		m := make(map[string]string)
		if err := json.NewDecoder(req.Body).Decode(&m); err != nil {
			log.Println(err)
		}

		key, ok := m["key"]
		if !ok {
			log.Println("Missing key in the request")
			return
		}

		val, ok := m["val"]
		if !ok {
			log.Println("Missing value in the request")
			return
		}

		if err := s.PutItem(key, val); err != nil {
			res.Write([]byte(err.Error()))
		}
	} else if strings.HasPrefix(path, "/join") {
    log.Println("Join request received")
		data := make(map[string]string)
		err := json.NewDecoder(req.Body).Decode(&data)
		if err != nil {
			res.Write([]byte(err.Error()))
			return
		}
		addr, ok := data["addr"]
		if !ok {
			res.Write([]byte("Did not find key 'addr'"))
			return
		}

		id, ok := data["id"]
		if !ok {
			res.Write([]byte("Did not find key 'id'"))
			return
		}

		if err := s.JoinCluster(id, addr); err != nil {
			res.Write([]byte(err.Error()))
		}
	} else {
		res.Write([]byte("Unknown endpoint reached"))
	}
}

func NewService(nodeId, serviceAddr, raftAddr, joinServiceAddr string) *Service {

	listener, err := net.Listen("tcp", serviceAddr)
	if err != nil {
		log.Fatal(err)
	}

	store := NewStore(raftAddr, nodeId)
	store.Open(joinServiceAddr == "")

	return &Service{
		store:    store,
		listener: listener,
	}
}

func (s *Service) Start() {
	server := http.Server{
		Addr: s.listener.Addr().String(),
	}

	http.Handle("/", s)

	if err := server.Serve(s.listener); err != nil {
		log.Fatal(err)
	}
}

func (s *Service) GetItem(key string) (string, error) {
	return s.store.GetItem(key)
}

func (s *Service) PutItem(key, val string) error {
	return s.store.PutItem(key, val)
}

func (s *Service) JoinCluster(nodeID, addr string) error {
	return s.store.Join(nodeID, addr)
}
