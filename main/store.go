package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type Log struct {
	Key   string `json:"key"`
	Value string `json:"val"`
}

type FSMSnapshot struct {
	data map[string]string
}

type Store struct {
	data     map[string]string
	raft     *raft.Raft
	mutex    sync.Mutex
	raftBind string
	nodeID   string
}

func NewStore(raftBind, nodeID string) *Store {
	return &Store{
		data:     make(map[string]string),
		nodeID:   nodeID,
		raftBind: raftBind,
	}
}

func (s *Store) Open(doBootstrap bool) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.nodeID)

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	addr, err := net.ResolveTCPAddr("tcp", s.raftBind)
	if err != nil {
		log.Println("Unable to resolve TCP", err)
		return
	}
	// Raft is listening on `localBind`
	// Raft is informing others to communicate on `addr`
	transport, err := raft.NewTCPTransport(s.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Println("Unable to set up TCP transport layer", err)
		return
	}

	node, err := raft.NewRaft(config, s, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
		return
	}

	if doBootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		if future := node.BootstrapCluster(config); future.Error() != nil {
			log.Fatal(future.Error())
			return
		}
	}

	s.raft = node

	log.Println("Store in open")

}

func (s *Store) Apply(raftLog *raft.Log) interface{} {
	var l Log
	if err := json.Unmarshal(raftLog.Data, &l); err != nil {
		log.Println("Unable to read log data")
		return errors.New("Error while reading log")
	}

	log.Println(l)

	s.mutex.Lock()
	s.data[l.Key] = l.Value
	s.mutex.Unlock()

	return nil
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	m := make(map[string]string)

	s.mutex.Lock()
	for k, v := range s.data {
		m[k] = v
	}
	s.mutex.Unlock()

	return &FSMSnapshot{data: m}, nil
}

func (s *Store) Restore(snapshot io.ReadCloser) error {
	m := make(map[string]string)
	if err := json.NewDecoder(snapshot).Decode(&m); err != nil {
		log.Println("Error while restore from snapshot")
		return err
	}

	s.data = m

	return nil
}

func (s *Store) GetItem(key string) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	val, ok := s.data[key]
	if !ok {
		return "", errors.New("Key does not exist")
	}

	return val, nil
}

func (s *Store) PutItem(key, value string) error {
	if s.raft.State() != raft.Leader {
		log.Println("Not a leader")
		return errors.New("Not a leader")
	}

	log.Println(s.raft)
	l := Log{Key: key, Value: value}
	byteStream, err := json.Marshal(l)
	if err != nil {
		log.Println("Error while serializing item")
		return err
	}

	future := s.raft.Apply(byteStream, 10*time.Second)

	if err := future.Error(); err != nil {
		log.Println("Unable to apply the log")
		return err
	}

	return nil
}

func (s *Store) Join(nodeID, addr string) error {
	fmt.Println("Received join request")

	config := s.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		fmt.Println("Failed to get configuration")
		return err
	}

	for _, server := range config.Configuration().Servers {
		fmt.Println(server)
		if server.ID == raft.ServerID(nodeID) && server.Address == raft.ServerAddress(addr) {
			log.Printf("Node %s is already a cluster %s member\n", nodeID, addr)
			return nil
		} else if server.ID == raft.ServerID(nodeID) || server.Address == raft.ServerAddress(addr) {
			future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
			if err := future.Error(); err != nil {
				log.Printf("Failed to remove node %s\n", nodeID)
				return nil
			}
		}
	}

	future := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		log.Printf("Failed to add node %s\n", nodeID)
		return errors.New(fmt.Sprintf("Failed to add node %s\n", nodeID))
	}

	log.Printf("Successfully added node %s to the cluster\n", nodeID)

	return nil
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(f.data)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(b); err != nil {
		sink.Cancel()
		return err
	}

	sink.Close()
	return nil
}

func (f *FSMSnapshot) Release() {}
