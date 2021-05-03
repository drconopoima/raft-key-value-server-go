// Package store provides a distributed key-value store. Keys and values are
// changed by the distributed Raft consensus algorithm, Hashicorp implementation.
// Values are changed only when a majority of nodes in the cluster agree on
// the new value.

package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	cmap "github.com/orcaman/concurrent-map"
)

type command struct {
	Action string `json:"op,omitempty"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
}
type syncMap struct {
	dataMap map[string]string
	rwmutex sync.RWMutex
}

type Store struct {
	InMemory      bool
	RaftDirectory string
	RaftAddress   string
	RaftPort      string

	rwmutex sync.RWMutex

	raft *raft.Raft // The consensus mechanism

	data cmap.ConcurrentMap

	logger hclog.Logger

	RetainSnapshotCount int
	RaftTimeout         time.Duration
}

type storeSnapshot struct {
	store map[string]string
}

// New creates and returns a new Store instance by reference
func New() *Store {
	return &Store{
		InMemory:            false,
		RaftDirectory:       "./",
		RaftAddress:         "",
		RaftPort:            "9080",
		rwmutex:             sync.RWMutex{},
		data:                cmap.New(),
		logger:              hclog.Default(),
		RetainSnapshotCount: 2,
		RaftTimeout:         10 * time.Second,
	}
}

// private methods of Store
type fsm Store

// Store.StartRaft configures a raft server
// localID Server identifier for this node
// singleNode Enables single node mode at launch, therefore node becomes leader automatically
// inMemory Whether Raft algorithm stored in memory-only, without filesystem database storage
func (self *Store) StartRaft(localID string, singleNode, inMemory bool) error {
	// Setup Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(localID)
	localAddress := self.RaftAddress + ":" + self.RaftPort
	// Setup Raft communication.
	address, err := net.ResolveTCPAddr("tcp", localAddress)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(localAddress, address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if inMemory {
		self.InMemory = true
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		self.InMemory = false
		stableDB, err := raftboltdb.NewBoltStore(filepath.Join(self.RaftDirectory, "stable", "stable.db"))
		if err != nil {
			return fmt.Errorf("Error when creating new stable bolt store: %v", err)
		}
		logDB, err := raftboltdb.NewBoltStore(filepath.Join(self.RaftDirectory, "log", "log.db"))
		if err != nil {
			return fmt.Errorf("Error when creating new log bolt store: %v", err)
		}
		logStore = logDB
		stableStore = stableDB
	}
	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(filepath.Join(self.RaftDirectory, "snaps"), 5, self.logger)
	if err != nil {
		return fmt.Errorf("Error when creating new snapshot store: %v", err)
	}
	// Instantiate the Raft systems.
	raftInstance, err := raft.NewRaft(raftConfig, (*Store)(self), logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("Error when instantiating Raft Consensus: %v", err)
	}

	self.raft = raftInstance

	if singleNode {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		self.raft.BootstrapCluster(configuration)
	}

	// Watch the leader election forever.
	leaderCh := self.raft.LeaderCh()
	go func() {
		for {
			select {
			case isLeader := <-leaderCh:
				if isLeader {
					log.Printf("Cluster leadership acquired")
					// snapshot at random
					chance := rand.Int() % 10
					if chance == 0 {
						self.raft.Snapshot()
					}
				}
			}
		}
	}()

	return nil
}

// Apply function to apply a Raft log entry to the key-value store.
func (self *Store) Apply(raftLog *raft.Log) interface{} {
	var command command
	if err := json.Unmarshal(raftLog.Data, &command); err != nil {
		panic(fmt.Sprintf("Error: Failed to unmarshal command %v. %v", raftLog.Data, err))
	}

	switch command.Action {
	case "set":
		self.localStoreSet(command.Key, command.Value)
		return nil
	case "delete":
		self.localStoreDelete(command.Key)
		return nil
	default:
		panic(fmt.Sprintf("Unrecognized command action: %v", command.Action))
	}
}

// Set: Establish a provided value for specified key
func (self *Store) Set(key, value string) error {
	if self.raft.State() != raft.Leader {
		return fmt.Errorf("Set Error: Not leader")
	}

	cmd := &command{
		Action: "set",
		Key:    key,
		Value:  value,
	}
	marshaledJson, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	result := self.raft.Apply(marshaledJson, self.RaftTimeout)
	return result.Error()
}

// Get: Retrieve value at specified key
func (self *Store) Get(key string) string {
	valueInterface, ok := self.data.Get(key)
	if !ok {
		return ""
	}
	value, ok := valueInterface.(string)
	if !ok {
		return ""
	}
	return value
}

// Delete: Remove a provided key:value pair
func (self *Store) Delete(key string) error {
	if self.raft.State() != raft.Leader {
		return fmt.Errorf("Delete Error: Not leader")
	}

	cmd := &command{
		Action: "delete",
		Key:    key,
	}
	marshaledJson, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	result := self.raft.Apply(marshaledJson, self.RaftTimeout)
	return result.Error()
}

// Remove a key-value pair from local store.
func (self *Store) localStoreDelete(key string) {
	self.data.Remove(key)
}

// Set a key-value pair at local store.
func (self *Store) localStoreSet(key, value string) {
	self.data.Set(key, value)
	return
}

// Snapshot returns a snapshot of the key-value store.
func (self *Store) Snapshot() (raft.FSMSnapshot, error) {
	snapshot_map := make(map[string]string)
	// Clone the map.
	for item := range self.data.Iter() {
		valueInterface := item.Val
		valueString := valueInterface.(string)
		snapshot_map[item.Key] = valueString
	}
	return &storeSnapshot{store: snapshot_map}, nil
}

// Restore Recovers a previous state of the key-value store from snapshot.
func (self *Store) Restore(rc io.ReadCloser) error {
	map_restore := syncMap{
		make(map[string]string),
		sync.RWMutex{},
	}
	if err := json.NewDecoder(rc).Decode(&map_restore.dataMap); err != nil {
		return err
	}

	// Set the state from the snapshot
	waitGroupData := sync.WaitGroup{}
	map_restore.rwmutex.RLock()
	for key, value := range map_restore.dataMap {
		waitGroupData.Add(1)
		go self.setWorker(key, value, &self.data, &waitGroupData)
	}
	waitGroupData.Wait()
	map_restore.rwmutex.RUnlock()
	return nil
}

func (self *Store) setWorker(key, value string, data *cmap.ConcurrentMap, wg *sync.WaitGroup) error {
	defer wg.Done()
	data.Set(key, value)
	return nil
}

func (self *storeSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		marshaledJson, err := json.Marshal(self.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(marshaledJson); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (self *storeSnapshot) Release() {}
