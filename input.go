//
// Fluentd Forwarder
//
// Copyright (C) 2014 Treasure Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package fluentd_forwarder

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"

	logging "github.com/op/go-logging"
	"github.com/ugorji/go/codec"
)

type forwardClient struct {
	input  *ForwardInput
	logger *logging.Logger
	conn   *net.TCPConn
	codec  *codec.MsgpackHandle
	dec    *codec.Decoder
}

type ForwardInput struct {
	entries        int64 // This variable must be on 64-bit alignment. Otherwise atomic.AddInt64 will cause a crash on ARM and x86-32
	port           Port
	logger         *logging.Logger
	bind           string
	listener       *net.TCPListener
	codec          *codec.MsgpackHandle
	clientsMtx     sync.Mutex
	clients        map[*net.TCPConn]*forwardClient
	wg             sync.WaitGroup
	acceptChan     chan *net.TCPConn
	shutdownChan   chan struct{}
	isShuttingDown uintptr
}

type EntryCountTopic struct{}

type ConnectionCountTopic struct{}

type ForwardInputFactory struct{}

func coerceInPlace(data map[string]interface{}) {
	for k, v := range data {
		switch v_ := v.(type) {
		case []byte:
			data[k] = string(v_) // XXX: byte => rune
		case map[string]interface{}:
			coerceInPlace(v_)
		}
	}
}

func (c *forwardClient) decodeRecordSet(tag []byte, entries []interface{}) (FluentRecordSet, error) {
	records := make([]TinyFluentRecord, len(entries))
	for i, _entry := range entries {
		entry, ok := _entry.([]interface{})
		if !ok {
			return FluentRecordSet{}, errors.New("Failed to decode recordSet")
		}
		timestamp, ok := entry[0].(uint64)
		if !ok {
			return FluentRecordSet{}, errors.New("Failed to decode timestamp field")
		}
		data, ok := entry[1].(map[string]interface{})
		if !ok {
			return FluentRecordSet{}, errors.New("Failed to decode data field")
		}
		coerceInPlace(data)
		records[i] = TinyFluentRecord{
			Timestamp: timestamp,
			Data:      data,
		}
	}
	return FluentRecordSet{
		Tag:     string(tag), // XXX: byte => rune
		Records: records,
	}, nil
}

func (c *forwardClient) shutdown() {
	err := c.conn.Close()
	if err != nil {
		c.input.logger.Infof("Error during closing connection: %s", err.Error())
	}
}

func newForwardClient(input *ForwardInput, logger *logging.Logger, conn *net.TCPConn, _codec *codec.MsgpackHandle) *forwardClient {
	c := &forwardClient{
		input:  input,
		logger: logger,
		conn:   conn,
		codec:  _codec,
		dec:    codec.NewDecoder(bufio.NewReader(conn), _codec),
	}
	input.markCharged(c)
	return c
}

func Start(listener net.TCPListener, acceptChan chan *net.TCPConn, c codec.MsgpackHandle) {
	log.Println("STARTING INPUT.GO")

	spawnAcceptor(listener, acceptChan)
	spawnDaemon(listener, acceptChan, c)
	log.Println("END OF STARTING INPUT.GO")
}

func spawnAcceptor(listener net.TCPListener, acceptChan chan *net.TCPConn) {
	log.Println("SPAWNING ACCEPTOR")
	go func() {
		log.Println("SPAWNING ACCEPTOR INSIDE GO FUNC")

		defer func() {
			close(acceptChan)
		}()

		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				log.Printf("\n\n ERROR IS %+v\n\n", err)
				break
			}
			if conn != nil {
				log.Printf("Connected from %s", conn.RemoteAddr().String())
				acceptChan <- conn
			} else {
				log.Printf("\n\n CONN IS EMPTY")
				break
			}
		}
		log.Println("ACCEPTOR ENDED")
	}()
}

func spawnDaemon(listener net.TCPListener, acceptChan chan *net.TCPConn, c codec.MsgpackHandle) {
	go func() {

		defer func() {
			close(acceptChan)
		}()

		for {
			select {
			case conn := <-acceptChan:
				log.Println("INSIDE DAEMON ACCEPTCHAN IS %+v", conn)
				if conn != nil {
					dec := codec.NewDecoder(bufio.NewReader(conn), &c)
					startHandling(dec) // required a decoder
				}
			}
		}
	}()
}

func startHandling(dec *codec.Decoder) {
	log.Println("STARTING TO HANDLE")
	go func() {
		for {
			recordSets, _ := decodeEntries(dec)

			if len(recordSets) > 0 {
				log.Println("THERE ARE MESSAGES")
				for _, item := range recordSets {
					for _, rec := range item.Records {
						for key, value := range rec.Data {
							d := string(value.([]uint8))
							fmt.Printf("TAG: %s,  TTTTTTIMESTAMP: %s, %s=>%s \n", item.Tag, rec.Timestamp, key, d)
						}
					}
				}
			}
		}
	}()
}

func decodeEntries(dec *codec.Decoder) ([]FluentRecordSet, error) {
	v := []interface{}{nil, nil, nil}
	err := dec.Decode(&v)
	if err != nil {
		log.Println("++++++++++++++++++1 %s", err)
		os.Exit(0)
		return nil, err
	}
	tag, ok := v[0].([]byte)
	if !ok {
		return nil, errors.New("Failed to decode tag field")
	}

	var retval []FluentRecordSet
	log.Println(" THE DATA HERE IS %+v", v)
	switch timestamp_or_entries := v[1].(type) {
	case int64:
		log.Println("STARTING TO DECODE ENTRIES TYPE INT64")
		timestamp := uint64(timestamp_or_entries)
		data, ok := v[2].(map[string]interface{})
		if !ok {
			log.Printf("FAILED TO DECODE DATA FIELD")
			return nil, errors.New("Failed to decode data field")
		}
		retval = []FluentRecordSet{
			{
				Tag: string(tag), // XXX: byte => rune
				Records: []TinyFluentRecord{
					{
						Timestamp: timestamp,
						Data:      data,
					},
				},
			},
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unknown type: %t", timestamp_or_entries))
	}
	return retval, nil
}

func (input *ForwardInput) markCharged(c *forwardClient) {
	input.clientsMtx.Lock()
	defer input.clientsMtx.Unlock()
	input.clients[c.conn] = c
}

func (input *ForwardInput) markDischarged(c *forwardClient) {
	input.clientsMtx.Lock()
	defer input.clientsMtx.Unlock()
	delete(input.clients, c.conn)
}

func (input *ForwardInput) String() string {
	return "input"
}

func (input *ForwardInput) WaitForShutdown() {
	input.wg.Wait()
}

func (input *ForwardInput) Stop() {
	if atomic.CompareAndSwapUintptr(&input.isShuttingDown, uintptr(0), uintptr(1)) {
		input.shutdownChan <- struct{}{}
	}
}

func NewForwardInput(logger *logging.Logger, bind string, port Port) (*ForwardInput, error) {
	_codec := codec.MsgpackHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	_codec.RawToString = false
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	return &ForwardInput{
		port:           port,
		logger:         logger,
		bind:           bind,
		listener:       listener,
		codec:          &_codec,
		clients:        make(map[*net.TCPConn]*forwardClient),
		clientsMtx:     sync.Mutex{},
		entries:        0,
		wg:             sync.WaitGroup{},
		acceptChan:     make(chan *net.TCPConn),
		shutdownChan:   make(chan struct{}),
		isShuttingDown: uintptr(0),
	}, nil
}
