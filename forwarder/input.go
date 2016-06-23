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

package forwarder

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"

	metro "github.com/boncheff/fluentd-forwarder/metro"
	"github.com/ugorji/go/codec"
)

func Start(producer *metro.LogHandler, listener net.TCPListener, acceptChan chan *net.TCPConn, c codec.MsgpackHandle) {
	spawnAcceptor(listener, acceptChan)
	spawnDaemon(listener, acceptChan, c)
}

func spawnAcceptor(listener net.TCPListener, acceptChan chan *net.TCPConn) {
	go func() {

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
				if conn != nil {
					dec := codec.NewDecoder(bufio.NewReader(conn), &c)
					startHandling(dec) // required a decoder
				}
			}
		}
	}()
}

func startHandling(dec *codec.Decoder) {
	go func() {
		for {
			recordSets, _ := decodeEntries(dec)

			if len(recordSets) > 0 {
				for _, item := range recordSets {
					for _, rec := range item.Records {
						for key, value := range rec.Data {
							d := string(value.([]uint8))
							fmt.Printf("TAG: %s,  TIMESTAMP: %s, %s=>%s \n", item.Tag, rec.Timestamp, key, d)
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
		return nil, err
	}
	tag, ok := v[0].([]byte)
	if !ok {
		return nil, errors.New("Failed to decode tag field")
	}

	var retval []FluentRecordSet
	switch timestamp_or_entries := v[1].(type) {
	case int64:
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
