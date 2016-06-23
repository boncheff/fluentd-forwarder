fluentd-forwarder
=================

A minimalist Fluentd-to-Kafka forwarder written in Go based on the excellent fluentd-forwarder found here <https://github.com/fluent/fluentd-forwarder>

Requirements
------------

- Go v1.4.1 or above
- Set the $GOPATH environment variable to get `fluentd_forwarder`
  under `$GOPATH/bin` directory.

Build Instructions
------------------

To install the required dependencies and build `fluentd_forwarder` do:

```
$ go get github.com/boncheff/fluentd-forwarder/forwarder/entrypoints/build_fluentd_forwarder
$ bin/build_fluentd_forwarder fluentd_forwarder
```

Running `fluentd_forwarder`
---------------------------

```
$ $GOPATH/bin/fluentd_forwarder
```
It simply listens on 127.0.0.1:24224 and forwards the events to Kafka using a SaramaAsyncProducer

```

Dependencies
------------

fluentd_forwarder depends on the following external libraries:

* github.com/ugorji/go/codec
* github.com/op/go-logging
* github.com/jehiah/go-strftime
* github.com/moriyoshi/go-ioextras
* github.com/Sirupsen/logrus 
* github.com/Shopify/sarama
* github.com/kelseyhightower/envconfig
* gopkg.in/gcfg.v1


License
-------

The source code and its object form ("Work"), unless otherwise specified, are licensed under the Apache Software License, Version 2.0.  You may not use the Work except in compliance with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.

A portion of the code originally written by Moriyoshi Koizumi and later modified by Treasure Data, Inc. continues to be published and distributed under the same terms and conditions as the MIT license, with its authorship being attributed to the both parties.  It is specified at the top of the applicable source files.
