
# kafka-amqp-sink

[![Build Status](https://travis-ci.org/hmrc/kafka-amqp-sink.svg?branch=master)](https://travis-ci.org/hmrc/kafka-amqp-sink) [ ![Download](https://api.bintray.com/packages/hmrc/releases/kafka-amqp-sink/images/download.svg) ](https://bintray.com/hmrc/releases/kafka-amqp-sink/_latestVersion)

This is an event consumption plugin for use with Kafka. It reads events from a
list of Kafka topics, and sends them to a (currently RabbitMQ) AMQP endpoint.

## Installation

This sink can be run on any Kafka node using Kafka Connect. See the
[documentation]("http://kafka.apache.org/documentation#connect") for details.

## Configuration

Configuration parameters for this AMQP sink are added to the `connector.properties`
file and are as follows :

Property            | Default | Description
--------------------|---------|------------------------------------------------
amqpHostname        | (none)  | The AMQP hostname to connect to.
amqpPort            | (none)  | The AMQP port to connect to.
amqpUsername        | (none)  | The AMQP username to use.
amqpPassword        | (none)  | The AMQP password to use.
amqpExchange        | (none)  | The name of the AMQP exchange to send messages to.
amqpConfirmTimeout  | 5000    | Time to wait for message confirmation from AMQP before assuming the message was lost.
retryTimeout        | 5000    | Time to wait after message loss before retrying.

Additionally, the `name`, `connector.class` (uk.gov.hmrc.kafka.sink.AMQPSinkConnector),
`tasks.max`, `key.converter`, `value.converter` and `topics` properties will need
to be set, as they are required by Kafka itself.


### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
    