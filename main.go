package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"time"
)
import "gopkg.in/alecthomas/kingpin.v2"

var app = kingpin.New("kafkadog", "kafka dog")
var (
	producer  = app.Command("producer", "kafka producer")
	topic     = producer.Flag("topic", "topic").Short('t').Required().String()
	message   = producer.Flag("message", "消息内容").Short('m').Required().String()
	count     = producer.Flag("count", "发送数量").Short('c').Default("1").Int()
	partition = producer.Flag("partition", "分区").Short('p').Default("0").Int()
	host      = producer.Flag("host", "host").Short('b').Required().String()
)
var (
	consumer          = app.Command("consumer", "kafka consumer")
	topicConsumer     = consumer.Flag("topic", "topic").Short('t').Required().String()
	partitionConsumer = consumer.Flag("partition", "分区").Short('p').Default("0").Int()
	hostConsumer      = consumer.Flag("host", "host").Short('b').Required().String()
	offsetConsumer    = consumer.Flag("offset", "offset").Short('o').Int64()
)

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	// Register user
	case producer.FullCommand():
		Producer()
	case consumer.FullCommand():
		Consumer()
	}
}

func Consumer() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*hostConsumer},
		Topic:     *topicConsumer,
		Partition: *partitionConsumer,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	if *offsetConsumer != 0 {
		r.SetOffset(*offsetConsumer)
	}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Printf("message at offset %d: %s\n", m.Offset, string(m.Value))
	}

	r.Close()
}

func Producer() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", *host, *topic, *partition)
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	log.Println("start to write message")
	for *count > 0 {
		*count--
		conn.WriteMessages(
			kafka.Message{Value: []byte(*message)},
		)
	}
	conn.Close()
	log.Println("finish")
}
