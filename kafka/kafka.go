package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func KafkaInit() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"47.98.225.138:9092"}, config)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Println("Error closing consumer", err)
		}
	}()
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}

	partitionList, err := consumer.Partitions("web_log") // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)

	var wg sync.WaitGroup
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	for partition := range partitionList {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			pc, err := consumer.ConsumePartition("web_log", partition, sarama.OffsetNewest)
			if err != nil {
				log.Printf("Failed to start consumer for partition %d: %v", partition, err)
				return
			}
			defer pc.AsyncClose()

			// 异步从每个分区消费信息
			for {
				select {
				case msg := <-pc.Messages():
					fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
					Consume(msg.Value)
				case <-signals:
					log.Printf("Interrupt is detected in partition %d", partition)
					return
				}
			}
		}(int32(partition))
	}

	wg.Wait()
}
