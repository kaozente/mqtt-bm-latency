package main

import (
	"fmt"
	"github.com/GaryBoone/GoStats/stats"
	"log"
	"strconv"
	"time"
)

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SubBenchClient struct {
	ID         int
	BrokerURL  string
	BrokerUser string
	BrokerPass string
	SubTopic   string
	AP         string
	MsgSize    int
	MsgCount   int
	SubQoS     byte
	KeepAlive  int
	Quiet      bool
}

func (c *SubBenchClient) run(res chan *PubResults) {
	runResults := new(PubResults)

	started := time.Now()
	// start generator

	runResults.ID = c.ID
	times := []float64{}

	client := c.getClient()

	for i := 0; i < c.MsgCount; i++ {

		topic := fmt.Sprintf("!AP/%s/%d/HASH{%s}", c.SubTopic, i, c.AP)
		subStarted := time.Now()

		subToken := client.Subscribe(topic, c.SubQoS, nil)
		subToken.Wait()

		duration := time.Now().Sub(subStarted).Seconds() * 1000

		if subToken.Error() == nil {
			runResults.Successes++
			times = append(times, duration)
		} else {
			log.Printf("error subscribing to %s", topic)
			runResults.Failures++
		}
	}

	client.Disconnect(250)

	totalDuration := time.Now().Sub(started)
	runResults.PubTimeMin = stats.StatsMin(times)
	runResults.PubTimeMax = stats.StatsMax(times)
	runResults.PubTimeMean = stats.StatsMean(times)
	runResults.PubTimeStd = stats.StatsSampleStandardDeviation(times)
	runResults.RunTime = totalDuration.Seconds()
	runResults.PubsPerSec = float64(runResults.Successes) / totalDuration.Seconds()

	res <- runResults
	return

}

func (c *SubBenchClient) getClient() mqtt.Client {

	ka, _ := time.ParseDuration(strconv.Itoa(c.KeepAlive) + "s")

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetKeepAlive(ka).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("SUBSCRIBE BENCHER %v lost connection to the broker: %v. Will reconnect...\n", c.ID, reason.Error())
		})

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("SUBSCRIBE BENCHER %v had error connecting to the broker: %v\n", c.ID, token.Error())
	}

	return client
}
