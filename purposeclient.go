package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"strings"
)
import "fmt"

type PurposeClient struct {
	mqttClient mqtt.Client
	beQuiet    bool
}

type PurposeSet struct {
	aip []string
	pip []string
}

func CreatePurposeClient(server string, beQuiet bool) *PurposeClient {

	var co = mqtt.NewClientOptions().AddBroker(server).SetClientID("purpose_bench_prepare")
	var mc = mqtt.NewClient(co)

	if token := mc.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if !beQuiet {
		fmt.Println("Connected to " + server)
	}

	var pc = PurposeClient{
		mqttClient: mc,
		beQuiet:    beQuiet,
	}

	return &pc
}

func (pc *PurposeClient) MakeSetting(setting string, val bool) {
	settingsTopic := "!PBAC/SET/" + setting
	var text string
	if val {
		text = "true"
	} else {
		text = "false"
	}
	pc.publish(settingsTopic, text)
}

func (pc *PurposeClient) Reset() {
	pc.publish("!PBAC/RESET", "")
}

func (pc *PurposeClient) Reserve(topic string, aip PurposeSet) {
	resTopic := fmt.Sprintf("!RESERVE/%s%s", topic, packPurposes(aip))
	pc.publish(resTopic, "")
}

func (pc *PurposeClient) MakeReservations(nres int) {
	aip := PurposeSet{
		aip: []string{"research", "benchmarking"},
		pip: []string{"benchmarking/other"},
	}

	for i := 0; i < nres; i++ {
		pc.Reserve(fmt.Sprintf("reserved/%d", i), aip)
	}
}

func packPurposes(aip PurposeSet) string {
	return fmt.Sprintf("{%s|%s}",
		strings.Join(aip.aip, ","),
		strings.Join(aip.pip, ","),
	)
}

func (pc *PurposeClient) SetMode(mode string) {
	FoP := strings.Contains(mode, "FoP")
	FoS := strings.Contains(mode, "FoS")
	Hbr := strings.Contains(mode, "Hbr")
	// NoF will result in none of them set
	TreeStore := !strings.Contains(mode, "Flat")
	Cache := strings.Contains(mode, "Cache")

	pc.setModeFlags(FoP, FoS, Hbr, TreeStore, Cache)

}

func (pc *PurposeClient) setModeFlags(FoP, FoS, Hbr, TreeStore, Cache bool) {
	pc.MakeSetting("filter_on_publish", FoP)
	pc.MakeSetting("filter_on_subscribe", FoS)
	pc.MakeSetting("filter_hybrid", Hbr)
	pc.MakeSetting("use_tree_store", TreeStore)
	pc.MakeSetting("cache_reservations", Cache)
	pc.MakeSetting("cache_subscriptions", Cache)
}

func (pc *PurposeClient) publish(topic string, payloadString string) {
	if !pc.beQuiet {
		fmt.Printf("sending %s to %s \n", payloadString, topic)
	}
	payload := []byte(payloadString)
	pc.mqttClient.Publish(topic, 2, false, payload)
}

func (pc *PurposeClient) Disconnect() {
	pc.mqttClient.Disconnect(1000)
}
