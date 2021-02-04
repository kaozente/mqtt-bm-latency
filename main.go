package main

import (
	"bytes"
	"github.com/gocarina/gocsv"
	"os"
	"strconv"
	//"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"time"

	"github.com/GaryBoone/GoStats/stats"
)

// Message describes a message
type Message struct {
	Topic     string
	QoS       byte
	Payload   interface{}
	Sent      time.Time
	Delivered time.Time
	Error     bool
}

// SubResults describes results of a single SUBSCRIBER / run
type SubResults struct {
	ID             int     `json:"id"`
	Published      int64   `json:"actual_published"`
	Received       int64   `json:"received"`
	FwdRatio       float64 `json:"fwd_success_ratio"`
	FwdLatencyMin  float64 `json:"fwd_time_min"`
	FwdLatencyMax  float64 `json:"fwd_time_max"`
	FwdLatencyMean float64 `json:"fwd_time_mean"`
	FwdLatencyStd  float64 `json:"fwd_time_std"`
}

// TotalSubResults describes results of all SUBSCRIBER / runs
type TotalSubResults struct {
	TotalFwdRatio     float64 `json:"fwd_success_ratio"`
	TotalReceived     int64   `json:"successes"`
	TotalPublished    int64   `json:"actual_total_published"`
	FwdLatencyMin     float64 `json:"fwd_latency_min"`
	FwdLatencyMax     float64 `json:"fwd_latency_max"`
	FwdLatencyMeanAvg float64 `json:"fwd_latency_mean_avg"`
	FwdLatencyMeanStd float64 `json:"fwd_latency_mean_std"`
}

// PubResults describes results of a single PUBLISHER / run
type PubResults struct {
	ID          int     `json:"id"`
	Successes   int64   `json:"pub_successes"`
	Failures    int64   `json:"failures"`
	RunTime     float64 `json:"run_time"`
	PubTimeMin  float64 `json:"pub_time_min"`
	PubTimeMax  float64 `json:"pub_time_max"`
	PubTimeMean float64 `json:"pub_time_mean"`
	PubTimeStd  float64 `json:"pub_time_std"`
	PubsPerSec  float64 `json:"publish_per_sec"`
}

// TotalPubResults describes results of all PUBLISHER / runs
type TotalPubResults struct {
	PubRatio        float64 `json:"publish_success_ratio"`
	Successes       int64   `json:"successes"`
	Failures        int64   `json:"failures"`
	TotalRunTime    float64 `json:"total_run_time"`
	AvgRunTime      float64 `json:"avg_run_time"`
	PubTimeMin      float64 `json:"pub_time_min"`
	PubTimeMax      float64 `json:"pub_time_max"`
	PubTimeMeanAvg  float64 `json:"pub_time_mean_avg"`
	PubTimeMeanStd  float64 `json:"pub_time_mean_std"`
	TotalMsgsPerSec float64 `json:"total_msgs_per_sec"`
	AvgMsgsPerSec   float64 `json:"avg_msgs_per_sec"`
}

// JSONResults are used to export results as a JSON document
type JSONResults struct {
	PubRuns   []*PubResults    `json:"publish runs"`
	SubRuns   []*SubResults    `json:"subscribe runs"`
	PubTotals *TotalPubResults `json:"publish totals"`
	SubTotals *TotalSubResults `json:"receive totals"`
}

type RunData struct {
	// input
	TestSet              string
	Action 				 string
	TestIteration        int
	Server               string
	NumberOfClients      int
	NumberOfReservations int
	PubQoS               int
	SubQoS               int
	MessageSize          int
	MessageCount         int

	// settings
	Mode     string
	Aware    bool
	Scenario string

	//output
	Throughput        float64
	TotalRunTime      float64
	Failures          int64
	PubTimeMax        float64
	PubTimeMeanAvg    float64
	PubTimeMeanStd    float64
	FwdLatencyMax     float64
	FwdLatencyMeanAvg float64
	FwdLatencyMeanStd float64
}

func (rd RunData) toStr() string {
	return fmt.Sprintf(
		"%d clients x %d msg, %d reservations (%s %s #%d): %f msg/s",
		rd.NumberOfClients,
		rd.MessageCount,
		rd.NumberOfReservations,
		rd.Mode,
		rd.Scenario,
		rd.TestIteration,
		rd.Throughput,
	)
}

//type CSVResults struct {
//
//}

func main() {

	var (
		broker = flag.String("broker", "tcp://localhost:1883", "MQTT broker endpoint as scheme://host:port")
		//topic    = flag.String("topic", "/test", "MQTT topic for outgoing messages")
		//username = flag.String("username", "", "MQTT username (empty if auth disabled)")
		//password = flag.String("password", "", "MQTT password (empty if auth disabled)")
		pubqos     = flag.Int("pubqos", 1, "QoS for published messages")
		subqos     = flag.Int("subqos", 1, "QoS for subscribed messages")
		size       = flag.Int("size", 100, "Size of the messages payload (bytes)")
		count      = flag.Int("count", 100, "Number of messages to send per pubclient")
		iterations = flag.Int("iterations", 1, "number of runs per config")
		//clients     = flag.Int("clients", 2, "Number of clients pair to start")
		//keepalive = flag.Int("keepalive", 60, "Keep alive period in seconds")
		//format    = flag.String("format", "csv", "Output format: text|json")
		quiet = flag.Bool("quiet", true, "Suppress logs while running")
		aware = flag.Bool("pbac", true, "use purposes")
		//scenario = flag.String("scenario", "simple", "simple/nested")
		action = flag.String("action", "pubsub", "pubsub/subscribe/reserve")
	)

	flag.Parse()

	time.Sleep(time.Second * 1)

	var testSet = time.Now().Format("2006_01_02__15_04_05")
	var clients = []int{1,10,25, 50, 100, 250}

	var riTemplate = RunData{
		TestSet: testSet,
		Server:  *broker,
		//NumberOfClients: ci,
		SubQoS:       *subqos,
		PubQoS:       *pubqos,
		MessageSize:  *size,
		MessageCount: *count,
		Aware:        *aware,
		Action:		  *action,
		//Scenario:     *scenario,
	}

	beQuiet := *quiet

	var modes []string
	var resNums []int
	if *aware {
		modes = []string{"FoS", "Hbr", "FoP", "FoP_Flat", "FoP_Cache", "NoF"}
		resNums = []int{0, 1000, 10000} // 10000, 0, 1000}

	} else {
		modes = []string{"Hive"}
		resNums = []int{0}
	}

	scenarios := []string{"simple", "nested"}

	fmt.Println("Modes:", modes)

	runs := make([]RunData, 0)

	total_start_time := time.Now()
	total_runs := *iterations * len(modes) * len(clients) * len(scenarios) * len(resNums)
	log.Printf("starting %d runs for %s at %v", total_runs, riTemplate.Action, total_start_time)
	run_count := 0

	var fileName = fmt.Sprintf("reports/report_%s.csv", testSet)

	for i := 0; i < *iterations; i++ {
		for _, ci := range clients {
			for _, mode := range modes {
				for _, sci := range scenarios {
					for _, nres := range resNums {
						numMessages := (10 * *count / ci)
						if numMessages < *count {
							numMessages = *count
						}

						var ri = riTemplate // no need to deep copy here
						ri.Mode = mode
						ri.NumberOfClients = ci
						ri.TestIteration = i
						ri.Scenario = sci
						ri.NumberOfReservations = nres
						ri.MessageCount = numMessages
						run(&ri, beQuiet)
						run_count++
						passed := time.Now().Sub(total_start_time)
						eta := passed.Minutes() / float64(run_count) * float64(total_runs-run_count)
						log.Printf(ri.toStr())
						log.Printf("finished %d/%d, ~ %f minutes remaining ", run_count, total_runs, eta)
						runs = append(runs, ri)
					}
				}
			}
			writeCSV(runs, fileName)
		}
	}

}

func writeCSV(runs []RunData, fileName string) {
	reportFile, fileErr := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if fileErr != nil {
		fmt.Println("error opening file: ", fileErr)
		return
	}
	err := gocsv.MarshalFile(runs, reportFile)
	if err != nil {
		fmt.Println("ALARM! ", err)
	}

	fmt.Println("Wrote file: ", fileName)
}

func run(ri *RunData, beQuiet bool) {

	const topicStub = "test/go/"

	if ri.NumberOfClients < 1 {
		log.Fatal("Invalid arguments")
	}

	// settings
	if !beQuiet {
		log.Printf("Setting mode to %s \n", ri.Mode)
	}

	if ri.Aware {

		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered from creating MC", r)
				return
			}
		}()

		mc := CreatePurposeClient(ri.Server, beQuiet)
		mc.MakeReservations(ri.NumberOfReservations)
		mc.SetMode(ri.Mode)
		mc.Reset()

		aip := PurposeSet{
			aip: []string{"research", "benchmarking"},
			pip: []string{"benchmarking/other"},
		}

		if ri.Scenario == "nested" {
			mc.Reserve(topicStub+"HASH", aip)
			mc.Reserve(topicStub+"1/HASH", aip)
			mc.Reserve(topicStub+"1/2/HASH", aip)
			mc.Reserve(topicStub+"1/2/3/PLUS/1", aip)
			mc.Reserve(topicStub+"1/2/3/PLUS/2", aip)
			mc.Reserve(topicStub+"1/2/3/PLUS/3", aip)
		} else {
			mc.Reserve(topicStub+"HASH", aip)
		}
		time.Sleep(500 * time.Millisecond)
		mc.Disconnect()

		// run depending on action
		switch ri.Action {

		case "reserve":
			runRes(ri, beQuiet, topicStub)
		case "subscribe":
			runSub(ri, beQuiet, topicStub)
		default:
			ri.Action = "pubsub"
			runPubSub(ri, beQuiet, topicStub)
		}
	}

}

func runRes(ri *RunData, beQuiet bool, topicStub string) {
	//start publish
	ri.Action = "reserve"
	if !beQuiet {
		log.Printf("Starting publish..\n")
	}
	pubResCh := make(chan *PubResults)
	start := time.Now()
	for i := 0; i < ri.NumberOfClients; i++ {

		var topic string
		if ri.Scenario == "nested" {
			topic = topicStub + "1/2/3/" + "bm-" + strconv.Itoa(i) + "/b"
		} else {
			topic = topicStub + "bm-" + strconv.Itoa(i)
		}


		aip := PurposeSet{
			aip: []string{"research", "benchmarking/reservation"},
			pip: []string{"benchmarking/other"},
		}

		c := &ResBenchClient{
			ID:         i,
			BrokerURL:  ri.Server,
			BrokerUser: "",
			BrokerPass: "",
			PubTopic:   topic,
			Aip: aip,
			MsgSize:    ri.MessageSize,
			MsgCount:   ri.MessageCount,
			PubQoS:     byte(ri.PubQoS),
			KeepAlive:  60,
			Quiet:      beQuiet,
		}
		go c.run(pubResCh)
	}

	// collect the publish results
	pubresults := make([]*PubResults, ri.NumberOfClients)
	for i := 0; i < ri.NumberOfClients; i++ {
		pubresults[i] = <-pubResCh
	}
	totalTime := time.Now().Sub(start)
	pubtotals := calculatePublishResults(pubresults, totalTime)

	for i := 0; i < 3; i++ {
		time.Sleep(1 * time.Second)
		if !beQuiet {
			log.Printf("Benchmark will stop after %v seconds.\n", 3-i)
		}
	}

	ri.Throughput = pubtotals.TotalMsgsPerSec
	ri.TotalRunTime = pubtotals.TotalRunTime
	ri.Failures = pubtotals.Failures
	ri.PubTimeMax = pubtotals.PubTimeMax
	ri.PubTimeMeanAvg = pubtotals.PubTimeMeanAvg
	ri.PubTimeMeanStd = pubtotals.PubTimeMeanStd
	ri.FwdLatencyMax = 0
	ri.FwdLatencyMeanAvg = 0
	ri.FwdLatencyMeanStd = 0
}


func runSub(ri *RunData, beQuiet bool, topicStub string) {
	//start publish
	ri.Action = "subscribe"
	if !beQuiet {
		log.Printf("Starting subscribe..\n")
	}
	pubResCh := make(chan *PubResults)
	start := time.Now()
	for i := 0; i < ri.NumberOfClients; i++ {

		var topic string
		if ri.Scenario == "nested" {
			topic = topicStub + "1/2/3/" + "bm-" + strconv.Itoa(i) + "/b"
		} else {
			topic = topicStub + "bm-" + strconv.Itoa(i)
		}

		c := &SubBenchClient{
			ID:         i,
			BrokerURL:  ri.Server,
			BrokerUser: "",
			BrokerPass: "",
			SubTopic:   topic, // will be numbered + AP'ed
			AP: "test/benchmarking/subscribe",
			MsgSize:    ri.MessageSize,
			MsgCount:   ri.MessageCount,
			SubQoS:     byte(ri.PubQoS),
			KeepAlive:  60,
			Quiet:      beQuiet,
		}
		go c.run(pubResCh)
	}

	// collect the publish results
	pubresults := make([]*PubResults, ri.NumberOfClients)
	for i := 0; i < ri.NumberOfClients; i++ {
		pubresults[i] = <-pubResCh
	}
	totalTime := time.Now().Sub(start)
	pubtotals := calculatePublishResults(pubresults, totalTime)

	for i := 0; i < 3; i++ {
		time.Sleep(1 * time.Second)
		if !beQuiet {
			log.Printf("Benchmark will stop after %v seconds.\n", 3-i)
		}
	}

	ri.Throughput = pubtotals.TotalMsgsPerSec
	ri.TotalRunTime = pubtotals.TotalRunTime
	ri.Failures = pubtotals.Failures
	ri.PubTimeMax = pubtotals.PubTimeMax
	ri.PubTimeMeanAvg = pubtotals.PubTimeMeanAvg
	ri.PubTimeMeanStd = pubtotals.PubTimeMeanStd
	ri.FwdLatencyMax = 0
	ri.FwdLatencyMeanAvg = 0
	ri.FwdLatencyMeanStd = 0
}

func runPubSub(ri *RunData, beQuiet bool, topicStub string) {
	ri.Action = "pubsub"

	const keepAlive = 60

	// not used atm
	const username = ""
	const password = ""

	const format = "csv"

	//start subscribe

	subResCh := make(chan *SubResults)
	jobDone := make(chan bool)
	subDone := make(chan bool)
	subCnt := 0

	if !beQuiet {
		log.Printf("Starting subscribe..\n")
	}

	for i := 0; i < ri.NumberOfClients; i++ {

		var topic string
		if ri.Scenario == "nested" {
			topic = topicStub + "1/2/3/" + "bm-" + strconv.Itoa(i) + "/#"
		} else {
			topic = topicStub + "bm-" + strconv.Itoa(i)
		}

		sub := &SubClient{
			ID:         i,
			BrokerURL:  ri.Server,
			BrokerUser: username,
			BrokerPass: password,
			SubTopic:   topic,
			SubQoS:     byte(ri.SubQoS),
			KeepAlive:  keepAlive,
			Quiet:      beQuiet,
			Aware:      ri.Aware,
		}
		go sub.run(subResCh, subDone, jobDone)
	}

	subTimeout := time.After(5 * time.Second)

SUBJOBDONE:
	for {
		select {
		case <-subDone:
			subCnt++
			if subCnt == ri.NumberOfClients {
				if !beQuiet {
					log.Printf("all subscribe job done.\n")
				}
				break SUBJOBDONE
			}
		case <-subTimeout:
			fmt.Println("ERROR SUBSCRIBING")
			return
		}
	}

	//start publish
	if !beQuiet {
		log.Printf("Starting publish..\n")
	}
	pubResCh := make(chan *PubResults)
	start := time.Now()
	for i := 0; i < ri.NumberOfClients; i++ {

		var topic string
		if ri.Scenario == "nested" {
			topic = topicStub + "1/2/3/" + "bm-" + strconv.Itoa(i) + "/b"
		} else {
			topic = topicStub + "bm-" + strconv.Itoa(i)
		}

		c := &PubClient{
			ID:         i,
			BrokerURL:  ri.Server,
			BrokerUser: username,
			BrokerPass: password,
			PubTopic:   topic,
			MsgSize:    ri.MessageSize,
			MsgCount:   ri.MessageCount,
			PubQoS:     byte(ri.PubQoS),
			KeepAlive:  keepAlive,
			Quiet:      beQuiet,
		}
		go c.run(pubResCh)
	}

	// collect the publish results
	pubresults := make([]*PubResults, ri.NumberOfClients)
	for i := 0; i < ri.NumberOfClients; i++ {
		pubresults[i] = <-pubResCh
	}
	totalTime := time.Now().Sub(start)
	pubtotals := calculatePublishResults(pubresults, totalTime)

	for i := 0; i < 3; i++ {
		time.Sleep(1 * time.Second)
		if !beQuiet {
			log.Printf("Benchmark will stop after %v seconds.\n", 3-i)
		}
	}

	// notify subscriber that job done
	for i := 0; i < ri.NumberOfClients; i++ {
		jobDone <- true
	}

	// collect subscribe results
	subresults := make([]*SubResults, ri.NumberOfClients)
	for i := 0; i < ri.NumberOfClients; i++ {
		subresults[i] = <-subResCh
	}

	// collect the sub results
	subtotals := calculateSubscribeResults(subresults, pubresults)

	// print stats (csv report is handled separately)
	printResults(pubresults, pubtotals, subresults, subtotals, format, ri, beQuiet)

	if !beQuiet {
		log.Printf("All jobs done.\n")
	}
}

func calculatePublishResults(pubresults []*PubResults, totalTime time.Duration) *TotalPubResults {
	pubtotals := new(TotalPubResults)
	pubtotals.TotalRunTime = totalTime.Seconds()

	pubTimeMeans := make([]float64, len(pubresults))
	msgsPerSecs := make([]float64, len(pubresults))
	runTimes := make([]float64, len(pubresults))
	bws := make([]float64, len(pubresults))

	pubtotals.PubTimeMin = pubresults[0].PubTimeMin
	for i, res := range pubresults {
		pubtotals.Successes += res.Successes
		pubtotals.Failures += res.Failures
		pubtotals.TotalMsgsPerSec += res.PubsPerSec

		if res.PubTimeMin < pubtotals.PubTimeMin {
			pubtotals.PubTimeMin = res.PubTimeMin
		}

		if res.PubTimeMax > pubtotals.PubTimeMax {
			pubtotals.PubTimeMax = res.PubTimeMax
		}

		pubTimeMeans[i] = res.PubTimeMean
		msgsPerSecs[i] = res.PubsPerSec
		runTimes[i] = res.RunTime
		bws[i] = res.PubsPerSec
	}
	pubtotals.PubRatio = float64(pubtotals.Successes) / float64(pubtotals.Successes+pubtotals.Failures)
	pubtotals.AvgMsgsPerSec = stats.StatsMean(msgsPerSecs)
	pubtotals.AvgRunTime = stats.StatsMean(runTimes)
	pubtotals.PubTimeMeanAvg = stats.StatsMean(pubTimeMeans)
	pubtotals.PubTimeMeanStd = stats.StatsSampleStandardDeviation(pubTimeMeans)

	return pubtotals
}

func calculateSubscribeResults(subresults []*SubResults, pubresults []*PubResults) *TotalSubResults {
	subtotals := new(TotalSubResults)
	fwdLatencyMeans := make([]float64, len(subresults))

	subtotals.FwdLatencyMin = subresults[0].FwdLatencyMin
	for i, res := range subresults {
		subtotals.TotalReceived += res.Received

		if res.FwdLatencyMin < subtotals.FwdLatencyMin {
			subtotals.FwdLatencyMin = res.FwdLatencyMin
		}

		if res.FwdLatencyMax > subtotals.FwdLatencyMax {
			subtotals.FwdLatencyMax = res.FwdLatencyMax
		}

		fwdLatencyMeans[i] = res.FwdLatencyMean
		for _, pubres := range pubresults {
			if pubres.ID == res.ID {
				subtotals.TotalPublished += pubres.Successes
				res.Published = pubres.Successes
				res.FwdRatio = float64(res.Received) / float64(pubres.Successes)
			}
		}
	}
	subtotals.FwdLatencyMeanAvg = stats.StatsMean(fwdLatencyMeans)
	subtotals.FwdLatencyMeanStd = stats.StatsSampleStandardDeviation(fwdLatencyMeans)
	subtotals.TotalFwdRatio = float64(subtotals.TotalReceived) / float64(subtotals.TotalPublished)
	return subtotals
}

func printResults(
	pubresults []*PubResults,
	pubtotals *TotalPubResults,
	subresults []*SubResults,
	subtotals *TotalSubResults,
	format string,
	ri *RunData,
	quiet bool,
) {

	ri.Throughput = pubtotals.TotalMsgsPerSec
	ri.TotalRunTime = pubtotals.TotalRunTime
	ri.Failures = pubtotals.Failures
	ri.PubTimeMax = pubtotals.PubTimeMax
	ri.PubTimeMeanAvg = pubtotals.PubTimeMeanAvg
	ri.PubTimeMeanStd = pubtotals.PubTimeMeanStd
	ri.FwdLatencyMax = subtotals.FwdLatencyMax
	ri.FwdLatencyMeanAvg = subtotals.FwdLatencyMeanAvg
	ri.FwdLatencyMeanStd = subtotals.FwdLatencyMeanStd

	if quiet {
		return
	}

	switch format {
	case "json":
		jr := JSONResults{
			PubRuns:   pubresults,
			SubRuns:   subresults,
			PubTotals: pubtotals,
			SubTotals: subtotals,
		}
		data, _ := json.Marshal(jr)
		var out bytes.Buffer
		json.Indent(&out, data, "", "\t")

		fmt.Println(string(out.Bytes()))
	default:
		fmt.Printf("\n")
		for _, pubres := range pubresults {
			fmt.Printf("=========== PUBLISHER %d ===========\n", pubres.ID)
			fmt.Printf("Publish Success Ratio:   %.3f%% (%d/%d)\n", float64(pubres.Successes)/float64(pubres.Successes+pubres.Failures)*100, pubres.Successes, pubres.Successes+pubres.Failures)
			fmt.Printf("Runtime (s):             %.3f\n", pubres.RunTime)
			fmt.Printf("Pub time min (ms):       %.3f\n", pubres.PubTimeMin)
			fmt.Printf("Pub time max (ms):       %.3f\n", pubres.PubTimeMax)
			fmt.Printf("Pub time mean (ms):      %.3f\n", pubres.PubTimeMean)
			fmt.Printf("Pub time std (ms):       %.3f\n", pubres.PubTimeStd)
			fmt.Printf("Pub Bandwidth (msg/sec): %.3f\n", pubres.PubsPerSec)
		}
		fmt.Printf("\n")
		for _, subres := range subresults {
			fmt.Printf("=========== SUBSCRIBER %d ===========\n", subres.ID)
			fmt.Printf("Forward Success Ratio:       %.3f%% (%d/%d)\n", subres.FwdRatio*100, subres.Received, subres.Published)
			fmt.Printf("Forward latency min (ms):    %.3f\n", subres.FwdLatencyMin)
			fmt.Printf("Forward latency max (ms):    %.3f\n", subres.FwdLatencyMax)
			fmt.Printf("Forward latency std (ms):    %.3f\n", subres.FwdLatencyStd)
			fmt.Printf("Mean forward latency (ms):   %.3f\n", subres.FwdLatencyMean)
		}
		fmt.Printf("\n")
		fmt.Printf("================= TOTAL PUBLISHER (%d) =================\n", len(pubresults))
		fmt.Printf("Total Publish Success Ratio:   %.3f%% (%d/%d)\n", pubtotals.PubRatio*100, pubtotals.Successes, pubtotals.Successes+pubtotals.Failures)
		fmt.Printf("Total Runtime (sec):           %.3f\n", pubtotals.TotalRunTime)
		fmt.Printf("Average Runtime (sec):         %.3f\n", pubtotals.AvgRunTime)
		fmt.Printf("Pub time min (ms):             %.3f\n", pubtotals.PubTimeMin)
		fmt.Printf("Pub time max (ms):             %.3f\n", pubtotals.PubTimeMax)
		fmt.Printf("Pub time mean mean (ms):       %.3f\n", pubtotals.PubTimeMeanAvg)
		fmt.Printf("Pub time mean std (ms):        %.3f\n", pubtotals.PubTimeMeanStd)
		fmt.Printf("Average Bandwidth (msg/sec):   %.3f\n", pubtotals.AvgMsgsPerSec)
		fmt.Printf("Total Bandwidth (msg/sec):     %.3f\n\n", pubtotals.TotalMsgsPerSec)

		fmt.Printf("================= TOTAL SUBSCRIBER (%d) =================\n", len(subresults))
		fmt.Printf("Total Forward Success Ratio:      %.3f%% (%d/%d)\n", subtotals.TotalFwdRatio*100, subtotals.TotalReceived, subtotals.TotalPublished)
		fmt.Printf("Forward latency min (ms):         %.3f\n", subtotals.FwdLatencyMin)
		fmt.Printf("Forward latency max (ms):         %.3f\n", subtotals.FwdLatencyMax)
		fmt.Printf("Forward latency mean std (ms):    %.3f\n", subtotals.FwdLatencyMeanStd)
		fmt.Printf("Total Mean forward latency (ms):  %.3f\n\n", subtotals.FwdLatencyMeanAvg)
	}
	return
}
