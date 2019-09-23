package main

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"net/http"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"strings"
	"strconv"
	"os"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type PrometheusNodeMetrics struct {
    nodeName string
    cpuFrequencyHertz  float64     
    occupiedMemoryPercentage float64
    networkPacketsReceived int  //adapter for ubuntu is enp3s0f1 and for raspberries is eth0
	networkPacketsSent int
	networkBandwith float64
    diskIONow int 
}

type Iperf struct {
Title     string     `json:"title"`    
Start     Start      `json:"start"`    
Intervals []Interval `json:"intervals"`
End       End        `json:"end"`      
}

type End struct {
Streams               []Stream              `json:"streams"`                
SumSent               SumReceived           `json:"sum_sent"`               
SumReceived           SumReceived           `json:"sum_received"`           
CPUUtilizationPercent CPUUtilizationPercent `json:"cpu_utilization_percent"`
}

type CPUUtilizationPercent struct {
HostTotal    float64 `json:"host_total"`   
HostUser     float64 `json:"host_user"`    
HostSystem   float64 `json:"host_system"`  
RemoteTotal  float64 `json:"remote_total"` 
RemoteUser   float64 `json:"remote_user"`  
RemoteSystem float64 `json:"remote_system"`
}

type Stream struct {
Sender   SumReceived `json:"sender"`  
Receiver SumReceived `json:"receiver"`
}

type SumReceived struct {
Socket        *int64  `json:"socket,omitempty"`     
Start         float64 `json:"start"`                
End           float64 `json:"end"`                  
Seconds       float64 `json:"seconds"`              
Bytes         int64   `json:"bytes"`                
BitsPerSecond float64   `json:"bits_per_second"`      
Retransmits   *int64  `json:"retransmits,omitempty"`
SndCwnd       *int64  `json:"snd_cwnd,omitempty"`   
Omitted       *bool   `json:"omitted,omitempty"`    
}

type Interval struct {
Streams []SumReceived `json:"streams"`
Sum     SumReceived   `json:"sum"`    
}

type Start struct {
Connected     []Connected  `json:"connected"`      
Version       string       `json:"version"`        
SystemInfo    string       `json:"system_info"`    
Timestamp     Timestamp    `json:"timestamp"`      
ConnectingTo  ConnectingTo `json:"connecting_to"`  
Cookie        string       `json:"cookie"`         
TCPMssDefault int64        `json:"tcp_mss_default"`
TestStart     TestStart    `json:"test_start"`     
}

type Connected struct {
Socket     int64  `json:"socket"`     
LocalHost  string `json:"local_host"` 
LocalPort  int64  `json:"local_port"` 
RemoteHost string `json:"remote_host"`
RemotePort int64  `json:"remote_port"`
}

type ConnectingTo struct {
Host string `json:"host"`
Port int64  `json:"port"`
}

type TestStart struct {
Protocol   string `json:"protocol"`   
NumStreams int64  `json:"num_streams"`
Blksize    int64  `json:"blksize"`    
Omit       int64  `json:"omit"`       
Duration   int64  `json:"duration"`   
Bytes      int64  `json:"bytes"`      
Blocks     int64  `json:"blocks"`     
Reverse    int64  `json:"reverse"`    
}

type Timestamp struct {
Time     string `json:"time"`    
Timesecs int64  `json:"timesecs"`
}

const SchedulerName = "netAwareScheduler"

type CustomScheduler struct {
	clientset  *kubernetes.Clientset
	podQueue   chan *v1.Pod
	nodeLister listersv1.NodeLister
}

func main() {

	podQueue := make(chan *v1.Pod, 300)
	defer close(podQueue)

	quit := make(chan struct{})
	defer close(quit)

	CustomScheduler := initKubernetesScheduler(podQueue, quit)
	CustomScheduler.Run(quit)
}

func (s *CustomScheduler) Run(quit chan struct{}) {
	wait.Until(s.Schedule, 0, quit)
}

func initKubernetesScheduler(podQueue chan *v1.Pod, quit chan struct{}) CustomScheduler {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	return CustomScheduler{
		clientset:  clientset,
		podQueue:   podQueue,
		nodeLister: initializeInformers(clientset, podQueue, quit),
	}
}

func initializeInformers(clientset *kubernetes.Clientset, podQueue chan *v1.Pod, quit chan struct{}) listersv1.NodeLister {
	informersList := informers.NewSharedInformerFactory(clientset, 0)
	podList := informersList.Core().V1().Pods()
	podList.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return
			}
			if pod.Spec.NodeName == "" && pod.Spec.SchedulerName == SchedulerName {
				podQueue <- pod
			}
		},
	})
	nodeList := informersList.Core().V1().Nodes()
	nodeList.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				return
			}
			log.Printf("%s", node.GetName())
		},
	})
	informersList.Start(quit)
	return nodeList.Lister()
}

func (s *CustomScheduler) Schedule() {

	p := <-s.podQueue
	node, err := s.findNodesThatFit(p)
	if err != nil {
		return
	}
	err = s.clientset.CoreV1().Pods(p.Namespace).Bind(&v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node,
		},
	})
	if err != nil {
		return
	}

	event := fmt.Sprintf("Assigned pod %s to %s\n", p.Name, node)

	timestamp := time.Now().UTC()
	_, err := s.clientset.CoreV1().Events(p.Namespace).Create(&v1.Event{
		Count:          1,
		Message:        event,
		Reason:         "Scheduled",
		LastTimestamp:  metav1.NewTime(timestamp),
		FirstTimestamp: metav1.NewTime(timestamp),
		Type:           "Normal",
		Source: v1.EventSource{
			Component: SchedulerName,
		},
		InvolvedObject: v1.ObjectReference{
			Kind:      "Pod",
			Name:      p.Name,
			Namespace: p.Namespace,
			UID:       p.UID,
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: p.Name + "-",
		},
	})
	if err != nil {
		return err
	}
}

func (s *CustomScheduler) findNodesThatFit(pod *v1.Pod) (string, error) {
	nodes, err := s.nodeLister.List(labels.Everything())
	if err != nil {
		return "", err
	}
	priorities := s.prioritize(nodes, pod)
	return s.findBestNode(priorities), nil
}

func (s *CustomScheduler) prioritize(nodes []*v1.Node, pod *v1.Pod) map[string]int {
	
	nodeMetricsMap := make(map[string]PrometheusNodeMetrics)
	nodePriorities := make(map[string]int)
	nodePriorities["ubuntu"] = 0
	nodePriorities["raspiworker0"] = 0
	nodePriorities["raspiworker1"] = 0
	nodePriorities["raspiworker2"] = 0
	nodePriorities["raspiworker3"] = 0

	sampleMetrics := PrometheusNodeMetrics {
		nodeName: "sample",
		cpuFrequencyHertz :99999999999,  
		occupiedMemoryPercentage : 99999999999,
		networkPacketsReceived : 99999999999,
		networkPacketsSent :99999999999,
		diskIONow :999,
	}

	bestCpuNode := "none"
	bestMemNode := "none"
	bestNetSentNode := "none"
	bestNetRecNode := "none"
	bestNetBandwith := "none"
	bestDiskNode := "none"


	ubuntuBody := makeHttpRequest("http://192.168.1.137:9100/metrics")   /* master node */
	raspiworker0Body:= makeHttpRequest("http://192.168.1.132:9100/metrics")
	raspiworker1Body := makeHttpRequest("http://192.168.1.135:9100/metrics")
	raspiworker2Body := makeHttpRequest("http://192.168.1.133:9100/metrics")
	raspiworker3Body := makeHttpRequest("http://192.168.1.134:9100/metrics")
	
	nodeMetricsMap["ubuntu"] = PrometheusNodeMetrics  {
        nodeName: "ubuntu",
        cpuFrequencyHertz :getCurrentCPUUsage(ubuntuBody),  
        occupiedMemoryPercentage :getOccupiedMemoryPercentage(ubuntuBody),  //TODO make this a MACRO
        networkPacketsReceived :getNetworkPacketsReceived(ubuntuBody, "ubuntu"),  //Also TODO, make only 1 hettprequest
		networkPacketsSent :getNetworkPacketsSent(ubuntuBody, "ubuntu"),
		networkBandwith: 0.0,
        diskIONow :getDiskIONow(ubuntuBody, "ubuntu"),
    }


    nodeMetricsMap["raspiworker0"] = PrometheusNodeMetrics  {
        nodeName: "raspiworker0",
        cpuFrequencyHertz : getCurrentCPUUsage(raspiworker0Body),  
        occupiedMemoryPercentage :getOccupiedMemoryPercentage(raspiworker0Body),
        networkPacketsReceived :getNetworkPacketsReceived(raspiworker0Body, "raspiworker0"),
		networkPacketsSent :getNetworkPacketsSent(raspiworker0Body, "raspiworker0"),
		networkBandwith: getNetworkBandwith("raspiworker0"),
        diskIONow :getDiskIONow(raspiworker0Body, "raspiworker0"),
    }

    nodeMetricsMap["raspiworker1"] = PrometheusNodeMetrics  {
        nodeName: "raspiworker1",
        cpuFrequencyHertz :getCurrentCPUUsage(raspiworker1Body),  
        occupiedMemoryPercentage :getOccupiedMemoryPercentage(raspiworker1Body),
        networkPacketsReceived :getNetworkPacketsReceived(raspiworker1Body, "raspiworker1"),
		networkPacketsSent :getNetworkPacketsSent(raspiworker1Body, "raspiworker1"),
		networkBandwith: getNetworkBandwith("raspiworker1"),
        diskIONow :getDiskIONow(raspiworker1Body, "raspiworker1"),
    }


    nodeMetricsMap["raspiworker2"] = PrometheusNodeMetrics  {
        nodeName: "raspiworker2",
        cpuFrequencyHertz :getCurrentCPUUsage(raspiworker2Body),  
        occupiedMemoryPercentage :getOccupiedMemoryPercentage(raspiworker2Body),
        networkPacketsReceived :getNetworkPacketsReceived(raspiworker2Body, "raspiworker2"),
		networkPacketsSent :getNetworkPacketsSent(raspiworker2Body, "raspiworker2"),
		networkBandwith: getNetworkBandwith("raspiworker2"),
        diskIONow :getDiskIONow(raspiworker2Body, "raspiworker2"),
	}

	nodeMetricsMap["raspiworker3"] = PrometheusNodeMetrics  {
        nodeName: "raspiworker3",
        cpuFrequencyHertz :getCurrentCPUUsage(raspiworker3Body),  
        occupiedMemoryPercentage :getOccupiedMemoryPercentage(raspiworker3Body),
        networkPacketsReceived :getNetworkPacketsReceived(raspiworker3Body, "raspiworker3"),
		networkPacketsSent :getNetworkPacketsSent(raspiworker3Body, "raspiworker3"),
		networkBandwith: getNetworkBandwith("raspiworker3"),
        diskIONow :getDiskIONow(raspiworker3Body, "raspiworker3"),
    }


	for node, nodeStats := range nodeMetricsMap {
		if nodeStats.cpuFrequencyHertz < sampleMetrics.cpuFrequencyHertz {
			sampleMetrics.cpuFrequencyHertz = nodeStats.cpuFrequencyHertz
			bestCpuNode = node
		}
		if nodeStats.occupiedMemoryPercentage < sampleMetrics.occupiedMemoryPercentage {
			sampleMetrics.occupiedMemoryPercentage = nodeStats.occupiedMemoryPercentage
			bestMemNode = node
		}
		if nodeStats.networkPacketsReceived < sampleMetrics.networkPacketsReceived {
			sampleMetrics.networkPacketsReceived = nodeStats.networkPacketsReceived
			bestNetRecNode = node
		}
		if nodeStats.networkPacketsSent < sampleMetrics.networkPacketsSent {
			sampleMetrics.networkPacketsSent = nodeStats.networkPacketsSent
			bestNetSentNode = node
		}
		if nodeStats.networkBandwith > sampleMetrics.networkBandwith {
			sampleMetrics.networkBandwith = nodeStats.networkBandwith
			bestNetSentNode = node
		}
		if nodeStats.diskIONow < sampleMetrics.diskIONow && nodeStats.diskIONow != 0{
			sampleMetrics.diskIONow = nodeStats.diskIONow
			bestDiskNode = node
		}
	}
	nodePriorities[bestCpuNode]+=3
	nodePriorities[bestMemNode]+=2
	nodePriorities[bestNetSentNode]+=1
	nodePriorities[bestNetRecNode]+=1
	nodePriorities[bestNetBandwith]+=3
	nodePriorities[bestDiskNode]+=1
	
	return nodePriorities
}

func (s *CustomScheduler) bindPod(p *v1.Pod, node string) error {
	return s.clientset.CoreV1().Pods(p.Namespace).Bind(&v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node,
		},
	})
}

func (s *CustomScheduler) findBestNode(priorities map[string]int) string {
	var maxP int
	var bestNode string
	for node, p := range priorities {
		if p > maxP {
			maxP = p
			bestNode = node
		}
	}
	return bestNode
}

func makeHttpRequest(address string)  string {
	resp, err := http.Get(address)
    if err != nil {
        println(err)
    }

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        println(err)
	}
	return string(body)
}

func getCurrentCPUUsage(body string) float64 {

    substring := body[strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"0\"}")+42:
		strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"1\"}")-1]

    substring1 := body[strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"1\"}")+42:
        strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"2\"}")-1]

    substring2 := body[strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"2\"}")+42:
		strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"3\"}")-1]

    substring3 := body[strings.Index(string(body),"node_cpu_scaling_frequency_hertz{cpu=\"3\"}")+42:
        strings.Index(string(body),"# HELP node_cpu_scaling_frequency_max_hrts")-1]
  
    i, err:= strconv.ParseFloat(substring, 32)
    if err != nil {
        println("Error while parsing float!")
    }
    i1, err1:= strconv.ParseFloat(substring1, 32)
    if err1 != nil {
        println("Error while parsing float!")
    }   
    i2, err2:= strconv.ParseFloat(substring2, 32)
    if err2 != nil {
        println("Error while parsing float!")
    }   
    i3, err3:= strconv.ParseFloat(substring3, 32)
    if err3 != nil {
		println("Error while parsing float!")
		i3 = i2  //TODO this assignment is due to the existence of 8 processors in the master node:
				 //the script fails whie parsing the last number
    }
    return (i + i1 + i2 + i3) / 4
}

func getOccupiedMemoryPercentage(body string) float64 {

    substring := body[strings.Index(string(body),"gauge\nnode_memory_MemTotal_bytes")+33:
		strings.Index(string(body),"# HELP node_memory_Mlocked_bytes")-1]

    substring1 := body[strings.Index(string(body),"gauge\nnode_memory_MemAvailable_bytes")+37:
		strings.Index(string(body),"# HELP node_memory_MemFree_bytes")-1]
    
    i, err:= strconv.ParseFloat(substring, 32)
    if err != nil {
        println("Error while parsing float!")
    }
    i1, err1:= strconv.ParseFloat(substring1, 32)
    if err1 != nil {
        println("Error while parsing float!")
	}
	return 100 - ((i1 * 100) / i)
    }

func getNetworkPacketsSent(body string, node string) int {

	var substring string
	if node == "ubuntu" {// master, interface is enp3s0f1
		substring = body[strings.Index(string(body),"node_network_transmit_packets_total{device=\"enp3s0f1\"}")+55:
			strings.Index(string(body),"node_network_transmit_packets_total{device=\"flannel.1\"}")-1]
	} else { //raspi, interface is eth0
		substring = body[strings.Index(string(body),"node_network_transmit_packets_total{device=\"eth0\"}")+51:
			strings.Index(string(body),"node_network_transmit_packets_total{device=\"flannel.1\"}")-1]
	}

    i, err:= strconv.Atoi(substring)
    if err != nil {
		println("Error while parsing integer!")
		return 0
	}
	return i
}

func getNetworkPacketsReceived(body string, node string) int {

	var substring string
	if node == "ubuntu" { // master, interface is enp3s0f1
		substring = body[strings.Index(string(body),"node_network_receive_packets_total{device=\"enp3s0f1\"}")+54:
			strings.Index(string(body),"node_network_receive_packets_total{device=\"flannel.1\"}")-1]
	} else { //raspi, interface is eth0
		substring = body[strings.Index(string(body),"node_network_receive_packets_total{device=\"eth0\"}")+50:
			strings.Index(string(body),"node_network_receive_packets_total{device=\"flannel.1\"}")-1]
	}

    i, err:= strconv.Atoi(substring)
    if err != nil {
		println("Error while parsing integer!")
		return 0
	}
	
	return i
}


func getNetworkBandwith(node string) float64 {

	nodeNames := make(map[string]string)

	nodeNames["raspimaster"] =  "/home/192.168.1.133.json"
	nodeNames["raspiworker0"] = "/home/192.168.1.135.json"
	nodeNames["raspiworker1"] = "/home/192.168.1.133.json"
	nodeNames["raspiworker2"] = "/home/192.168.1.134.json"
	// Open our jsonFile
	jsonFile, err := os.Open(nodeNames[node])
	// if we os.Open returns an error then handle it
	if err != nil {
		println(err)
	}
	println("json loaded")

	bytes, _ := ioutil.ReadAll(jsonFile)
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	iperfValues, err := UnmarshalIperf(bytes)

	println(iperfValues.End.Streams[0].Sender.BitsPerSecond)
	println(iperfValues.End.Streams[0].Receiver.BitsPerSecond)

	return iperfValues.End.Streams[0].Receiver.BitsPerSecond

}

func getDiskIONow(body string, node string) int {

	var substring string
	if node == "ubuntu" { // master, disk is sda
		substring = body[strings.Index(string(body),"node_disk_io_now{device=\"sda\"}")+31:
			strings.Index(string(body),"node_disk_io_now{device=\"sr0\"}")-1]
	} else { //raspi, disk is mmcblk0
		substring = body[strings.Index(string(body),"node_disk_io_now{device=\"mmcblk0\"}")+35:
			strings.Index(string(body),"node_disk_io_now{device=\"mmcblk0p1\"}")-1]
	}

    i, err:= strconv.Atoi(substring)
    if err != nil {
		println("Error while parsing integer!")
		return 0
	}
	return i
}

func UnmarshalIperf(data []byte) (Iperf, error) {
	var r Iperf
	err := json.Unmarshal(data, &r)
	return r, err
}
	
func (r *Iperf) Marshal() ([]byte, error) {
	return json.Marshal(r)
}