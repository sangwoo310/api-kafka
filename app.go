package main

import (
	"api-kafka/service"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

type Config struct {
	Ip			string			`yaml:"ip" json:"ip"`
	Port		string			`yaml:"port" json:"port"`
}

func main() {
	// System init mode variable
	var mode int
	// kafka address variable
	var kAddress string
	// kafka topic variable
	var kTopic string
	// kafka topic variable
	var kOffset int64

	// prompt for system init mode
	fmt.Printf("Select mode (1:produce, 2:consume, 3:create-topic):")
	fmt.Scanln(&mode)

	// Set service address
	prompt("Enter service address: ", &kAddress)

	// Set service topic
	prompt("Enter service Topic: ", &kTopic)

	ks := service.KafkaSetting{
		Address: kAddress,
		Topic:   kTopic,
		Offset:  kOffset,
	}

	// Get config setting
	conf, err := getConfig()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// Set rpc server
	if mode == 1 {
		var produceMode int
		prompt("Select Produce mode (1:General mode, 2:JSON file read mode): ", &produceMode)

		var jsonFilePath string

		if produceMode == 1 {
			log.Println("Selected General mode")
		} else if produceMode == 2 {
			prompt("Enter JSON file path: ", &jsonFilePath)
		}

		kafkaProducer := service.KafkaProducer{
			KafkaSetting: &ks,
			JsonFilePath: jsonFilePath,
		}

		// Create a new RPC server
		s := rpc.NewServer()    // Register the type of data requested as JSON
		s.RegisterCodec(json.NewCodec(), "application/json")

		// Register the service by creating a new JSON server
		if err := s.RegisterService(&kafkaProducer, ""); err != nil {
			fmt.Println(err)
		}

		// RPC server open
		r := mux.NewRouter()
		r.Handle("/rpc", s)
		fmt.Println("Server is open :", conf.Ip + ":" + conf.Port)

		address := conf.Ip + ":" + conf.Port
		if err := http.ListenAndServe(address, r); err != nil {
			log.Println(err)
		}
	} else if mode == 2 {
		// Start consumer
		ks.Consumer()
	}
}

func getConfig() (*Config,error) {
	data, err := ioutil.ReadFile(filepath.Join("./config","config.yaml"))
	if err != nil {
		fmt.Println("Error on read config")
		fmt.Println("Config path is ./config/config.yaml")
		return nil, err
	}

	conf := &Config{}
	if err := yaml.Unmarshal(data, conf); err != nil {
		fmt.Println("Error on unmarshal config file")
		return nil, err
	}
	return conf, nil
}

func prompt(message string, input interface{}) {
	fmt.Printf(message)
	fmt.Scanln(input)
}
