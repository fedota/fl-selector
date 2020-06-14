package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	viper "github.com/spf13/viper"
	"google.golang.org/grpc"

	pbIntra "fedota/fl-selector/genproto/fl_intra"
	pbRound "fedota/fl-selector/genproto/fl_round"
)

// Selector struct to implement gRPC Round service interface
type Selector struct {
	mutex                 sync.Mutex
	selectorID            string
	coordinatorAddress    string
	flRootPath            string
	waitForConfiguration  chan bool
	updatesReceived       chan int
	// protected by mutex
	numCheckIns           int
	numSelected           int
	numUpdatesStart       int
}

// CheckIn rpc
// Clients check in with the Selector
// Selector send count to Coordinator and waits for signal from it
func (s *Selector) CheckIn(stream pbRound.FlRound_CheckInServer) error {

	// receive check-in request
	checkinReq, err := stream.Recv()
	if (err != nil) {
		log.Println("Unable to receive checkin request by client")
		log.Println(err)
		return err
	}
	log.Println("Client checked in with message:", checkinReq.Message)

	selected := false
	index := 0
	// check selection
	s.mutex.Lock()
	s.numCheckIns++
	selected, err = checkSelection(s.coordinatorAddress, s.numCheckIns, s.selectorID)
	if (err != nil) {
		log.Println("Unable to check selection with Coordinator")
		log.Println(err)
		return err
	}
	if selected {
		log.Println("Client checked-in is selected")
		s.numSelected++
		index = s.numSelected
	}
	s.mutex.Unlock()

	// if not selected
	if !selected {
		log.Println("Client checked is not selected")
		err := stream.Send(&pbRound.FlData{
			IntVal: viper.GetInt64("POST_CHECKIN_RECONNECTION_TIME"),
			Type:   pbRound.Type_FL_INT,
		})
		if err != nil {
			log.Println("Unable to send rejection response to client")
			log.Println(err)
			return err
		}
		return nil
	}

	log.Println("Selected Client", index, "waiting for configuration to begin")
	// wait till configuration phase begins
	<- s.waitForConfiguration
	log.Println("Selected Client", index, ", Configuration started")

	// proceed with sending configuration files
	err = sendConfigurationFiles(s.flRootPath, stream)
	if (err != nil) {
		log.Println("Unable to send configuration files to client")
		log.Println(err)
		return err
	}
	return nil
}

// Update rpc
// Accumulate FL checkpoint update sent by client
func (s *Selector) Update(stream pbRound.FlRound_UpdateServer) error {
	// get index to store files
	s.mutex.Lock()
	s.numUpdatesStart++
	index := s.numUpdatesStart
	s.mutex.Unlock()

	log.Println("Update request from client, Index : ", index)

	// saved the received files
	err := storeUpdates(s.flRootPath, s.selectorID, index, stream)
	if (err != nil) {
		log.Println("Unable to store received updates by client")
		return err
	}

	// indicate to handler updates are received
	s.updatesReceived <- 1
	return nil
}

// GoalCountReached rpc
// Once broadcast to proceed with configuration phase is received from the coordinator
// based on the count of routines waiting on waitForConfiguration channel, they are sent messages to proceed
func (s *Selector) GoalCountReached(ctx context.Context, empty *pbIntra.Empty) (*pbIntra.Empty, error) {
	// get selected no of clients waiting on the waitForConfiguration channel
	s.mutex.Lock()
	numSelected := s.numSelected
	s.mutex.Unlock()

	log.Println("GoalCountReached starting Configuration for", numSelected, "Clients")

	// begin configuration for numSelected number of clients
	// by unblocking them
	for i := 0; i < numSelected; i++ {
		s.waitForConfiguration <- true
	}

	return &pbIntra.Empty{}, nil
}

// Handler for checking no of updates received and starting federated averaging
func (s *Selector) averagingHandler() {
	numUpdatesFinished := 0
	for {
		select {
		// read on updates received channel
		case <- s.updatesReceived:
			// update variables
			numUpdatesFinished++
			s.mutex.Lock()
			numSelected := s.numSelected
			s.mutex.Unlock()
			// check condition to start averaging
			if numUpdatesFinished == numSelected {
				log.Println("Received updates from selected clients, starting mid averaging")
				err := midAveraging(numUpdatesFinished, s.flRootPath, s.selectorID)
				if (err != nil) {
					log.Println("Unable to complete mid aggregation")
					log.Println(err)
				} else {
					log.Println("Aggregation complete")
					err := notifyAggregationComplete(s.coordinatorAddress, s.selectorID)
					if (err != nil) {
						log.Println("Unable to send aggregation complete msg to coordinator")
						log.Println(err)
					} else {
						log.Println("Sent notification of aggregation complete to coordinator")
					}
				}

				// reset variables
				numUpdatesFinished = 0
				s.resetVariables()
			}
		}
	}
}

// reset variables
func (s *Selector) resetVariables() {
	s.mutex.Lock()
	s.numCheckIns = 0
	s.numSelected = 0
	s.numUpdatesStart = 0
	s.mutex.Lock()
}

func sendConfigurationFiles(flRootPath string, stream pbRound.FlRound_CheckInServer) error {
	completeInitPath := filepath.Join(flRootPath, viper.GetString("INIT_FILES_PATH"))
	// walk through directory and send all the files
	err := filepath.Walk(completeInitPath, func(path string, info os.FileInfo, errX error) error {
		// Skip if directory
		if info.IsDir() {
			return nil
		}
		// open file
		file, err := os.Open(path)
		if err != nil {
			log.Println("Unable to open a configuration file")
			return err
		}
		defer file.Close()

		// make a buffer of a defined chunk size
		buf := make([]byte, viper.GetInt64("CHUNK_SIZE"))

		for {
			// read the content (by using chunks)
			n, err := file.Read(buf)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Println("CheckIn: Unable to read a configuration file")
				return err
			}
			// send the FL checkpoint Data (file chunk + type: FL checkpoint)
			err = stream.Send(&pbRound.FlData{
				Chunk:    buf[:n],
				Type:     pbRound.Type_FL_FILES,
				FilePath: info.Name(),
			})
			if err != nil {
				log.Println("CheckIn: Unable to stream a configuration file")
				return err
			}
		}
	})
	return err
}

func storeUpdates(flRootPath string, selectorID string, index int, stream pbRound.FlRound_UpdateServer) error{
	// open the file to save checkpoint received
	checkpointFilePath := filepath.Join(flRootPath, selectorID, viper.GetString("ROUND_CHECKPOINT_UPDATES_DIR")) + strconv.Itoa(index)
	checkpointWeightPath := filepath.Join(flRootPath, selectorID, viper.GetString("ROUND_CHECKPOINT_WEIGHT_DIR")) + strconv.Itoa(index)

	os.MkdirAll(path.Dir(checkpointFilePath), os.ModePerm)
	os.MkdirAll(path.Dir(checkpointWeightPath), os.ModePerm)

	file, err := os.OpenFile(checkpointFilePath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println("Unable to open update checkpoint file")
		os.Remove(checkpointFilePath)
		return err
	}
	// not at the end as we are returning in between when other error come
	defer file.Close()

	for {
		// receive Fl data
		flData, err := stream.Recv()
		// exit after data transfer completes
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Unable to receive the update files")
			os.Remove(checkpointFilePath)
			return err
		}

		if flData.Type == pbRound.Type_FL_FILES {
			// write data to file
			_, err = file.Write(flData.Chunk)
			if err != nil {
				log.Println("Unable to write into a checkpoint file")
				os.Remove(checkpointFilePath)
				return err
			}
		} else if flData.Type == pbRound.Type_FL_INT {
			// write weight to file
			os.MkdirAll(path.Dir(checkpointWeightPath), os.ModePerm)
			weightFile, err := os.OpenFile(checkpointWeightPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
			defer weightFile.Close()

			if err != nil {
				log.Println("Unable to open a update file")
				os.Remove(checkpointWeightPath)
				return err
			}
			log.Println("Checkpoint weight: ", strconv.FormatInt(flData.IntVal, 10))
			_, err = weightFile.WriteString(strconv.FormatInt(flData.IntVal, 10))
			if err != nil {
				log.Println("Unable to write into weight file")
				os.Remove(checkpointWeightPath)
				return err
			}
		}
	}
	return nil
}

// Runs mid averaging (federated averaging wrt its clients)
// stores the aggregated checkpoint and weights (amount of data the client trained on)
func midAveraging(numUpdatesFinished int, flRootPath string, selectorID string) error {
	// build arguments for federated averaging process
	var argsList []string
	var path = filepath.Join(flRootPath, selectorID)
	argsList = append(argsList, "mid_averaging.py", "--cf", 
		filepath.Join(path, viper.GetString("AGG_CHECKPOINT_FILE_PATH")), "--mf", 
		filepath.Join(flRootPath, viper.GetString("INIT_FILES_PATH"), 
		viper.GetString("MODEL_FILE")), "--u")
	var totalWeight int64 = 0
	for i := 1; i <= numUpdatesFinished; i++ {
		checkpointFilePath := filepath.Join(path, viper.GetString("ROUND_CHECKPOINT_UPDATES_DIR")) + strconv.Itoa(i)
		checkpointWeightPath := filepath.Join(path, viper.GetString("ROUND_CHECKPOINT_WEIGHT_DIR")) + strconv.Itoa(i)
		data, err := ioutil.ReadFile(checkpointWeightPath)
		if err != nil {
			log.Println("Unable to read checkpoint weight file")
			return err
		}
		dataInt, _ := strconv.ParseInt(string(data), 10, 64)
		totalWeight += dataInt
		argsList = append(argsList, string(data), checkpointFilePath)
	}

	log.Println("Arguments passed to federated averaging python file: ", argsList)

	// model path
	cmd := exec.Command("python", argsList...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Println("Unable to run mid federated averaging python process")
		return err
	}

	// store aggregated weight in file
	aggCheckpointWeightPath := filepath.Join(path, viper.GetString("AGG_CHECKPOINT_WEIGHT_PATH"))
	os.MkdirAll(path, os.ModePerm)
	aggWeightFile, err := os.OpenFile(aggCheckpointWeightPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println("Unable to open agg checkpoint weight file")
		os.Remove(aggCheckpointWeightPath)
		return err
	}
	defer aggWeightFile.Close()
	_, err = aggWeightFile.WriteString(strconv.FormatInt(totalWeight, 10))
	if err != nil {
		log.Println("Unable to write to agg checkpoint weight")
		return err
	}
	return nil
}

// check if the current client is selected by interacting with coordinator
func checkSelection(coordinatorAddress string, numCheckIns int, selectorID string) (bool, error) {
	conn, err := grpc.Dial(coordinatorAddress, grpc.WithInsecure(), grpc.WithBlock())
	if (err != nil) {
		log.Println("Unable to connect to coordinator")
		return false, err
	}

	client := pbIntra.NewFlIntraClient(conn)
	result, err := client.ClientCountUpdate(context.Background(), 
		&pbIntra.ClientCount{Count: uint32(numCheckIns), Id: selectorID})
	if (err != nil) {
		// log.Println("Unable to send client selection msg to coordinator")
		return false, err
	}

	return result.Accepted, nil
}

// notify selector about completion of aggregation of updates
func notifyAggregationComplete(coordinatorAddress string, selectorID string) error {
	conn, err := grpc.Dial(coordinatorAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Println("Unable to connect to coordinator")
		return err
	}

	client := pbIntra.NewFlIntraClient(conn)
	_, err = client.SelectorAggregationComplete(context.Background(), &pbIntra.SelectorId{Id: selectorID})
	if err != nil {
		// log.Println("Unable to send aggregation complete msg to coordinator")
		return err
	}
	return nil
}

// init 
// set viper
func init() {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	viper.AutomaticEnv()          // enable viper to read env

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}
	// TODO: Add defaults for config using viper
}

func main() {

	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile )

	// get variables
	port := viper.GetString("PORT")
	selectorID := viper.GetString("HOSTNAME")
	// log.Println(viper.GetString("SELECTOR_SERVICE"))
	if viper.IsSet("SELECTOR_SERVICE") {
		selectorID += "." + viper.GetString("SELECTOR_SERVICE") + ":" + port
	} else {
		selectorID += ":" + port
	}
	log.Println("SelectorID:", selectorID)
	coordinatorAddress := viper.GetString("COORDINATOR_ADDRESS")
	flRootPath := viper.GetString("FL_ROOT_PATH")
	address := ":" + port

	// Selector impl instance
	srv := grpc.NewServer()
	selector := &Selector{
		selectorID:           selectorID,
		coordinatorAddress:   coordinatorAddress,
		flRootPath:           flRootPath,
		waitForConfiguration: make(chan bool),
		updatesReceived:      make(chan int),
		// protected by mutex
		numCheckIns:        0,
		numUpdatesStart:    0,
		numSelected:        0,
	}

	// register FL round Selector
	pbRound.RegisterFlRoundServer(srv, selector)
	// register FL intra broadcast Selector
	pbIntra.RegisterFLGoalCountBroadcastServer(srv, selector)

	// start handlers
	go selector.averagingHandler()

	// listen
	lis, err := net.Listen("tcp", address)
	if (err != nil) {
		log.Println("Failed to listen on ", address)
		log.Fatal(err)
	} else {
		log.Println( "Listening on address:", address)
	}

	// start serving
	log.Println("Serving Selector Service")
	err = srv.Serve(lis)
}
