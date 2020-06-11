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
	"time"

	viper "github.com/spf13/viper"
	"google.golang.org/grpc"

	pbIntra "fedota/fl-selector/genproto/fl_intra"
	pbRound "fedota/fl-selector/genproto/fl_round"
)

var start time.Time

// constants
const (
	varNumCheckins      = iota
	varNumUpdatesStart  = iota
	varNumUpdatesFinish = iota
	varNumSelected      = iota
)

// to handle read writes
type readOp struct {
	varType  int
	response chan int
}
type writeOp struct {
	varType int
	// val      int
	response chan int
}

// server struct to implement gRPC Round service interface
type server struct {
	selectorID         string
	coordinatorAddress string
	flRootPath         string
	clientCountReads   chan readOp
	clientCountWrites  chan writeOp
	updateCountReads   chan readOp
	updateCountWrites  chan writeOp
	selected           chan bool // hold clients before configuration starts
	numCheckIns        int
	numSelected        int
	numUpdatesStart    int
	numUpdatesFinish   int
}

func init() {
	start = time.Now()

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
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	port := viper.GetString("PORT")
	selectorID := viper.GetString("HOSTNAME")
	log.Println(viper.GetString("SELECTOR_SERVICE"))
	if viper.IsSet("SELECTOR_SERVICE") {
		selectorID += "." + viper.GetString("SELECTOR_SERVICE") + ":" + port
	} else {
		selectorID += ":" + port
	}
	log.Println(selectorID)

	coordinatorAddress := viper.GetString("COORDINATOR_ADDRESS")
	flRootPath := viper.GetString("FL_ROOT_PATH")
	address := ":" + port
	// listen
	lis, err := net.Listen("tcp", address)
	check(err, "Failed to listen on " + address)

	srv := grpc.NewServer()
	// server impl instance
	flServer := &server{
		selectorID:         selectorID,
		coordinatorAddress: coordinatorAddress,
		flRootPath:         flRootPath,
		numCheckIns:        0,
		numUpdatesStart:    0,
		numUpdatesFinish:   0,
		numSelected:        0,
		clientCountReads:   make(chan readOp),
		clientCountWrites:  make(chan writeOp),
		updateCountReads:   make(chan readOp),
		updateCountWrites:  make(chan writeOp),
		selected:           make(chan bool)}

	// register FL round server
	pbRound.RegisterFlRoundServer(srv, flServer)
	// register FL intra broadcast server
	pbIntra.RegisterFLGoalCountBroadcastServer(srv, flServer)

	go flServer.ClientSelectionHandler()
	go flServer.ClientUpdateConnectionHandler()

	// start serving
	log.Println("Starting server on port:", port, "Time:", time.Since(start))
	err = srv.Serve(lis)
	check(err, "Failed to serve on port "+port)
}

// Check In rpc
// Clients check in with FL selector
// Selector send count to Coordinator and waits for signal from it
func (s *server) CheckIn(stream pbRound.FlRound_CheckInServer) error {
	var (
		buf []byte
		n   int
	)
	// receive check-in request
	checkinReq, err := stream.Recv()
	check(err, "Unable to receive checkin request by client")
	log.Println("CheckIn Request: Client Name: ", checkinReq.Message, "Time:", time.Since(start))

	// create a write operation
	write := writeOp{
		varType:  varNumCheckins,
		response: make(chan int)}
	// send to handler(ClientSelectionHandler) via writes channel
	s.clientCountWrites <- write

	// wait for start of configuration (by ClientSelectionHandler)
	// <-write.response waits for that client to be selected by client
	// selected clients then have to wait (on the s.selected channel) for configuration to be started once goal count is reached
	if (<-write.response) == -1 || !(<-s.selected) {
		// not selected
		log.Println("Not selected")
		err := stream.Send(&pbRound.FlData{
			IntVal: viper.GetInt64("POST_CHECKIN_RECONNECTION_TIME"),
			Type:   pbRound.Type_FL_INT,
		})
		if err != nil {
			log.Println("CheckIn: Unable to send reconnection time. Time:", time.Since(start))
			log.Println(err)
			return err
		}
		return nil
	}

	// Proceed with sending initial files
	completeInitPath := filepath.Join(s.flRootPath, viper.GetString("INIT_FILES_PATH"))
	err = filepath.Walk(completeInitPath, func(path string, info os.FileInfo, errX error) error {

		// Skip if directory
		if info.IsDir() {
			return nil
		}

		// open file
		file, err := os.Open(path)
		if err != nil {
			log.Println("CheckIn: Unable to open init file. Time:", time.Since(start))
			log.Println(err)
			return err
		}
		defer file.Close()

		// make a buffer of a defined chunk size
		buf = make([]byte, viper.GetInt64("CHUNK_SIZE"))

		for {
			// read the content (by using chunks)
			n, err = file.Read(buf)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Println("CheckIn: Unable to read init file. Time:", time.Since(start))
				log.Println(err)
				return err
			}
			// send the FL checkpoint Data (file chunk + type: FL checkpoint)
			err = stream.Send(&pbRound.FlData{
				Chunk:    buf[:n],
				Type:     pbRound.Type_FL_FILES,
				FilePath: info.Name(),
			})
			if err != nil {
				log.Println("CheckIn: Unable to stream init file. Time:", time.Since(start))
				log.Println(err)
				return err
			}
		}
	})
	return err
}

// Update rpc
// Accumulate FL checkpoint update sent by client
func (s *server) Update(stream pbRound.FlRound_UpdateServer) error {
	log.Println("Update Request: Time:", time.Since(start))

	// create a write operation
	write := writeOp{
		varType:  varNumUpdatesStart,
		response: make(chan int)}
	// send to handler (ClientUpdateConnectionHandler) via writes channel
	s.updateCountWrites <- write
	index := <-write.response

	log.Println("Index : ", index)

	// open the file to save checkpoint received
	checkpointFilePath := filepath.Join(s.flRootPath, s.selectorID, viper.GetString("ROUND_CHECKPOINT_UPDATES_DIR")) + strconv.Itoa(index)
	checkpointWeightPath := filepath.Join(s.flRootPath, s.selectorID, viper.GetString("ROUND_CHECKPOINT_WEIGHT_DIR")) + strconv.Itoa(index)

	os.MkdirAll(path.Dir(checkpointFilePath), os.ModePerm)
	os.MkdirAll(path.Dir(checkpointWeightPath), os.ModePerm)

	file, err := os.OpenFile(checkpointFilePath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println("Update: Unable to open file. Time:", time.Since(start))
		os.Remove(checkpointFilePath)
		log.Println(err)
		return err
	}
	defer file.Close()

	for {
		// receive Fl data
		flData, err := stream.Recv()
		// exit after data transfer completes
		if err == io.EOF {
			// create a write operation
			write = writeOp{
				varType:  varNumUpdatesFinish,
				response: make(chan int)}
			// send to handler (ConnectionHandler) via writes channel
			s.updateCountWrites <- write
			<-write.response // proceed
			return stream.SendAndClose(&pbRound.FlData{
				IntVal: viper.GetInt64("POST_UPDATE_RECONNECTION_TIME"),
				Type:   pbRound.Type_FL_INT,
			})
		}
		if err != nil {
			log.Println("Update: Unable to receive file. Time:", time.Since(start))
			os.Remove(checkpointFilePath)
			log.Println(err)
			return err
		}

		if flData.Type == pbRound.Type_FL_FILES {
			// write data to file
			_, err = file.Write(flData.Chunk)
			if err != nil {
				log.Println("Update: unable to write into file. Time:", time.Since(start))
				os.Remove(checkpointFilePath)
				return err
			}
		} else if flData.Type == pbRound.Type_FL_INT {
			// write weight to file
			os.MkdirAll(path.Dir(checkpointWeightPath), os.ModePerm)
			weightFile, err := os.OpenFile(checkpointWeightPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
			if err != nil {
				log.Println("Update: Unable to open file. Time:", time.Since(start))
				os.Remove(checkpointWeightPath)
				log.Println(err)
				return err
			}
			log.Println("Checkpoint weight: ", strconv.FormatInt(flData.IntVal, 10))
			_, err = weightFile.WriteString(strconv.FormatInt(flData.IntVal, 10))
			if err != nil {
				log.Println("Update: Unable to write into weight file. Time:", time.Since(start))
				os.Remove(checkpointWeightPath)
				return err
			}
			defer weightFile.Close()
		}
	}
}

// Once broadcast to proceed with configuration phase is received from the coordinator
// based on the count of routines waiting on selected channel, they are sent messages to proceed
func (s *server) GoalCountReached(ctx context.Context, empty *pbIntra.Empty) (*pbIntra.Empty, error) {
	log.Println("Broadcast Received")
	// get the number of selected clients
	read := readOp{
		varType:  varNumSelected,
		response: make(chan int)}
	s.clientCountReads <- read
	numSelected := <-read.response

	// get the number of checkIn clients
	read = readOp{
		varType:  varNumCheckins,
		response: make(chan int)}
	s.clientCountReads <- read
	numCheckIns := <-read.response

	log.Println("GoalCountReached ==> CheckIns: ", numCheckIns, "Selected: ", numSelected, "Time:", time.Since(start))

	// select num selected number of clients
	for i := 0; i < numSelected; i++ {
		s.selected <- true
	}
	// reject the rest
	for i := 0; i < numCheckIns-numSelected; i++ {
		s.selected <- false
	}
	return &pbIntra.Empty{}, nil
}

// Runs mid averaging (federated averaging wrt it clients)
// then stores the aggregated checkpoint and total weight to the coordinator
func (s *server) MidAveraging() {
	// build arguments for federated averaging process
	var argsList []string
	var path = filepath.Join(s.flRootPath, s.selectorID)
	argsList = append(argsList, "mid_averaging.py", "--cf", filepath.Join(path, viper.GetString("AGG_CHECKPOINT_FILE_PATH")), "--mf", filepath.Join(s.flRootPath, viper.GetString("INIT_FILES_PATH"), viper.GetString("MODEL_FILE")), "--u")
	var totalWeight int64 = 0
	for i := 1; i <= s.numUpdatesFinish; i++ {
		checkpointFilePath := filepath.Join(path, viper.GetString("ROUND_CHECKPOINT_UPDATES_DIR")) + strconv.Itoa(i)
		checkpointWeightPath := filepath.Join(path, viper.GetString("ROUND_CHECKPOINT_WEIGHT_DIR")) + strconv.Itoa(i)
		data, err := ioutil.ReadFile(checkpointWeightPath)
		if err != nil {
			log.Println("MidAveraging: Unable to read checkpoint weight file. Time:", time.Since(start))
			log.Println(err)
			return
		}
		dataInt, _ := strconv.ParseInt(string(data), 10, 64)
		totalWeight += dataInt
		argsList = append(argsList, string(data), checkpointFilePath)
	}

	log.Println("MidAveraging ==> Arguments passed to federated averaging python file: ", argsList)

	// model path
	cmd := exec.Command("python", argsList...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Println("MidAveraging ==> Unable to run mid federated averaging. Time:", time.Since(start))
		log.Println(err)
		return
	}

	// store aggregated weight in file
	aggCheckpointWeightPath := filepath.Join(path, viper.GetString("AGG_CHECKPOINT_WEIGHT_PATH"))
	os.MkdirAll(path, os.ModePerm)
	aggWeightFile, err := os.OpenFile(aggCheckpointWeightPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println("Mid Averaging: Unable to open agg checkpoint weight file. Time:", time.Since(start))
		os.Remove(aggCheckpointWeightPath)
		log.Println(err)
		return
	}
	defer aggWeightFile.Close()
	_, err = aggWeightFile.WriteString(strconv.FormatInt(totalWeight, 10))
	if err != nil {
		log.Println("MidAveraging: unable to write to agg checkpoint weight. Time:", time.Since(start))
		log.Println(err)
		return
	}

	// send indication of mid averaging completed to coordinator
	conn, err := grpc.Dial(s.coordinatorAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Println("MidAveraging: unable to connect to coordinator. Time:", time.Since(start))
		log.Println(err)
		return
	}
	client := pbIntra.NewFlIntraClient(conn)
	_, err = client.SelectorAggregationComplete(context.Background(), &pbIntra.SelectorId{Id: s.selectorID})
	if err != nil {
		log.Println("MidAveraging: unable to send aggregation complete to coordinator. Time:", time.Since(start))
		log.Println(err)
		return
	}
	log.Println("MidAveraging: sent aggregation complete to coordinator. Time", time.Since(start))
}

// Handler for communicating with coordinator for selection process for clients
func (s *server) ClientSelectionHandler() {
	for {
		select {
		case read := <-s.clientCountReads:
			switch read.varType {
			case varNumCheckins:
				log.Println("Selection Handler ==> Read Query:", read.varType, "Time:", time.Since(start))
				read.response <- s.numCheckIns
			case varNumSelected:
				log.Println("Selection Handler ==> Read Query:", read.varType, "Time:", time.Since(start))
				read.response <- s.numSelected
			}
		case write := <-s.clientCountWrites:
			log.Println("Selection Handler ==> Write Query:", write.varType, "Time:", time.Since(start))
			s.numCheckIns++
			log.Println("Selection Handler ==> numCheckIns", s.numCheckIns, "Time:", time.Since(start))

			// TODO reject clients if we know they won't be selected after limit is reached
			// send client count to coordinator
			conn, err := grpc.Dial(s.coordinatorAddress, grpc.WithInsecure(), grpc.WithBlock())
			check(err, "Unable to connect to coordinator")
			client := pbIntra.NewFlIntraClient(conn)
			result, err := client.ClientCountUpdate(context.Background(), &pbIntra.ClientCount{Count: uint32(s.numCheckIns), Id: s.selectorID})
			if err != nil {
				log.Println("Selection Handler: unable to send client count. Time:", time.Since(start))
				log.Println(err)
			} else {
				log.Println("Selection Handler ==> Sent client count", s.numCheckIns, ". Time:", time.Since(start))
				// accepted connection
				if result.Accepted {
					s.numSelected++
					// select the client for configuration
					write.response <- s.numSelected
				} else {
					write.response <- -1
				}
			}
		}
	}
}

// Handler for maintaining counts of client connections updated received
func (s *server) ClientUpdateConnectionHandler() {
	for {
		select {
		// read query
		case read := <-s.updateCountReads:
			log.Println("Update Handler ==> Read Query:", read.varType, "Time:", time.Since(start))
			switch read.varType {
			case varNumUpdatesStart:
				read.response <- s.numUpdatesStart
			case varNumUpdatesFinish:
				read.response <- s.numUpdatesFinish
			}
		// write query
		case write := <-s.updateCountWrites:
			// log.Println("Update Handler ==> Write Query:", write.varType, "Time:", time.Since(start))
			switch write.varType {
			case varNumUpdatesStart:
				s.numUpdatesStart++
				log.Println("Update Handler ==> numUpdates", s.numUpdatesStart, "Time:", time.Since(start))
				write.response <- s.numUpdatesStart
			case varNumUpdatesFinish:
				s.numUpdatesFinish++
				log.Println("Update Handler ==> numUpdates: ", s.numUpdatesFinish, "Finish Time:", time.Since(start))
				write.response <- s.numUpdatesStart

				// if enough updates available, start FA
				if s.numUpdatesFinish == s.numSelected {
					// begin federated averaging process
					log.Println("Update Handler ==> Begin Mid Federated Averaging", "Time:", time.Since(start))
					s.MidAveraging()
					s.resetFLVariables()
				}
			}
		}
	}
}

// reset variables
func (s *server) resetFLVariables() {
	s.numCheckIns = 0
	s.numSelected = 0
	s.numUpdatesStart = 0
	s.numUpdatesFinish = 0
}

// Check for error, log and exit if err
func check(err error, errorMsg string) {
	if err != nil {
		log.Fatalf(errorMsg, " ==> ", err)
	}
}
