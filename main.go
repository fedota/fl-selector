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
	"path/filepath"
	"strconv"
	"time"

	pbIntra "federated-learning/fl-selector/genproto/fl_intra"
	pbRound "federated-learning/fl-selector/genproto/fl_round"

	"google.golang.org/grpc"

	viper "github.com/spf13/viper"
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
// Credit: Mark McGranaghan
// Source: https://gobyexample.com/stateful-goroutines
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

	err := viper.ReadInConfig()   // Find and read the config file
	if err != nil {               // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	// TODO: Add defaults for config using viper
}

func main() {

	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	selectorID := viper.GetString("selector_id")
	port := viper.GetString("port")
	coordinatorAddress := viper.GetString("coordinator_address")
	flRootPath := viper.GetString("fl_root_path")
	address := ":" + viper.GetString("port")
	// listen
	lis, err := net.Listen("tcp", address)
	check(err, "Failed to listen on "+address)

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
	check(err, "Unable to recerive checkin request by client")
	log.Println("CheckIn Request: Client Name: ", checkinReq.Message, "Time:", time.Since(start))

	// create a write operation
	write := writeOp{
		varType:  varNumCheckins,
		response: make(chan int)}
	// send to handler(ClientSelectionHandler) via writes channel
	s.clientCountWrites <- write

	// wait for start of configuration (by ClientSelectionHandler)
	if (<-write.response) == -1 || !(<-s.selected) {
		log.Println("Not selected")
		err := stream.Send(&pbRound.FlData{
			IntVal: viper.GetInt64("post_checkin_reconnection_time"),
			Type:   pbRound.Type_FL_INT,
		})
		if err != nil {
			log.Println("CheckIn: Unable to send reconnection time. Time:", time.Since(start))
			return err
		}
		return nil
	}

	// Proceed with sending initial files
	completeInitPath := s.flRootPath + viper.GetString("init_files_path")
	err = filepath.Walk(completeInitPath, func(path string, info os.FileInfo, errX error) error {
		// open file
		file, err := os.Open(path)
		if err != nil {
			log.Println("CheckIn: Unable to open init file. Time:", time.Since(start))
			return err
		}
		defer file.Close()

		// make a buffer of a defined chunk size
		buf = make([]byte, viper.GetInt64("chunk_size"))

		for {
			// read the content (by using chunks)
			n, err = file.Read(buf)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Println("CheckIn: Unable to read init file. Time:", time.Since(start))
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
	checkpointFilePath := s.flRootPath + s.selectorID + viper.GetString("round_checkpoint_updates_dir") + strconv.Itoa(index)
	checkpointWeightPath := s.flRootPath + s.selectorID + viper.GetString("round_checkpoint_weight_dir") + strconv.Itoa(index)

	file, err := os.OpenFile(checkpointFilePath, os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Println("Update: Unable to open file. Time:", time.Since(start))
		os.Remove(checkpointFilePath)
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
				IntVal: viper.GetInt64("post_update_reconnection_time"),
				Type:   pbRound.Type_FL_INT,
			})
		}
		if err != nil {
			log.Println("Update: Unable to receive file. Time:", time.Since(start))
			os.Remove(checkpointFilePath)
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
			weightFile, err := os.OpenFile(checkpointWeightPath, os.O_CREATE|os.O_WRONLY, os.ModeAppend)
			if err != nil {
				log.Println("Update: Unable to open file. Time:", time.Since(start))
				os.Remove(checkpointWeightPath)
				return err
			}
			_, err = weightFile.Write(flData.Chunk)
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
// based on the count, the rountine waiting on selected channel are sent messages to proceed
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

// Runs mid averaging
// then stores the aggrageted checkpoint and total weight to the coordinator
func (s *server) MidAveraging() {
	var argsList []string
	var path string = s.flRootPath + s.selectorID
	argsList = append(argsList, "mid_averaging.py", "--cf", path+viper.GetString("agg_checkpoint_file_path"), "--mf", s.flRootPath+viper.GetString("init_files_path")+viper.GetString("model_file"), "--u")
	var totalWeight int64 = 0
	for i := 1; i <= s.numUpdatesFinish; i++ {
		checkpointFilePath := path + viper.GetString("round_checkpoint_updates_dir") + strconv.Itoa(i)
		checkpointWeightPath := path + viper.GetString("round_checkpoint_weight_dir") + strconv.Itoa(i)
		data, err := ioutil.ReadFile(checkpointWeightPath)
		if err != nil {
			log.Println("MidAveraging: Unable to read checkpoint weight file. Time:", time.Since(start))
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
		return
	}

	// store aggregated weight in file
	aggWeightFile, err := os.OpenFile(path+viper.GetString("agg_checkpoint_weight_path"), os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Println("Mid Averaging: Unable to openagg checkpoint weight file. Time:", time.Since(start))
		os.Remove(path + viper.GetString("agg_checkpoint_weight_path"))
		return
	}
	defer aggWeightFile.Close()
	_, err = aggWeightFile.WriteString(string(totalWeight))
	if err != nil {
		log.Println("MidAveraging: unable to write to agg checkpoint weight. Time:", time.Since(start))
		return
	}

	// send indication of mid averaging completed to coordinator
	conn, err := grpc.Dial(s.coordinatorAddress, grpc.WithInsecure())
	if err != nil {
		log.Println("MidAveraging: unable to connect to coordinator. Time:", time.Since(start))
		return
	}
	client := pbIntra.NewFlIntraClient(conn)
	_, err = client.SelectorAggregationComplete(context.Background(), &pbIntra.SelectorId{Id: s.selectorID})
	if err != nil {
		log.Println("MidAveraging: unable to send aggregation complete to coordinator. Time:", time.Since(start))
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
			// send client count to coordinator
			conn, err := grpc.Dial(s.coordinatorAddress, grpc.WithInsecure())
			check(err, "Unable to connect to coordinator")
			client := pbIntra.NewFlIntraClient(conn)
			result, err := client.ClientCountUpdate(context.Background(), &pbIntra.ClientCount{Count: uint32(s.numCheckIns), Id: s.selectorID})
			if err != nil {
				log.Println("Selection Handler: unable to send client count. Time:", time.Since(start))
			}
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
		// After wait period check if everything is fine
		case <-time.After(time.Duration(viper.GetInt64("estimated_waiting_time")) * time.Second):
			log.Println("Update Handler ==> Timeout", "Time:", time.Since(start))
			// if checkin limit is not reached
			// abandon round
			// TODO: after checkin is done

			// TODO: Decide about updates not received in time
		}
	}
}

// Check for error, log and exit if err
func check(err error, errorMsg string) {
	if err != nil {
		log.Fatalf(errorMsg, " ==> ", err)
	}
}

func (s *server) resetFLVariables() {
	s.numCheckIns = 0
	s.numSelected = 0
	s.numUpdatesStart = 0
	s.numUpdatesFinish = 0
}
