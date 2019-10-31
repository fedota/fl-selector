package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	pbIntra "federated-learning/fl-selector/genproto/fl_intra"
	pbRound "federated-learning/fl-selector/genproto/fl_round"

	"google.golang.org/grpc"
)

var start time.Time

// constants
const (
	coordinatorAddress			= "localhost:50050"
	port = ":50051"
	modelFilePath               = "./data/model/model.h5"
	checkpointFilePath          = "./data/checkpoint/fl_checkpoint"
	aggregateCheckpointFilePath          = "./data/checkpoint/fl_agg_checkpoint"
	weightUpdatesDir            = "./data/weight_updates/"
	chunkSize                   = 64 * 1024
	postCheckinReconnectionTime = 8000
	postUpdateReconnectionTime  = 8000
	// estimatedRoundTime          = 8000
	estimatedWaitingTime = 20000
	checkinLimit                = 3
	VAR_NUM_CHECKINS            = 0
	VAR_NUM_UPDATES_START       = 1
	VAR_NUM_UPDATES_FINISH      = 2
	VAR_NUM_SELECTED            = 0
)

// store the result from a client
type flRoundClientResult struct {
	checkpointWeight   int64
	checkpointFilePath string
}

// to handle read writes
// Credit: Mark McGranaghan
// Source: https://gobyexample.com/stateful-goroutines
type readOp struct {
	varType  int
	response chan int
}
type writeOp struct {
	varType  int
	val      int
	response chan int
}

// server struct to implement gRPC Round service interface
type server struct {
	selectorId	int
	clientCountReads             chan readOp
	clientCountWrites            chan writeOp
	updateCountReads             chan readOp
	updateCountWrites            chan writeOp
	selected          chan bool
	numCheckIns       int
	numSelected       int
	numUpdatesStart   int
	numUpdatesFinish  int
	mu                sync.Mutex
	checkpointUpdates map[int]flRoundClientResult
}

func init() {
	start = time.Now()
}

func main() {	
	// listen
	lis, err := net.Listen("tcp", port)
	check(err, "Failed to listen on port"+port)

	srv := grpc.NewServer()
	// server impl instance
	flServer := &server{
		selectorId:	1,
		numCheckIns:       0,
		numUpdatesStart:   0,
		numUpdatesFinish:  0,
		checkpointUpdates: make(map[int]flRoundClientResult),
		clientCountReads:             make(chan readOp),
		clientCountWrites:            make(chan writeOp),
		updateCountReads:             make(chan readOp),
		updateCountWrites:            make(chan writeOp),
		selected:          make(chan bool)}
	// register FL round server
	pbRound.RegisterFlRoundServer(srv, flServer)
	// register FL intra broadcast server
	pbIntra.RegisterFLGoalCountBroadcastServer(srv, flServer)

	go flServer.ClientSelectionHandler()
	go flServer.ClientConnectionUpdateHandler()

	// start serving
	log.Println("Starting server on port:", port)
	err = srv.Serve(lis)
	check(err, "Failed to serve on port "+port)
}

// Check In rpc
// Client check in with FL selector
// Selector send count to Coordinator and waits for signal from it
func (s *server) CheckIn(stream pbRound.FlRound_CheckInServer) error {

	var (
		buf  []byte
		n    int
		file *os.File
	)

	// receive check-in request
	checkinReq, err := stream.Recv()
	log.Println("CheckIn Request: Client Name: ", checkinReq.Message, "Time:", time.Since(start))

	// create a write operation
	write := writeOp{
		varType:  VAR_NUM_CHECKINS, 
		response: make(chan int)}
	// send to handler(ClientSelectionHandler) via writes channel
	s.clientCountWrites <- write
	<-write.response

	// wait for start of configuration (by ClientSelectionHandler)
	if !(<-s.selected) {
		log.Println("Not selected")
		err := stream.Send(&pbRound.FlData{
			IntVal: postCheckinReconnectionTime,
			Type:   pbRound.Type_FL_RECONN_TIME,
		})
		check(err, "Unable to send post checkin reconnection time")
		return nil
	}

	// Proceed with sending checkpoint file to client

	// open file
	file, err = os.Open(checkpointFilePath)
	check(err, "Unable to open checkpoint file")
	defer file.Close()

	// make a buffer of a defined chunk size
	buf = make([]byte, chunkSize)

	for {
		// read the content (by using chunks)
		n, err = file.Read(buf)
		if err == io.EOF {
			return nil
		}
		check(err, "Unable to read checkpoint file")

		// send the FL checkpoint Data (file chunk + type: FL checkpoint)
		err = stream.Send(&pbRound.FlData{
			Message: &pbRound.Chunk{
				Content: buf[:n],
			},
			Type: pbRound.Type_FL_CHECKPOINT,
		})
		check(err, "Unable to send checkpoint data")
	}

}

// Update rpc
// Accumulate FL checkpoint update sent by client
// TODO: delete file when error and after round completes
func (s *server) Update(stream pbRound.FlRound_UpdateServer) error {

	var checkpointWeight int64

	log.Println("Update Request: Time:", time.Since(start))

	// create a write operation
	write := writeOp{
		varType:  VAR_NUM_UPDATES_START,
		response: make(chan int)}
	// send to handler (ClientConnectionUpdateHandler) via writes channel
	s.updateCountWrites <- write
	index := <- write.response

	log.Println("Index : ", index)

	// open the file
	// log.Println(weightUpdatesDir + strconv.Itoa(index))
	filePath := weightUpdatesDir + "weight_updates_" + strconv.Itoa(index)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	check(err, "Unable to open new checkpoint file")
	defer file.Close()

	for {
		// receive Fl data
		flData, err := stream.Recv()
		// exit after data transfer completes
		if err == io.EOF {

			// create a write operation
			write = writeOp{
				varType:  VAR_NUM_UPDATES_FINISH,
				response: make(chan int)}
			// send to handler (ConnectionHandler) via writes channel
			s.updateCountWrites <- write

			// put result in map
			// TODO: modify by making a go-routine to do updates
			s.mu.Lock()
			s.checkpointUpdates[index] = flRoundClientResult{
				checkpointWeight:   checkpointWeight,
				checkpointFilePath: filePath,
			}
			log.Println("Checkpoint Update: ", s.checkpointUpdates[index])
			s.mu.Unlock()

			<-write.response
			// if !(<-write.response) {
			// 	log.Println("Checkpoint Update confirmed. Time:", time.Since(start))
			// }

			return stream.SendAndClose(&pbRound.FlData{
				IntVal: postUpdateReconnectionTime,
				Type:   pbRound.Type_FL_RECONN_TIME,
			})
		}
		check(err, "Unable to receive update data from client")

		if flData.Type == pbRound.Type_FL_CHECKPOINT_UPDATE {
			// write data to file
			_, err = file.Write(flData.Message.Content)
			check(err, "Unable to write into new checkpoint file")
		} else if flData.Type == pbRound.Type_FL_CHECKPOINT_WEIGHT {
			checkpointWeight = flData.IntVal
		}
	}

}

// Once broadcast to proceed with configuration is receivec from the coordinator
// based on the count, the rountine waiting on selected channel are sent messages 
func (s *server) GoalCountReached(ctx context.Context, empty *pbIntra.Empty) (*pbIntra.Empty, error) {
	// get the number of selected clients
	read := readOp {
		varType: VAR_NUM_SELECTED,
		response: make(chan int)}
	s.clientCountReads <- read
	numSelected := <-read.response

	// get the number of checkIn clients
	read = readOp{
		varType: VAR_NUM_CHECKINS,
		response: make(chan int)}
	s.clientCountReads <- read
	numCheckIns := <-read.response

	// select num selected number of clients
	for i := 0; i < numSelected; i++ {
		s.selected <- true
	}
	// reject the rest
	for i := 0; i < numCheckIns - numSelected; i++ {
		s.selected <- false
	}

	return &pbIntra.Empty{}, nil
}

// Runs mid averaging
// then sends the aggrageted checkpoint and total weight to the coordinator 
func (s *server) MidAveraging() {

	var argsList []string
	argsList = append(argsList, "mid_averaging.py", "--cf", aggregateCheckpointFilePath, "--mf", modelFilePath, "--u")
	var totalWeight int64 = 0 
	for _, v := range s.checkpointUpdates {
		totalWeight += v.checkpointWeight
		argsList = append(argsList, strconv.FormatInt(v.checkpointWeight, 10), v.checkpointFilePath)
	}

	log.Println("Arguments passed to federated averaging python file: ", argsList)

	// model path
	cmd := exec.Command("python", argsList...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	check(err, "Unable to run federated averaging")

	var (
		buf  []byte
		n    int
		file *os.File
	)

	log.Println("Post Averaging ==> Sending checkpoint file", 	"Time:", time.Since(start))

	// connect to the coordinator
	conn, err := grpc.Dial(coordinatorAddress)
	check(err, "Unable to connect to coordinator")
	client := pbIntra.NewFlIntraClient(conn)

	// initialize a context object
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// open stream
	updateStream, err := client.Update(ctx)
	check(err, "Unable to send client count")

	// send file
	file, err = os.Open(aggregateCheckpointFilePath)
	check(err, "Unable to open agg checkpoint file")
	defer file.Close()
	// make a buffer of a defined chunk size
	buf = make([]byte, chunkSize)
	for {
		// read the content (by using chunks)
		n, err = file.Read(buf)
		if err == io.EOF {
			err = updateStream.Send(&pbIntra.FlData{
				IntVal: totalWeight,
				Type: pbIntra.Type_FL_CHECKPOINT_WEIGHT,
			})
			check(err, "Unable to send checkpoint data")
		}
		check(err, "Unable to read checkpoint file")

		// send the Aggregated FL checkpoint Data (file chunk + type: FL checkpoint)
		err = updateStream.Send(&pbIntra.FlData{
			Message: &pbIntra.Chunk{
				Content: buf[:n],
			},
			Type: pbIntra.Type_FL_CHECKPOINT_UPDATE,
		})
		check(err, "Unable to send aggregated checkpoint data")
	}

	log.Println("Selection Handler ==> Sent aggregated checkpoint and weight", "Time:", time.Since(start))

}

// Handler for communicating with coordinator for selection process
func (s *server) ClientSelectionHandler() {
	for {
		select {
		case read := <-s.clientCountReads:
			switch read.varType {
			case VAR_NUM_UPDATES_START:
				log.Println("Selection Handler ==> Read Query:", read.varType, "Time:", time.Since(start))
				read.response <- s.numCheckIns
			case VAR_NUM_SELECTED:
				log.Println("Selection Handler ==> Read Query:", read.varType, "Time:", time.Since(start))
				read.response <- s.numSelected
			}
		case write := <-s.clientCountWrites:
			log.Println("Selection Handler ==> Write Query:", write.varType, "Time:", time.Since(start))
			s.numCheckIns++
			log.Println("Selection Handler ==> numCheckIns", s.numCheckIns, "Time:", time.Since(start))	
			write.response <- s.numCheckIns
	
			// send client count to coordinator
			conn, err := grpc.Dial(coordinatorAddress)
			check(err, "Unable to connect to coordinator")
			client := pbIntra.NewFlIntraClient(conn)
			result, err := client.ClientCountUpdate(context.Background(), &pbIntra.ClientCount{Count: uint32(s.numCheckIns), Id: uint32(s.selectorId)})
			check(err, "Unable to send client count")
			log.Println("Selection Handler ==> Sent client count", s.numCheckIns, "Time:", time.Since(start))
			if (result.Accepted) {
				s.numSelected++
			}
		}
	}
}

// Handler for maintaining counts of client connections
func (s *server) ClientConnectionUpdateHandler() {
	for {
		select {
		// read query
		case read := <-s.updateCountReads:
			log.Println("Update Handler ==> Read Query:", read.varType, "Time:", time.Since(start))
			switch read.varType {
			case VAR_NUM_UPDATES_START:
				read.response <- s.numUpdatesStart
			case VAR_NUM_UPDATES_FINISH:
				read.response <- s.numUpdatesFinish
			}
		// write query
		case write := <-s.updateCountWrites:
			log.Println("Update Handler ==> Write Query:", write.varType, "Time:", time.Since(start))
			switch write.varType {
			case VAR_NUM_UPDATES_START:
				s.numUpdatesStart++
				log.Println("Handler ==> numUpdates", s.numUpdatesStart, "Time:", time.Since(start))
				log.Println("Handler ==> accepted update", "Time:", time.Since(start))
				write.response <- s.numUpdatesStart

			case VAR_NUM_UPDATES_FINISH:
				s.numUpdatesFinish++
				log.Println("Handler ==> numUpdates: ", s.numUpdatesFinish, "Finish Time:", time.Since(start))
				log.Println("Handler ==> accepted update", "Time:", time.Since(start))
				write.response <- s.numUpdatesStart
				
				// if enough updates available, start FA
				if s.numUpdatesFinish == s.numUpdatesStart {
					// begin federated averaging process
					log.Println("Begin Mid Averaging Process")
					s.MidAveraging()
					s.resetFLVariables()
				}
			}
		// After wait period check if everything is fine
		case <-time.After(estimatedWaitingTime * time.Second):
			log.Println("Timeout")
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
	s.numUpdatesStart = 0
	s.numUpdatesFinish = 0
	s.checkpointUpdates = make(map[int]flRoundClientResult)
}
