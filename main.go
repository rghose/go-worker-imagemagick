package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/gographics/imagick.v3/imagick"
	"net/http"
	"encoding/base64"
)

var (
	NWorkers = flag.Int("n", 4, "The number of workers to start")
	HTTPAddr = flag.String("http", "127.0.0.1:8000", "Address to listen for HTTP requests on")
)

type WorkRequest struct {
	FileName string `json:"file"`
	Data     string `json:"data"`
}

// A buffered channel that we can send work requests on.
var WorkQueue = make(chan WorkRequest, 100)

func payloadHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var content = &WorkRequest{}
	err := json.NewDecoder(r.Body).Decode(&content)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// let's create a job with the payload
	work := WorkRequest{FileName: content.FileName, Data: content.Data}

	// Push the work onto the queue.
	WorkQueue <- work

	w.WriteHeader(http.StatusOK)
}

var WorkerQueue chan chan WorkRequest

func StartDispatcher(nworkers int) {
	// First, initialize the channel we are going to but the workers' work channels into.
	WorkerQueue = make(chan chan WorkRequest, nworkers)

	// Now, create all of our workers.
	for i := 0; i < nworkers; i++ {
		fmt.Println("Starting worker", i + 1)
		worker := NewWorker(i + 1, WorkerQueue)
		worker.Start()
	}

	go func() {
		for {
			select {
			case work := <-WorkQueue:
				fmt.Println("Received work requeust")
				go func() {
					worker := <-WorkerQueue

					fmt.Println("Dispatching work request")
					worker <- work
				}()
			}
		}
	}()
}

// NewWorker creates, and returns a new Worker object. Its only argument
// is a channel that the worker can add itself to whenever it is done its
// work.
func NewWorker(id int, workerQueue chan chan WorkRequest) Worker {
	// Create, and return the worker.
	worker := Worker{
		ID:          id,
		Work:        make(chan WorkRequest),
		WorkerQueue: workerQueue,
		QuitChan:    make(chan bool)}

	return worker
}

type Worker struct {
	ID          int
	Work        chan WorkRequest
	WorkerQueue chan chan WorkRequest
	QuitChan    chan bool
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w *Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case work := <-w.Work:
			// Receive a work request.
				fmt.Printf("worker%d: Filename: %s\n", w.ID, work.FileName)
				mv := imagick.NewMagickWand()
				data, err := base64.StdEncoding.DecodeString(work.Data)
				if err != nil {
					fmt.Printf("Error in decoding", err)
					return
				}
				err = mv.ReadImageBlob(data)
				if err != nil {
					fmt.Printf("Error in reading imagemagick", err)
					return
				}
				mv.SetFormat("jpeg")
				mv.WriteImage(fmt.Sprintf("/tmp/encoded_%s.jpg", work.FileName))

			case <-w.QuitChan:
			// We have been asked to stop.
				fmt.Printf("worker%d stopping\n", w.ID)
				return
			}
		}
	}()
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func (w *Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

func main() {
	flag.Parse()

	fmt.Println("Starting the dispatcher")
	StartDispatcher(*NWorkers)

	imagick.Initialize()
	defer imagick.Terminate()

	// Register our collector as an HTTP handler function.
	fmt.Println("Registering the handler")
	http.HandleFunc("/", payloadHandler)

	fmt.Println("HTTP server listening on", *HTTPAddr)
	if err := http.ListenAndServe(*HTTPAddr, nil); err != nil {
		fmt.Println(err.Error())
	}
}
