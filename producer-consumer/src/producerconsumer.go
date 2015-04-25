package main

import (
    "fmt"
    "time"
    "sync"
    "math"
    "os"
    "strconv"
)

var wg sync.WaitGroup

func main() {

    //arguments
    var producers int
    if (len(os.Args)<2) {
        producers = 10
    } else {
        producers,_ = strconv.Atoi(os.Args[1])
    }


    //start single consumer
    messages, stopConsumer := startConsumer()

    //start x producers
    wg.Add(producers)
    stopProducers := make([]chan struct {}, 0)
    for i := 0; i<producers; i++ {
        stopProducer := startProducer(messages, fmt.Sprintf("producer%d", i))
        stopProducers = append(stopProducers, stopProducer)
    }

    //Run the show for 1 second
    time.Sleep(time.Second)

    //send done signal to x producers
    for _, stopProducer := range stopProducers {
        stopProducer <- struct {}{}
    }

    //wait for all producers to finish
    wg.Wait()

    //increment wait group to wait for consumer to finish
    wg.Add(1)
    stopConsumer <- struct {}{}
    wg.Wait()

    fmt.Println("Done...")
}

func startConsumer() (chan string, chan struct {}) {

    //channels to communicate messages and stop signal
    messages := make(chan string)
    stopConsumer := make(chan struct {})

    //start go routine
    go func() {
        //results map
        counter := make(map[string]int)
        defer wg.Done()
        fmt.Println("Starting consumer in seperate go routine...")

        for {
            select {
            case msg := <-messages :
                counter[msg]++
            case <-stopConsumer :
                fmt.Println("Consumer received done signal...")
                analyseResults(counter)
                return
            }
        }
    }()

    return messages, stopConsumer
}

func analyseResults(results map[string]int) {
    var min float64
    min = math.MaxFloat64
    var max float64
    max = 0
    sum := 0;

    for _, v := range results {
        sum +=v
        min = math.Min(float64(min), float64(v))
        max = math.Max(float64(max), float64(v))
    }

    fmt.Println("Number of active producers:",len(results));
    fmt.Println("Number of messages:", sum);
    fmt.Println("Laziest producer:", min);
    fmt.Println("Busiest producer:", max);
}

func startProducer(messages chan string, name string) chan struct {} {

    //channel to signal to stop this producer
    stopProducer := make(chan struct {})

    go func(stopProducer2 chan struct {}, name2 string) {
        fmt.Println("Starting producer", name2, "in seperate go routine...")

        //defer decreasing workgroup counter until exit
        defer wg.Done()

        for {
            select {
            case <-stopProducer2 :
                fmt.Println("Producer", name2, "received done signal...")
                return
            default :
                messages <- name2
            }
        }
    }(stopProducer, name)

    return stopProducer
}