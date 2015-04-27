package main

import (
    "fmt"
    "time"
    "sync"
    "math"
    "os"
    "strconv"
)

//wait group to synchronise shutting down of concurrent producers and consumers. Global to prevent data race.
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
    messagesChannel, stopConsumerChannel := startConsumer()

    //start x producers
    wg.Add(producers)
    stopProducerChannels := make([]chan struct{}, 0)
    for i := 0; i<producers; i++ {
        stopProducerChannel := startProducer(messagesChannel, fmt.Sprintf("producer%d", i))
        stopProducerChannels = append(stopProducerChannels, stopProducerChannel)
    }

    //Run the show for 1 second
    time.Sleep(time.Second)

    //send done signal to x producers
    for _, stopProducerChannel := range stopProducerChannels {
        stopProducerChannel <- struct {}{}
    }

    //wait for all producers to finish
    wg.Wait()

    //increment wait group to wait for consumer to finish
    wg.Add(1)
    stopConsumerChannel <- struct {}{}
    wg.Wait()

    fmt.Println("Done...")
}

func startConsumer() (chan string, chan struct {}) {

    //channels to communicate messages and stop signal
    messagesChannel := make(chan string)
    stopConsumerChannel := make(chan struct {})

    //start go routine
    go func() {
        //results map
        results := make(map[string]int)
        defer wg.Done()
        fmt.Println("Starting consumer in seperate go routine...")

        for {
            select {
            case msg := <-messagesChannel :
                results[msg]++
            case <-stopConsumerChannel :
                fmt.Println("Consumer received done signal...")
                analyseResults(results)
                return
            }
        }
    }()

    return messagesChannel, stopConsumerChannel
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
    stopProducerChannel := make(chan struct {})

    go func(stopProducerChannelCopy chan struct {}, nameCopy string) {
        fmt.Println("Starting producer", nameCopy, "in seperate go routine...")

        //defer decreasing workgroup counter until exit
        defer wg.Done()

        for {
            select {
            case <-stopProducerChannelCopy :
                fmt.Println("Producer", nameCopy, "received done signal...")
                return
            default :
                messages <- nameCopy
            }
        }
    }(stopProducerChannel, name)

    return stopProducerChannel
}