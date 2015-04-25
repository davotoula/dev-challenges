package main

import (
    "fmt"
    "time"
    "sync"
)

func main() {

    //arguments
    producers := 10

    //start single consumer
    messages, stopConsumer := startConsumer()

    //start x producers
    var wg sync.WaitGroup
    wg.Add(producers)
    stopProducers := make([]chan struct {},0)
    for i := 0; i<producers; i++ {
        stopProducer := startProducer(messages, fmt.Sprintf("producer%d", i), wg)
        stopProducers = append(stopProducers, stopProducer)
    }

    //Run the show for 1 second
    time.Sleep(time.Second)

    //send done signal to x producers
    for _, stopProducer := range stopProducers {
        stopProducer <- struct{}{}
    }

    //wait for all producers to finish
    wg.Wait()
    stopConsumer <- struct {}{}

    fmt.Println("Done...")
}

func startConsumer() (chan string, chan struct{}) {

    //channels to communicate messages and stop signal
    messages := make(chan string)
    stopConsumer := make(chan struct {})

    //results map
    counter := make(map[string]int)

    //start go routine
    go func() {
        for {
            select {
            case msg := <-messages :
                counter[msg]++
            case <-stopConsumer :
                fmt.Println("Received consumer done signal...")
                return
            }
        }
    }()

    return messages, stopConsumer
}

func startProducer(messages chan string, name string, wg sync.WaitGroup) chan struct {} {

    //channel to signal to stop this producer
    stopProducer := make(chan struct {})

    //defer decreasing workgroup counter until exit
    defer wg.Done()

    go func() {
        for {
            select {
            case <-stopProducer :
                fmt.Println("Received done signal...")
                return
            default :
                messages <- name
            }
        }
    }()

    return stopProducer
}