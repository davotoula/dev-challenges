package main

import (
    "fmt"
    "time"
    "sync"
)

var wg sync.WaitGroup

func main() {

    //arguments
    producers := 10

    //start single consumer
    messages, stopConsumer := startConsumer()

    //start x producers
    wg.Add(producers)
    stopProducers := make([]chan struct {},0)
    for i := 0; i<producers; i++ {
        stopProducer := startProducer(messages, fmt.Sprintf("producer%d", i))
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

    //increment wait group to wait for consumer to finish
    wg.Add(1)
    stopConsumer <- struct {}{}
    wg.Wait()

    fmt.Println("Done...")
}

func startConsumer() (chan string, chan struct{}) {

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
                fmt.Println(counter)
                return
            }
        }
    }()

    return messages, stopConsumer
}

func startProducer(messages chan string, name string) chan struct {} {

    //channel to signal to stop this producer
    stopProducer := make(chan struct {})

    go func(stopProducer2 chan struct{}, name2 string) {
        fmt.Println("Starting producer",name2,"in seperate go routine...")

        //defer decreasing workgroup counter until exit
        defer wg.Done()

        for {
            select {
            case <-stopProducer2 :
                fmt.Println("Producer",name2,"received done signal...")
                return
            default :
                messages <- name2
            }
        }
    }(stopProducer, name)

    return stopProducer
}