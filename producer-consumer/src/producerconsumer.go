package main

import (
    "fmt"
    "time"
    "sync"
)

func main() {

    //arguments
    producers := 10

    messages := make(chan string)
    done := make(chan bool)

    //start single consumer
    go consumer(messages)

    //start x producers
    var wg sync.WaitGroup
    wg.Add(producers)
    for i:=0;i<producers;i++ {
        go producer(messages,fmt.Sprintf("producer%d",i),done,wg)
    }

    //Run the show for 1 second
    time.Sleep(time.Second)

    //send done signal to x producers
    for i:=0;i<producers;i++ {
        done <- true
    }

    //wait for all producers to finish
    wg.Wait()
    close(messages)
    close(done)
    fmt.Println("Done...")
}

func consumer(messages chan string) {

    for {
        msg, more := <-messages
        if more {
            fmt.Println(msg)
        } else {
            fmt.Println("No more messages, press any key to terminate...")
            return
        }
    }
}

func producer(messages chan string, name string, done chan bool, wg sync.WaitGroup) {
    isDone:=false
    defer wg.Done()
    for i := 0;isDone==false; i++ {
        select {
        case isDone = <- done :
                fmt.Println("Received done signal...")
                return
        default :
            messages <- fmt.Sprintf("%s-%d", name, i)
        }
    }
}