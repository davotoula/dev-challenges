package main

import (
    "fmt"
)

func main() {

    messages := make(chan string)

    go consumer(messages)
    go producer(messages)

    var input string
    fmt.Scanln(&input)
    fmt.Println("Done...")
}

func consumer(messages chan string) {
    messages <- "hello"
}

func producer(messages chan string) {
    msg := <-messages
    fmt.Println(msg)
}