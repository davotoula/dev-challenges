package main

import (
    "fmt"
    "os"
    "encoding/csv"
    "strconv"
    "io"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {
    //input args
    homeCurrency := "GBP"

    //result object
    aggregatedTransactions := make(map[string]float32)

    //load rates into map
    fmt.Println("\nLoading exchange rates...")
    exchangeRates := loadExchangeRates("/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv")

    //load transactions one line at a time and start aggregating results
    fmt.Println("\nCalculating partner totals...")
    csvfile, err := os.Open("/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/transactions.csv")
    check(err)
    defer csvfile.Close()

    reader := csv.NewReader(csvfile)
    reader.FieldsPerRecord = 3 // Expected records per line

    for {
        transactionLine, err := reader.Read() //Reaad one line at a time

        if ((err != nil)&&(err == io.EOF)) {break}

        partnerName := transactionLine[0]
        aggregatedTransactions[partnerName] += convertToHomeAmount(homeCurrency, exchangeRates, transactionLine)
    }

    fmt.Println(aggregatedTransactions)
}

func convertToHomeAmount(homeCurrency string, exchangeRates map[Key]float32, transactionLine []string) float32 {
    transactionCurrency := transactionLine[1]
    transactionAmount,_ := strconv.ParseFloat(transactionLine[2],32)
    if (transactionCurrency==homeCurrency) {
        return float32(transactionAmount)
    } else {
        return float32(transactionAmount)*exchangeRates[Key{transactionCurrency, homeCurrency}]
    }
}

//load exchange rates into map of maps
//    GPB ->
//        CHF:1.243
//        AUD:1.342435
//        ...
//    AUD ->
//        USD: 0.778
//        EUR: 0.789
//        ...

type Key struct {
    FromCurrency string
    ToCurrency string
}

func loadExchangeRates(filePath string) map[Key]float32 {
    csvfile, err := os.Open("/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv")
    check(err)
    defer csvfile.Close()

    reader := csv.NewReader(csvfile)
    reader.FieldsPerRecord = 3 // Expected records per line

    rawCSVdata, err := reader.ReadAll() //Read all at once
    check(err)

    exchangeRates := make(map[Key]float32)
    for _, each := range rawCSVdata {
        fromCurrency := each[0]
        toCurrency := each[1]
        exchangeRate, _ := strconv.ParseFloat(each[2], 32)
        exchangeRates[Key{fromCurrency, toCurrency}] = float32(exchangeRate)
    }

    return exchangeRates
}