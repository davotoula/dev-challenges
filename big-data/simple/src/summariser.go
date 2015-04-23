package main

import (
    //"bufio"
    "fmt"
    //"io"
    //"io/ioutil"
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

//read each line from transaction
    //convert to correct exchange rate if needed
    //add to the partner in map

func main() {
    //input args
    homeCurrency := "GBP"

    //print rates
    //dat, err := ioutil.ReadFile("/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv")
    //check(err)
    //fmt.Print(string(dat))

    //load rates into map
    fmt.Println("\nLoading exchange rates...")
    exchangeRates := loadExchangeRates("/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv")
    //fmt.Println(exchangeRates)

    //load transactions one line at a time and start aggregating results
    fmt.Println("\nCalculating partner totals...")
    aggregatedTransactions := make(map[string]float32)

    csvfile, err := os.Open("/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/transactions.csv")
    check(err)
    defer csvfile.Close()

    reader := csv.NewReader(csvfile)
    reader.FieldsPerRecord = 3 // Expected records per line

    for {
        transactionLine, err := reader.Read()

        if err != nil {
            if err == io.EOF {
                break
            }
        }

        fmt.Println(transactionLine)
        partnerName := transactionLine[0]
        transactionCurrency := transactionLine[1]
        transactionAmount,_ := strconv.ParseFloat(transactionLine[2],32)
        if (transactionCurrency==homeCurrency) {
            aggregatedTransactions[partnerName] = aggregatedTransactions[partnerName] + float32(transactionAmount)
        } else {
            aggregatedTransactions[partnerName] = aggregatedTransactions[partnerName] + (float32(transactionAmount)*exchangeRates[Key{transactionCurrency, homeCurrency}])
        }
    }

    fmt.Println(aggregatedTransactions)
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

    rawCSVdata, err := reader.ReadAll()
    check(err)

    exchangeRates := make(map[Key]float32)
    for _, each := range rawCSVdata {
        fromCurrency := each[0]
        toCurrency := each[1]
        exchangeRate, _ := strconv.ParseFloat(each[2], 32)
        exchangeRates[Key{fromCurrency, toCurrency}] = float32(exchangeRate)
        //fmt.Printf("from %s, to %s, exchange rate is %f\n", fromCurrency, toCurrency, exchangeRates[Key{fromCurrency, toCurrency}])
    }

    return exchangeRates
}