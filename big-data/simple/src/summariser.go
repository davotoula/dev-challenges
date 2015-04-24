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
    //input args java -jar Aggregator.jar transactions.csv exchangerates.csv "Defence ltd." GBP
    if (len(os.Args)!=5) {
        fmt.Printf("=== Usage %s [transactions file path] [exchange rates file path] [partner to calculate total for] [home currency]\n", os.Args[0])
        os.Exit(0)
    }

    transactionsFilePath := os.Args[1]
    exchangeRatesFilePath := os.Args[2]
    partner := os.Args[3]
    homeCurrency := os.Args[4]


    //result object
    aggregatedTransactions := make(map[string]float32)

    //load rates into map
    fmt.Println("Loading exchange rates...")
    exchangeRates := loadExchangeRates(exchangeRatesFilePath) //"/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv"

    //load transactions one line at a time and start aggregating results
    fmt.Printf("Calculating partner totals for [%s]...\n",partner)
    csvfile, err := os.Open(transactionsFilePath) //"/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/transactions2.csv"
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

    //write aggregated transactions to disk
    writeMapToDiskAsCsv(aggregatedTransactions)

    //output aggregated total for specified partner to console
    fmt.Printf("%.02f (for partner %s and currency %s)\n",aggregatedTransactions[partner], partner, homeCurrency)
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

func writeMapToDiskAsCsv(records map[string]float32) {
    csvfile, err := os.Create("aggregated_transactions_by_partner.csv")
    check(err)
    defer csvfile.Close()

    for k, v := range records {

        _,err = csvfile.WriteString(fmt.Sprintf("%s,%.02f\n",k,v))
        if err != nil {
            panic(err)
        }
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