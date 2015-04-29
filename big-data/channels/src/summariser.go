package main
import (
    "os"
    "fmt"
    "encoding/csv"
    "strconv"
    "io"
    "time"
)

func main() {
    start := time.Now()

    //input args java -jar Aggregator.jar transactions.csv exchangerates.csv "Defence ltd." GBP
    if (len(os.Args)!=5) {
        fmt.Printf("=== Usage %s [transactions file path] [exchange rates file path] [partner to calculate total for] [home currency]\n", os.Args[0])
        os.Exit(0)
    }

    transactionsFilePath := os.Args[1]
    exchangeRatesFilePath := os.Args[2]
    partner := os.Args[3]
    homeCurrency := os.Args[4]

    resultsHaveBeenPrintedChannel := make(chan struct{})

    collectorIn := startCollector(partner, homeCurrency,resultsHaveBeenPrintedChannel)
    time.Sleep(time.Second)
    converterIn := startCurrencyConverter(homeCurrency, collectorIn, exchangeRatesFilePath)
    time.Sleep(time.Second)
    startReader(transactionsFilePath,converterIn)

    //blocks until results have been printed
    <-resultsHaveBeenPrintedChannel
    fmt.Println("Results channel closed, exiting main")

    elapsed := time.Since(start)
    fmt.Printf("Excution took %s\n", elapsed)
}

type Transaction struct {
    PartnerName string
    Amount float32
    Currency string
}


/// Collector
func startCollector(partner string, homeCurrency string, resultsHaveBeenPrintedChannel chan struct{}) chan Transaction {
    fmt.Println("Starting Collector")
    collectorIn := make(chan Transaction)

    go func() {
        //result object
        fmt.Printf("Calculating partner totals for [%s]...\n", partner)
        aggregatedTransactions := make(map[string]float32)

        for {
            message, more := <-collectorIn
            if (more) {
                aggregatedTransactions[message.PartnerName] += message.Amount
            } else {
                fmt.Println("Collector: channel closed, breaking...")
                break
            }
        }

        fmt.Println("Write results to disk")
        writeMapToDiskAsCsv(aggregatedTransactions)
        fmt.Printf("%.02f (for partner %s and currency %s)\n", aggregatedTransactions[partner], partner, homeCurrency)

        resultsHaveBeenPrintedChannel<-struct{}{}
        fmt.Println("Collector routine terminating...");
    }()

    return collectorIn
}

func writeMapToDiskAsCsv(records map[string]float32) {
    csvfile, err := os.Create("aggregated_transactions_by_partner.csv")
    check(err)
    defer csvfile.Close()

    for k, v := range records {

        _, err = csvfile.WriteString(fmt.Sprintf("%s,%.02f\n", k, v))
        if err != nil {
            panic(err)
        }
    }
}

// Reader
func startReader(transactionsFilePath string, nextStage chan<- Transaction) {

    go func() {
        fmt.Println("Starting Reader")
        //load transactions one line at a time and start aggregating results
        csvfile, err := os.Open(transactionsFilePath) //"/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/transactions2.csv"
        check(err)
        defer csvfile.Close()

        reader := csv.NewReader(csvfile)
        reader.FieldsPerRecord = 3 // Expected records per line

        for i:=1;;i++ {
            transactionLine, err := reader.Read() //Reaad one line at a time

            if ((err != nil)&&(err == io.EOF)) {
                fmt.Println("Reader: reached end of file, breaking");
                close(nextStage)
                break
            }

            partnerName := transactionLine[0]
            currency := transactionLine[1]
            amount, err := strconv.ParseFloat(transactionLine[2], 32)
            check(err)

            nextStage <- Transaction{partnerName, float32(amount), currency}
            if(i%1000000==0) {
                fmt.Println(i, "records processed...")
            }
        }
        fmt.Println("Reader routine terminating");
    }()

}

// Currency converter
func startCurrencyConverter(homeCurrency string, nextStage chan Transaction, exchangeRatesFilePath string) chan<- Transaction {
    fmt.Println("startCurrencyConverter")
    currencyConverterIn := make(chan Transaction)

    go func() {
        defer close(nextStage)

        //load rates into map
        fmt.Println("Loading exchange rates...")
        exchangeRates := loadExchangeRates(exchangeRatesFilePath) //"/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv"

        for {
            transaction,more := <-currencyConverterIn

            if (more) {
                convertedAmount := convertToHomeAmount(homeCurrency, exchangeRates, transaction)
                nextStage <- Transaction{transaction.PartnerName, convertedAmount, transaction.PartnerName}
            } else {
                fmt.Println("Currency covertor: incoming channel closed, breaking.")
                break
            }
        }
        fmt.Println("Currency Converter routine terminating...");
    }()

    return currencyConverterIn
}

type Key struct {
    FromCurrency string
    ToCurrency string
}

func loadExchangeRates(filePath string) map[Key]float32 {
    csvfile, err := os.Open(filePath)
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

func convertToHomeAmount(homeCurrency string, exchangeRates map[Key]float32, transaction Transaction) float32 {
    if (transaction.Currency==homeCurrency) {
        return transaction.Amount
    } else {
        return transaction.Amount*exchangeRates[Key{transaction.Currency, homeCurrency}]
    }
}

//utils
func check(e error) {
    if e != nil {
        panic(e)
    }
}
