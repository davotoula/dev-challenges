package main
import (
    "os"
    "fmt"
    "encoding/csv"
    "strconv"
)

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


}

type Transaction struct {
    PartnerName string
    Amount float64
    Currency string
}


/// Collector
func startCollector(partner string, homeCurrency string) chan Transaction {

    collectorIn := make(chan Transaction)

    go func(collectorInCopy chan Transaction) {
        //result object
        fmt.Printf("Calculating partner totals for [%s]...\n", partner)
        aggregatedTransactions := make(map[string]float32)

        for {
            message := collectorInCopy
            if (message!=nil) {
                aggregatedTransactions[message.PartnerName] += message.Amount
            } else {
                break
            }
        }

        writeMapToDiskAsCsv(aggregatedTransactions)
        fmt.Printf("%.02f (for partner %s and currency %s)\n", aggregatedTransactions[partner], partner, homeCurrency)

    }(collectorIn)

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
func startReader(transactionsFilePath string, ) {

    //load transactions one line at a time and start aggregating results
    csvfile, err := os.Open(transactionsFilePath) //"/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/transactions2.csv"
    check(err)
    defer csvfile.Close()

}

// Currency converter
func startCurrencyConverter(homeCurrency string, nextStage chan Transaction, exchangeRatesFilePath string) chan Transaction {

    currencyConverterIn := make(chan Transaction)

    go func() {
        //load rates into map
        fmt.Println("Loading exchange rates...")
        exchangeRates := loadExchangeRates(exchangeRatesFilePath) //"/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/exchangerates.csv"

        for {
            transaction := <-currencyConverterIn
            convertedAmount := convertToHomeAmount(homeCurrency, exchangeRates, transaction)
            nextStage <- Transaction{transaction.PartnerName, convertedAmount, transaction.PartnerName}
        }

    }()

    return currencyConverterIn
}

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
