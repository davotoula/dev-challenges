package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
	"flag"
	"log"
	"runtime/pprof"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	//define input arguments
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	transactionsFilePath := flag.String("tf", "transactions.csv", "transactions file")
	exchangeRatesFilePath := flag.String("ef", "exchangerates.csv", "exchange rates file")
	partner := flag.String("partner", "Partner 1", "the partner name to group by")
	homeCurrency := flag.String("hc", "GBP", "Home currency to use")

	//parse for input arguments
	flag.Parse()

	if *cpuprofile != "" {
		fmt.Println("running profiler")
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}


	start := time.Now()

	//result object
	aggregatedTransactions := make(map[string]float32,200)

	//load rates into map
	fmt.Println("Loading exchange rates...")
	exchangeRates := loadExchangeRates(*exchangeRatesFilePath)

	//load transactions one line at a time and start aggregating results
	fmt.Printf("Calculating partner totals for [%s]...\n", *partner)
	csvfile, err := os.Open(*transactionsFilePath)
	check(err)
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = 3 // Expected records per line

	for i := 1; ; i++ {
		transactionLine, err := reader.Read() //Reaad one line at a time

		if (err != nil) && (err == io.EOF) {
			break
		}

		partnerName := transactionLine[0]
		aggregatedTransactions[partnerName] += convertToHomeAmount(*homeCurrency, exchangeRates, transactionLine)

		if i%1000000 == 0 {
			fmt.Println(i, "records processed...")
		}
	}

	//write aggregated transactions to disk
	writeMapToDiskAsCsv(aggregatedTransactions)

	//output aggregated total for specified partner to console
	fmt.Printf("%.02f (for partner %s and currency %s)\n", aggregatedTransactions[*partner], *partner, *homeCurrency)

	elapsed := time.Since(start)
	fmt.Printf("Excution took %s\n", elapsed)
}

func convertToHomeAmount(homeCurrency string, exchangeRates map[Key]float32, transactionLine []string) float32 {
	transactionCurrency := transactionLine[1]
	transactionAmount, _ := strconv.ParseFloat(transactionLine[2], 32)
	if transactionCurrency == homeCurrency {
		return float32(transactionAmount)
	} else {
		return float32(transactionAmount) * exchangeRates[Key{transactionCurrency, homeCurrency}]
	}
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
	ToCurrency   string
}

func loadExchangeRates(filePath string) map[Key]float32 {
	csvfile, err := os.Open(filePath)
	check(err)
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = 3 // Expected records per line

	rawCSVdata, err := reader.ReadAll() //Read all at once
	check(err)

	exchangeRates := make(map[Key]float32,100)
	for _, each := range rawCSVdata {
		fromCurrency := each[0]
		toCurrency := each[1]
		exchangeRate, _ := strconv.ParseFloat(each[2], 32)
		exchangeRates[Key{fromCurrency, toCurrency}] = float32(exchangeRate)
	}

	return exchangeRates
}
