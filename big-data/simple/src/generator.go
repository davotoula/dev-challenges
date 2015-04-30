package main
import (
    "os"
    "bufio"
    "fmt"
    "math/rand"
    "strconv"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {

    filePath := "transactions_gen.csv"

    if (len(os.Args)<3) {
        fmt.Printf("=== Usage %s [number of partners, eg. 1000] [number of total transactions, eg. 1000000] [output file, defaults to transactions_gen.csv]\n", os.Args[0])
        os.Exit(0)
    }

    numPartners,_ := strconv.Atoi(os.Args[1])
    numTransactions,_ := strconv.Atoi(os.Args[2])

	if (len(os.Args[3]) > 0) {
		filePath = os.Args[3]
	}

    f, err := os.Create(filePath)
    check(err)
    defer f.Close()

    w := bufio.NewWriter(f)
    for i:=1; i<=numTransactions; i++ {
        transactionString := fmt.Sprintf("Partner %d,GBP,%f\n",rand.Intn(numPartners),rand.Float32()*1000)
        _, err := w.WriteString(transactionString)
        check(err)
        if (i%10000==0) {
            fmt.Println(i)
        }
    }

    w.Flush()
}
