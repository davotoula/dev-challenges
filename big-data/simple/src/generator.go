package main
import (
    "os"
    "bufio"
    "fmt"
    "math/rand"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {
    //input parameters
    numPartners := 1000
    numTransactions := 1000000
    filePath := "/Users/david.kaspar/CODE/dev-challenges/big-data/simple/src/transactions_gen.csv"

    f, err := os.Create(filePath)
    check(err)
    defer f.Close()

    w := bufio.NewWriter(f)
    for i:=0; i<numTransactions; i++ {
        transactionString := fmt.Sprintf("Partner %d,GBP,%f\n",rand.Intn(numPartners),rand.Float32()*1000)
        _, err := w.WriteString(transactionString)
        check(err)
        if (i%10000==0) {
            fmt.Println(i)
        }
    }

    w.Flush()
}
