package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
)

var benchTbl = "go-hbase-test"
var cli hbase.HBaseClient

func createTable(tblName string) error {
	desc := hbase.NewTableDesciptor(hbase.NewTableNameWithDefaultNS(tblName))
	desc.AddColumnDesc(hbase.NewColumnFamilyDescriptor("cf"))
	err := cli.CreateTable(desc, nil)
	if err != nil {
		dropTable(tblName)
		err = createTable(tblName)
		if err != nil {
			panic(err)
		}
	}
	return err
}

func dropTable(tblName string) {
	t := hbase.NewTableNameWithDefaultNS(tblName)
	cli.DisableTable(t)
	cli.DropTable(t)
}

func main() {
	//ts := time.Now()
	//prefix := fmt.Sprintf("%ld", ts.UnixNano())
	var err error
	cli, err = hbase.NewClient([]string{"localhost"}, "/hbase")
	if err != nil {
		panic(err)
	}
	dropTable(benchTbl)
	createTable(benchTbl)

	go func() {
		log.Error(http.ListenAndServe("localhost:8889", nil))
	}()

	ct := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var puts []*hbase.Put
			for j := 0; j < 1000; j++ {
				p := hbase.NewPut([]byte(fmt.Sprintf("row_%d_%d", i, j)))
				for k := 0; k < 1; k++ {
					p.AddStringValue("cf", "q"+strconv.Itoa(k), string(bytes.Repeat([]byte{'A'}, 1000)))
				}
				puts = append(puts, p)
			}
			cli.Puts(benchTbl, puts)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(ct)
	log.Errorf("took %s", elapsed)
}
