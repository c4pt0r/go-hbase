package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
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
	ts := time.Now()
	prefix := fmt.Sprintf("%ld", ts.UnixNano())
	var err error
	cli, err = hbase.NewClient([]string{"cuiqiu-pc"}, "/hbase")
	if err != nil {
		panic(err)
	}
	dropTable(benchTbl)
	createTable(benchTbl)

	go func() {
		log.Error(http.ListenAndServe("localhost:8889", nil))
	}()

	ct := time.Now()
	wg := &sync.WaitGroup{}
	for j := 0; j < 1000; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				p := hbase.NewPut([]byte(fmt.Sprintf("row_%s_%d_%d", prefix, i, j)))
				p.AddStringValue("cf", "q", "val_"+strconv.Itoa(i*j))
				_, err := cli.Put(benchTbl, p)
				if err != nil {
					log.Error(err)
					return
				}
			}
		}(j)
	}
	wg.Wait()
	elapsed := time.Since(ct)
	log.Errorf("took %s", elapsed)
}
