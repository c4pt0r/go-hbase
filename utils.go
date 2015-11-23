package hbase

import "time"

func RetrySleep(retries int) {
	time.Sleep(time.Duration(retries*500) * time.Millisecond)
}
