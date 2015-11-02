package hbase

import (
	"github.com/pingcap/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
)

type action interface {
	ToProto() pb.Message
}

func (c *client) do(table, row []byte, action action, useCache bool, retries int) chan pb.Message {
	region := c.LocateRegion(table, row, useCache)
	if region == nil {
		return nil
	}
	conn := c.getRegionConn(region.Server)
	if conn == nil {
		return nil
	}

	regionSpecifier := &proto.RegionSpecifier{
		Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte(region.Name),
	}

	var cl *call = nil
	switch a := action.(type) {
	case *Get:
		cl = newCall(&proto.GetRequest{
			Region: regionSpecifier,
			Get:    a.ToProto().(*proto.Get),
		})
	case *Put, *Delete:
		cl = newCall(&proto.MutateRequest{
			Region:   regionSpecifier,
			Mutation: a.ToProto().(*proto.MutationProto),
		})

	case *CoprocessorServiceCall:
		cl = newCall(&proto.CoprocessorServiceRequest{
			Region: regionSpecifier,
			Call:   a.ToProto().(*proto.CoprocessorServiceCall),
		})
	}

	result := make(chan pb.Message)

	go func() {
		r := <-cl.responseCh

		switch r.(type) {
		case *exception:
			if retries <= c.maxRetries {
				// retry action, and refresh region info
				log.Infof("Retrying action for the %d time", retries+1)
				newr := c.do(table, row, action, false, retries+1)
				result <- <-newr
			} else {
				result <- r
			}
			return
		default:
			result <- r
		}
	}()

	if cl != nil {
		err := conn.call(cl)

		if err != nil {
			log.Warningf("Error return while attempting call [err=%#v]", err)
			// purge dead server
			delete(c.cachedConns, region.Server)

			if retries <= c.maxRetries {
				// retry action
				log.Infof("Retrying action for the %d time", retries+1)
				c.do(table, row, action, false, retries+1)
			}
		}
	}

	return result
}
