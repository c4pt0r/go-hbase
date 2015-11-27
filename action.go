package hbase

import (
	"reflect"

	pb "github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase/proto"
)

type action interface {
	ToProto() pb.Message
}

func (c *client) do(table, row []byte, action action, useCache bool, retries int) (chan pb.Message, error) {
	region, err := c.LocateRegion(table, row, useCache)
	if err != nil {
		return nil, errors.Trace(err)
	}

	conn, err := c.getRegionConn(region.Server)
	if err != nil {
		return nil, errors.Trace(err)
	}

	regionSpecifier := &proto.RegionSpecifier{
		Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte(region.Name),
	}

	var cl *call
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
	default:
		return nil, errors.Errorf("Unknown action - %v - %v", reflect.TypeOf(action), action)
	}

	result := make(chan pb.Message)
	go func() {
		r := <-cl.responseCh

		switch r.(type) {
		case *exception:
			if retries <= c.maxRetries {
				// retry action, and refresh region info
				log.Warnf("Retrying action for the %d time(s), error - %v", retries+1, r)
				// clean old region info
				c.CleanRegionCache(table)
				RetrySleep(retries + 1)
				newr, err := c.do(table, row, action, false, retries+1)
				if err == nil {
					result <- <-newr
					return
				}

				log.Warnf("Retrying action for the %d time(s), error - %v", retries+1, errors.ErrorStack(err))
			}

			result <- r
			return
		default:
			result <- r
		}
	}()

	err = conn.call(cl)
	if err != nil {
		log.Warnf("Error return while attempting call [err=%#v]", err)

		// remove bad server cache.
		delete(c.cachedConns, region.Server)

		// retry action, do not use cache
		// Question: why use retry here?
		if retries <= c.maxRetries {
			log.Warnf("Retrying action for the %d time(s), error - %v", retries+1, errors.ErrorStack(err))
			RetrySleep(retries + 1)
			c.do(table, row, action, false, retries+1)
		}
	}

	return result, nil
}
