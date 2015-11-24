package hbase

import (
	"sync"

	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase/proto"
)

type action interface {
	ToProto() pb.Message
}

type multiaction struct {
	row    []byte
	action action
}

func merge(cs ...chan pb.Message) chan pb.Message {
	var wg sync.WaitGroup
	out := make(chan pb.Message)

	output := func(c <-chan pb.Message) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (c *client) multi(table []byte, actions []multiaction, useCache bool, retries int) chan pb.Message {
	actionsByServer := make(map[string]map[string][]multiaction)

	for _, action := range actions {
		region := c.LocateRegion(table, action.row, useCache)

		if _, ok := actionsByServer[region.Server]; !ok {
			actionsByServer[region.Server] = make(map[string][]multiaction)
		}

		if _, ok := actionsByServer[region.Server][region.Name]; ok {
			actionsByServer[region.Server][region.Name] = append(actionsByServer[region.Server][region.Name], action)
		} else {
			actionsByServer[region.Server][region.Name] = []multiaction{action}
		}
	}

	chs := make([]chan pb.Message, 0)

	for server, as := range actionsByServer {
		regionActions := make([]*proto.RegionAction, len(as))
		i := 0
		for region, acts := range as {
			racts := make([]*proto.Action, len(acts))
			for j, act := range acts {
				racts[j] = &proto.Action{
					Index: pb.Uint32(uint32(j)),
				}

				switch a := act.action.(type) {
				case *Get:
					racts[j].Get = a.ToProto().(*proto.Get)
				case *Put, *Delete:
					racts[j].Mutation = a.ToProto().(*proto.MutationProto)
				}
			}
			regionActions[i] = &proto.RegionAction{
				Region: &proto.RegionSpecifier{
					Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
					Value: []byte(region),
				},
				Action: racts,
			}
			i++
		}
		req := &proto.MultiRequest{
			RegionAction: regionActions,
		}
		cl := newCall(req)
		result := make(chan pb.Message)
		go func(actionsByServer map[string]map[string][]multiaction, server string) {
			r := <-cl.responseCh
			switch r.(type) {
			case *exception:
				actions := make([]multiaction, 0)
				for _, acts := range actionsByServer[server] {
					actions = append(actions, acts...)
				}
				newr := c.multi(table, actions, false, retries+1)
				for x := range newr {
					result <- x
				}
				return
			default:
				result <- r
			}
			close(result)
		}(actionsByServer, server)

		conn := c.getRegionConn(server)
		err := conn.call(cl)

		if err != nil {
			delete(c.cachedConns, server)
			cl.complete(err, nil)
		}

		chs = append(chs, result)
	}

	return merge(chs...)
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
				log.Warnf("Retrying action for the %d time(s)", retries+1)
				// clean old region info
				c.CleanRegionCache(table)
				RetrySleep(retries + 1)
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
			log.Warnf("Error return while attempting call [err=%#v]", err)
			// purge dead server
			delete(c.cachedConns, region.Server)

			if retries <= c.maxRetries {
				// retry action
				log.Infof("Retrying action for the %d time", retries+1)
				RetrySleep(retries + 1)
				c.do(table, row, action, false, retries+1)
			}
		}
	}

	return result
}
