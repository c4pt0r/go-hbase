package hbase

import (
	"github.com/juju/errors"
	"github.com/pingcap/go-hbase/proto"
)

func (c *client) Delete(table string, del *Delete) (bool, error) {
	ch := c.do([]byte(table), del.GetRow(), del, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}
	return false, errors.Errorf("No valid response seen [response: %#v]", response)
}

func (c *client) Get(table string, get *Get) (*ResultRow, error) {
	ch := c.do([]byte(table), get.GetRow(), get, true, 0)
	if ch == nil {
		return nil, errors.Errorf("Create region server connection failed")
	}

	response := <-ch
	switch r := response.(type) {
	case *proto.GetResponse:
		return NewResultRow(r.GetResult()), nil
	case *exception:
		return nil, errors.New(r.msg)
	}
	return nil, errors.Errorf("No valid response seen [response: %#v]", response)
}

func (c *client) Put(table string, put *Put) (bool, error) {
	ch := c.do([]byte(table), put.GetRow(), put, true, 0)
	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}
	return false, errors.Errorf("No valid response seen [response: %#v]", response)
}

func (c *client) Puts(table string, puts []*Put) (bool, error) {
	actions := make([]multiaction, len(puts))

	for i, v := range puts {
		actions[i] = multiaction{
			row:    v.Row,
			action: v,
		}
	}

	ch := c.multi([]byte(table), actions, true, 0)
	for _ = range ch {
		// wait until all regions return
	}
	return true, nil
}

func (c *client) ServiceCall(table string, call *CoprocessorServiceCall) (*proto.CoprocessorServiceResponse, error) {
	ch := c.do([]byte(table), call.Row, call, true, 0)
	response := <-ch
	switch r := response.(type) {
	case *proto.CoprocessorServiceResponse:
		return r, nil
	case *exception:
		return nil, errors.New(r.msg)
	}
	return nil, errors.Errorf("No valid response seen [response: %#v]", response)
}
