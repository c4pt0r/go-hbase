package hbase

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/pingcap/go-hbase/iohelper"
	"github.com/pingcap/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
)

type idGenerator struct {
	n  int
	mu *sync.RWMutex
}

func newIdGenerator() *idGenerator {
	return &idGenerator{
		n:  0,
		mu: &sync.RWMutex{},
	}
}

func (a *idGenerator) get() int {
	a.mu.RLock()
	v := a.n
	a.mu.RUnlock()
	return v
}

func (a *idGenerator) incrAndGet() int {
	a.mu.Lock()
	a.n++
	v := a.n
	a.mu.Unlock()
	return v
}

type connection struct {
	mu           sync.Mutex
	addr         string
	conn         net.Conn
	bw           *bufio.Writer
	idGen        *idGenerator
	isMaster     bool
	in           chan *iohelper.PbBuffer
	ongoingCalls map[int]*call
}

func processMessage(msg []byte) [][]byte {
	buf := pb.NewBuffer(msg)
	payloads := make([][]byte, 0)

	for {
		hbytes, err := buf.DecodeRawBytes(true)
		if err != nil {
			break
		}

		payloads = append(payloads, hbytes)
	}

	return payloads
}

func readPayloads(r io.Reader) ([][]byte, error) {
	nBytesExpecting, err := iohelper.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if nBytesExpecting > 0 {
		buf, err := iohelper.ReadN(r, nBytesExpecting)

		if err != nil && err == io.EOF {
			return nil, err
		}

		payloads := processMessage(buf)

		if len(payloads) > 0 {
			return payloads, err
		}
	}
	return nil, errors.New("unexcepted payload")
}

func newConnection(addr string, isMaster bool) (*connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &connection{
		addr:         addr,
		bw:           bufio.NewWriter(conn),
		conn:         conn,
		in:           make(chan *iohelper.PbBuffer, 20),
		isMaster:     isMaster,
		idGen:        newIdGenerator(),
		ongoingCalls: map[int]*call{},
	}
	err = c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *connection) init() error {
	err := c.writeHead()
	if err != nil {
		return err
	}
	err = c.writeConnectionHeader()
	if err != nil {
		return err
	}
	go func() {
		err := c.processMessages()
		if err != nil {
			log.Warn(err)
			return
		}
	}()
	go c.dispatch()
	return nil
}

func (c *connection) processMessages() error {
	for {
		msgs, err := readPayloads(c.conn)
		if err != nil {
			return err
		}

		var rh proto.ResponseHeader
		err = pb.Unmarshal(msgs[0], &rh)
		if err != nil {
			return err
		}

		callId := rh.GetCallId()
		c.mu.Lock()
		call, ok := c.ongoingCalls[int(callId)]
		if !ok {
			c.mu.Unlock()
			return fmt.Errorf("Invalid call id: %d", callId)
		}
		delete(c.ongoingCalls, int(callId))
		c.mu.Unlock()

		exception := rh.GetException()
		if exception != nil {
			call.complete(fmt.Errorf("Exception returned: %s\n%s", exception.GetExceptionClassName(), exception.GetStackTrace()), nil)
		} else if len(msgs) == 2 {
			call.complete(nil, msgs[1])
		}
	}
	return nil
}

func (c *connection) writeHead() error {
	buf := bytes.NewBuffer(nil)
	buf.Write(hbaseHeaderBytes)
	buf.WriteByte(0)
	buf.WriteByte(80)
	_, err := c.conn.Write(buf.Bytes())
	return err
}

func (c *connection) writeConnectionHeader() error {
	buf := iohelper.NewPbBuffer()
	service := pb.String("ClientService")
	if c.isMaster {
		service = pb.String("MasterService")
	}

	err := buf.WritePBMessage(&proto.ConnectionHeader{
		UserInfo: &proto.UserInformation{
			EffectiveUser: pb.String("pingcap"),
		},
		ServiceName: service,
	})
	if err != nil {
		return err
	}

	err = buf.PrependSize()
	if err != nil {
		return err
	}

	_, err = c.conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) dispatch() {
	for {
		select {
		case buf := <-c.in:
			c.bw.Write(buf.Bytes())
			if len(c.in) == 0 {
				c.bw.Flush()
			}
		}
	}
}

func (c *connection) call(request *call) error {
	id := c.idGen.incrAndGet()
	rh := &proto.RequestHeader{
		CallId:       pb.Uint32(uint32(id)),
		MethodName:   pb.String(request.methodName),
		RequestParam: pb.Bool(true),
	}

	request.id = uint32(id)

	bfrh := iohelper.NewPbBuffer()
	err := bfrh.WritePBMessage(rh)
	if err != nil {
		return err
	}

	bfr := iohelper.NewPbBuffer()
	err = bfr.WritePBMessage(request.request)
	if err != nil {
		return err
	}

	buf := iohelper.NewPbBuffer()
	//Buf=> | total size | pb1 size| pb1 size | pb2 size | pb2 | ...
	buf.WriteDelimitedBuffers(bfrh, bfr)

	c.mu.Lock()
	c.ongoingCalls[id] = request
	//n, err := c.conn.Write(buf.Bytes())
	c.in <- buf
	c.mu.Unlock()

	/*
		if err != nil {
			return err
		}

		if n != len(buf.Bytes()) {
			return fmt.Errorf("Sent bytes not match number bytes [n=%d] [actual_n=%d]", n, len(buf.Bytes()))
		}
	*/
	return nil
}

func (c *connection) close() error {
	return c.conn.Close()
}
