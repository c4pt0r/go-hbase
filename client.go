package hbase

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"

	pb "github.com/golang/protobuf/proto"

	"github.com/c4pt0r/go-hbase/proto"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
)

const (
	zkRootRegionPath = "/meta-region-server"
	zkMasterAddrPath = "/master"

	magicHeadByte            = 0xff
	magicHeadSize            = 1
	idLengthSize             = 4
	md5HexSize               = 32
	servernameSeparator      = ","
	rpcTimeout               = 30000
	pingTimeout              = 30000
	callTimeout              = 5000
	defaultMaxActionRetries  = 3
	socketDefaultRetryWaitMs = 200
)

var (
	hbaseHeaderBytes []byte = []byte("HBas")
	metaTableName    []byte = []byte("hbase:meta")
	metaRegionName   []byte = []byte("hbase:meta,,1")
)

type regionInfo struct {
	server         string
	startKey       []byte
	endKey         []byte
	name           string
	ts             string
	tableNamespace string
	tableName      string
}

type tableInfo struct {
	tableName string
	families  []string
}

// export client interface
type HBaseClient interface {
	Get(tbl string, g *Get) (*ResultRow, error)
	Put(tbl string, p *Put) (bool, error)
	Delete(tbl string, d *Delete) (bool, error)
	ServiceCall(table string, call *CoprocessorServiceCall) (*proto.CoprocessorServiceResponse, error)
}

// hbase client implemetation
var _ HBaseClient = (*client)(nil)

type client struct {
	mu               sync.RWMutex // for read/update region info
	zkClient         *zk.Conn
	zkHosts          []string
	zkRoot           string
	prefetched       map[string]bool
	cachedConns      map[string]*connection
	cachedRegionInfo map[string]map[string]*regionInfo
	maxRetries       int
	rootServerName   *proto.ServerName
	masterServerName *proto.ServerName
}

func serverNameToAddr(server *proto.ServerName) string {
	return fmt.Sprintf("%s:%d", server.GetHostName(), server.GetPort())
}

func NewClient(zkHosts []string, zkRoot string) (HBaseClient, error) {
	cl := &client{
		zkHosts:          zkHosts,
		zkRoot:           zkRoot,
		cachedConns:      make(map[string]*connection),
		cachedRegionInfo: make(map[string]map[string]*regionInfo),
		prefetched:       make(map[string]bool),
		maxRetries:       defaultMaxActionRetries,
	}
	err := cl.init()
	if err != nil {
		return nil, err
	}
	return cl, nil
}

func (c *client) decodeMeta(data []byte) (*proto.ServerName, error) {
	if data[0] != magicHeadByte {
		return nil, errors.New("unknown packet")
	}

	var n int32
	binary.Read(bytes.NewBuffer(data[1:]), binary.BigEndian, &n)
	dataOffset := magicHeadSize + idLengthSize + int(n)
	data = data[(dataOffset + 4):]

	var mrs proto.MetaRegionServer
	err := pb.Unmarshal(data, &mrs)
	if err != nil {
		return nil, err
	}

	return mrs.GetServer(), nil
}

// init and get root region server addr and master addr
func (c *client) init() error {
	zkclient, _, err := zk.Connect(c.zkHosts, time.Second*30)
	if err != nil {
		return err
	}
	c.zkClient = zkclient

	res, _, _, err := c.zkClient.GetW(c.zkRoot + zkRootRegionPath)
	if err != nil {
		return err
	}
	c.rootServerName, err = c.decodeMeta(res)
	if err != nil {
		return err
	}
	log.Debug("connect root region server...", c.rootServerName)
	conn, err := newConnection(serverNameToAddr(c.rootServerName))
	if err != nil {
		return err
	}
	// set buffered regionserver conn
	c.cachedConns[serverNameToAddr(c.rootServerName)] = conn

	res, _, _, err = c.zkClient.GetW(c.zkRoot + zkMasterAddrPath)
	if err != nil {
		return err
	}
	c.masterServerName, err = c.decodeMeta(res)
	if err != nil {
		return err
	}
	return nil
}

// get connection
func (c *client) getRegionConn(addr string) *connection {
	c.mu.RLock()
	if s, ok := c.cachedConns[addr]; ok {
		defer c.mu.RUnlock()
		return s
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	conn, err := newConnection(addr)
	if err != nil {
		log.Error(err)
		return nil
	}
	c.cachedConns[addr] = conn
	return conn
}

// http://stackoverflow.com/questions/27602013/correct-way-to-get-region-name-by-using-hbase-api
func (c *client) createRegionName(table, startKey []byte, id string, newFormat bool) []byte {
	if len(startKey) == 0 {
		startKey = make([]byte, 1)
	}

	b := bytes.Join([][]byte{table, startKey, []byte(id)}, []byte(","))

	if newFormat {
		m := md5.Sum(b)
		mhex := []byte(hex.EncodeToString(m[:]))
		b = append(bytes.Join([][]byte{b, mhex}, []byte(".")), []byte(".")...)
	}
	return b
}

func (c *client) parseRegion(rr *ResultRow) *regionInfo {
	if regionInfoCol, ok := rr.Columns["info:regioninfo"]; ok {
		offset := strings.Index(string(regionInfoCol.Value), "PBUF") + 4
		regionInfoBytes := regionInfoCol.Value[offset:]

		var info proto.RegionInfo
		err := pb.Unmarshal(regionInfoBytes, &info)
		if err != nil {
			log.Errorf("Unable to parse region location: %#v", err)
		}

		return &regionInfo{
			server:         string(rr.Columns["info:server"].Value),
			startKey:       info.GetStartKey(),
			endKey:         info.GetEndKey(),
			name:           bytes.NewBuffer(rr.Row).String(),
			tableNamespace: string(info.GetTableName().GetNamespace()),
			tableName:      string(info.GetTableName().GetQualifier()),
		}
	}
	log.Errorf("Unable to parse region location (no regioninfo column): %#v", rr)
	return nil
}

func (c *client) getMetaRegion() *regionInfo {
	return &regionInfo{
		startKey: []byte{},
		endKey:   []byte{},
		name:     string(metaRegionName),
		server:   serverNameToAddr(c.rootServerName),
	}
}

func (c *client) getCachedLocation(table, row []byte) *regionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tableStr := string(table)
	if regions, ok := c.cachedRegionInfo[tableStr]; ok {
		for _, region := range regions {
			if (len(region.endKey) == 0 ||
				bytes.Compare(row, region.endKey) < 0) &&
				(len(region.startKey) == 0 ||
					bytes.Compare(row, region.startKey) >= 0) {
				return region
			}
		}
	}
	return nil
}

func (c *client) updateRegionCache(table []byte, region *regionInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tableStr := string(table)
	if _, ok := c.cachedRegionInfo[tableStr]; !ok {
		c.cachedRegionInfo[tableStr] = make(map[string]*regionInfo)
	}
	c.cachedRegionInfo[tableStr][region.name] = region
}

func (c *client) locateRegion(table, row []byte, useCache bool) *regionInfo {
	// if user wants to locate meteregion, just return it
	if bytes.Equal(table, metaTableName) {
		return c.getMetaRegion()
	}

	// try to get from cache
	if r := c.getCachedLocation(table, row); r != nil && useCache {
		return r
	}

	// cache miss, try to update region info
	metaRegion := c.getMetaRegion()
	conn := c.getRegionConn(metaRegion.server)
	regionRow := c.createRegionName(table, row, "", true)

	call := newCall(&proto.GetRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: metaRegionName,
		},
		Get: &proto.Get{
			Row: regionRow,
			Column: []*proto.Column{&proto.Column{
				Family: []byte("info"),
			}},
			ClosestRowBefore: pb.Bool(true),
		},
	})

	conn.call(call)

	response := <-call.responseCh

	switch r := response.(type) {
	case *proto.GetResponse:
		res := r.GetResult()
		if r == nil {
			log.Warnf("Couldn't find the region: [table=%s] [row=%s] [region_row=%s]", table, row, regionRow)
			return nil
		}
		rr := NewResultRow(res)
		if region := c.parseRegion(rr); region != nil {
			c.updateRegionCache(table, region)
			return region
		}
	}
	return nil
}
