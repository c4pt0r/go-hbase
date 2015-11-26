package hbase

import (
	"bytes"
	"time"

	"github.com/pingcap/go-hbase/proto"
)

func RetrySleep(retries int) {
	time.Sleep(time.Duration(retries*500) * time.Millisecond)
}

func NewRegionSpecifier(regionName string) *proto.RegionSpecifier {
	return &proto.RegionSpecifier{
		Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte(regionName),
	}
}

func FindKey(region *RegionInfo, key []byte) bool {
	if region == nil {
		return false
	}
	// StartKey <= key < EndKey
	return (region.StartKey == nil || bytes.Compare(region.StartKey, key) <= 0) &&
		(region.EndKey == nil || len(region.EndKey) == 0 ||
			bytes.Compare(key, region.EndKey) < 0)
}
