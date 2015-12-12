package hbase

import (
	"bytes"
	"time"

	"github.com/pingcap/go-hbase/proto"

	"github.com/juju/errors"
)

func retrySleep(retries int) {
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

// TODO: The following functions can be moved later.
// ErrorEqual returns a boolean indicating whether err1 is equal to err2.
func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}

	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	return e1.Error() == e2.Error()
}

// ErrorNotEqual returns a boolean indicating whether err1 isn't equal to err2.
func ErrorNotEqual(err1, err2 error) bool {
	return !ErrorEqual(err1, err2)
}
