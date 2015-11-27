package hbase

import (
	"time"

	"github.com/juju/errors"
)

func RetrySleep(retries int) {
	time.Sleep(time.Duration(retries*500) * time.Millisecond)
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
