package hbase

type Type byte

const (
	TypeMinimum             = Type(0)
	TypePut                 = Type(4)
	TypeDelete              = Type(8)
	TypeDeleteFamilyVersion = Type(10)
	TypeDeleteColumn        = Type(12)
	TypeDeleteFamily        = Type(14)
	TypeMaximum             = Type(0xff)
)

type set map[string]struct{}

func newSet() set {
	return set(map[string]struct{}{})
}

func (s set) exists(k string) bool {
	_, ok := s[k]
	return ok
}

func (s set) add(k string) {
	s[k] = struct{}{}
}

func (s set) remove(k string) {
	delete(s, k)
}
