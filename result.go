package hbase

import "github.com/c4pt0r/go-hbase/proto"

type Kv struct {
	Row   []byte
	Ts    uint64
	Value []byte
	// history results
	Values map[uint64][]byte
	Column
}

type ResultRow struct {
	Row           []byte
	Columns       map[string]*Kv
	SortedColumns []*Kv
}

func NewResultRow(result *proto.Result) *ResultRow {
	// empty response
	if len(result.GetCell()) == 0 {
		return nil
	}
	res := &ResultRow{}
	res.Columns = make(map[string]*Kv)
	res.SortedColumns = make([]*Kv, 0)

	for _, cell := range result.GetCell() {
		res.Row = cell.GetRow()

		col := &Kv{
			Row: res.Row,
			Column: Column{
				Family: cell.GetFamily(),
				Qual:   cell.GetQualifier(),
			},
			Value: cell.GetValue(),
			Ts:    cell.GetTimestamp(),
		}

		colName := string(col.Column.Family) + ":" + string(col.Column.Qual)

		if v, exists := res.Columns[colName]; exists {
			// renew the same cf result
			if col.Ts > v.Ts {
				v.Value = col.Value
				v.Ts = col.Ts
			}
			v.Values[col.Ts] = col.Value
		} else {
			col.Values = map[uint64][]byte{col.Ts: col.Value}
			res.Columns[colName] = col
			res.SortedColumns = append(res.SortedColumns, col)
		}
	}
	return res
}
