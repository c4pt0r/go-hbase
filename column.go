package hbase

import (
	"bytes"
	"io"
	"log"

	"github.com/c4pt0r/go-hbase/iohelper"
	"github.com/cznic/exp/lldb"
)

type Column struct {
	Family []byte
	Qual   []byte
}

func NewColumn(family, qual []byte) *Column {
	return &Column{
		Family: family,
		Qual:   qual,
	}
}

func (c *Column) Write(w io.Writer) {
	iohelper.WriteVarBytes(w, c.Family)
	iohelper.WriteVarBytes(w, c.Qual)
}

func (c *Column) String() string {
	b, err := lldb.EncodeScalars(c.Family, c.Qual)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}

func (c *Column) ParseFromString(s string) {
	pairs, err := lldb.DecodeScalars([]byte(s))
	if err != nil {
		log.Fatal(err)
	}
	c.Family = pairs[0].([]byte)
	c.Qual = pairs[1].([]byte)
}

type ColumnCoordinate struct {
	Table []byte
	Row   []byte
	Column
}

func NewColumnCoordinate(table, row, family, qual []byte) *ColumnCoordinate {
	return &ColumnCoordinate{
		Table: table,
		Row:   row,
		Column: Column{
			Family: family,
			Qual:   qual,
		},
	}
}

func (c *ColumnCoordinate) Write(w io.Writer) {
	iohelper.WriteVarBytes(w, c.Table)
	iohelper.WriteVarBytes(w, c.Row)
	c.Column.Write(w)
}

func (c *ColumnCoordinate) Equal(a *ColumnCoordinate) bool {
	return bytes.Compare(c.Table, a.Table) == 0 &&
		bytes.Compare(c.Row, a.Row) == 0 &&
		bytes.Compare(c.Family, a.Family) == 0 &&
		bytes.Compare(c.Qual, a.Qual) == 0
}

func (c *ColumnCoordinate) String() string {
	b, err := lldb.EncodeScalars(c.Table, c.Row, c.Family, c.Qual)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}

func (c *ColumnCoordinate) ParseFromString(s string) {
	pairs, err := lldb.DecodeScalars([]byte(s))
	if err != nil {
		log.Fatal(err)
	}
	c.Table = pairs[0].([]byte)
	c.Row = pairs[1].([]byte)
	c.Family = pairs[2].([]byte)
	c.Qual = pairs[3].([]byte)
}

func (c *ColumnCoordinate) ParseField(b iohelper.ByteMultiReader) error {
	table, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Table = table

	row, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Row = row

	family, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Family = family

	qual, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Qual = qual
	return nil
}

func (c *ColumnCoordinate) GetColumn() *Column {
	return &Column{
		Family: c.Family,
		Qual:   c.Qual,
	}
}
