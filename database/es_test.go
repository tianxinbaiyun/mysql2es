package database

import (
	"github.com/stretchr/testify/assert"
	"github.com/tianxinbaiyun/mysql2es/config"
	"testing"
)

func init() {
	config.InitConfig()
	InitES()
}

func TestESCreatIndex(t *testing.T) {
	err := CreateESIndex("clx")
	assert.NoError(t, err)
}

func TestESInsert(t *testing.T) {
	data := `{
	"name": "clx",
	"country": "China",
	"age": 30,
	"date": "1987-03-07"
	}`
	err := InsertESData("clx", data)
	assert.NoError(t, err)
}

func TestESDelete(t *testing.T) {
	ok, err := DeleteESIndex("clx")
	assert.NoError(t, err)
	t.Log(ok)
}
