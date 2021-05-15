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
	err := ESCreatIndex("clx")
	assert.NoError(t, err)
}

func TestESInsert(t *testing.T) {
	data := `{
	"name": "clx",
	"country": "China",
	"age": 30,
	"date": "1987-03-07"
	}`
	err := ESInsert("clx", "man1", data)
	assert.NoError(t, err)
}

func TestESInsert2(t *testing.T) {
	data := `{
	"name": "clx3",
	"collect": "cat",
	"height": "165cm",
	"weight": "50kg"
	}`
	err := ESInsert("clx", "man1", data)
	assert.NoError(t, err)
}

func TestESQuery(t *testing.T) {

	data, err := ESQuery("clx", "man1")
	assert.NoError(t, err)
	t.Log(data)
}

func TestESIndexExist(t *testing.T) {
	ok := ESIndexExist("clx")
	t.Log(ok)
}

func TestESDelete(t *testing.T) {
	ok, err := ESDeleteIndex("clx", "man1")
	assert.NoError(t, err)
	t.Log(ok)
}
