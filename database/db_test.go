package database

import (
	"github.com/stretchr/testify/assert"
	"github.com/tianxinbaiyun/mysql2es/config"
	"testing"
)

func init() {
	config.InitConfig()
	InitDB()
}

func TestGetFieldList(t *testing.T) {
	list, err := GetFieldList("item")
	assert.NoError(t, err)
	t.Log(list)
}
