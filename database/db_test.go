package database

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tool.site/mysql2es/config"
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
