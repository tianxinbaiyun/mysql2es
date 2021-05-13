package main

import (
	"testing"
	"tool.site/mysql2es/service"
)

func TestJob(t *testing.T) {
	service.Sync()
}
