package crontab

import (
	"github.com/robfig/cron/v3"
	"github.com/tianxinbaiyun/mysql2es/service"
	"log"
)

// AddCron 添加定时任务
func AddCron() {
	var err error
	c := cron.New()
	// 添加任务
	_, err = c.AddFunc("*/2 * * * *", func() {
		service.Sync()
	})
	if err != nil {
		log.Printf("AddCron err:%v", err)
	}
	c.Start()
}
