package service

import (
	"github.com/tianxinbaiyun/mysql2es/config"
	"github.com/tianxinbaiyun/mysql2es/database"
	"log"
)

// Sync 同步函数
func Sync() {

	// 变量定义
	var (
		err      error
		rows     [][]string
		offset   int64
		fistFlag bool
	)

	// 读取配置文件到struct,初始化变量
	config.InitConfig()

	// 连接数据库
	database.InitES()
	database.InitDB()

	//同步数据
	for _, table := range config.C.Table {
		fields := make([]string, 0)
		// 获取表字段
		fields, err = database.GetFieldList(table.Name)
		if err != nil {
			return
		}

		// 如果配置重建，则清空数据
		if table.Rebuild {
			// 如果不存在，创建索引
			if !database.ESIndexExist(config.C.ES.Index) {
				err = database.ESCreatIndex(config.C.ES.Index)
				if err != nil {
					return
				}
			}
			// 请求索引对应的数据库
			_, err = database.ESDeleteIndex(config.C.ES.Index, table.Name)
			if err != nil {
				log.Println("err:", err)
				return
			}
		}

		fistFlag = true
		syncCount := 0

		for fistFlag || len(rows) > 0 {

			// 从新获取数据
			rows, offset, err = database.FetchRows(table, offset, table.Batch)
			if err != nil {
				log.Println("err:", err)
				return
			}

			rowLen := len(rows)

			if rowLen <= 0 {
				break
			}
			fistFlag = false

			// 循环插入数据
			for _, row := range rows {
				data := map[string]string{}
				for i, s := range row {
					data[fields[i]] = s
				}
				err = database.ESInsert(config.C.ES.Index, table.Name, data)
				if err != nil {
					log.Println("err:", err)
					return
				}
			}

			// 统计同步数量
			syncCount = syncCount + rowLen

			// 如果返回数量小于size，结束循环
			if int64(rowLen) < table.Batch {
				break
			}
		}
		log.Printf("sync done Table name:%s  sync count %d", table.Name, syncCount)
	}
	return
}
