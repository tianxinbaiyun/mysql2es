package database

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/tianxinbaiyun/mysql2es/config"
	"log"
)

// ESClient es客户端
var ESClient *elastic.Client

// InitES InitES
func InitES() {
	GetESClient(config.C.ES)
}

// GetESClient 获取客户端，获取GetESClient
func GetESClient(conn config.EsConn) *elastic.Client {
	if ESClient != nil {
		return ESClient
	}
	client, err := elastic.NewClient(elastic.SetURL(fmt.Sprintf("http://%s:%s", conn.Host, conn.Port)), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}
	ESClient = client
	return ESClient
}

// CreateESIndex 创建索引
func CreateESIndex(index string) (err error) {
	_, err = ESClient.CreateIndex(index).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// GetESIndexExist 索引是否存在
func GetESIndexExist(index string) (ok bool) {
	ok, err := ESClient.IndexExists().Index([]string{index}).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// InsertESData 创建
func InsertESData(index string, data interface{}) (err error) {
	_, err = ESClient.Index().Index(index).BodyJson(data).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// UpdateESData 更新
func UpdateESData(index, ID string, data map[string]string) (err error) {
	_, err = ESClient.Update().
		Index(index).
		Id(ID).
		Doc(data).
		Do(context.Background())
	if err != nil {
		log.Println(err.Error())
	}

	return
}

// DeleteESIndex 删除es表
func DeleteESIndex(index string) (ok bool, err error) {
	res, err := ESClient.DeleteIndex().Index([]string{index}).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	if res.Acknowledged {
		ok = true
	}
	return
}
