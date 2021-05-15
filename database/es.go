package database

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
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

// ESCreatIndex 创建索引
func ESCreatIndex(index string) (err error) {
	_, err = ESClient.CreateIndex(index).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// ESIndexExist 索引是否存在
func ESIndexExist(index string) (ok bool) {
	ok, err := ESClient.IndexExists().Index([]string{index}).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// ESInsert 创建
func ESInsert(index string, esType string, data interface{}) (err error) {
	_, err = ESClient.Index().Index(index).Type(esType).BodyJson(data).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// ESQuery ESQuery 查询
func ESQuery(index, esType string) (data interface{}, err error) {

	data, err = ESClient.Get().Index(index).Type(esType).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// ESUpdate ESUpdate 更新
func ESUpdate(index, esType, ID string, data map[string]string) (err error) {
	_, err = ESClient.Update().
		Index(index).
		Type(esType).
		Id(ID).
		Doc(data).
		Do(context.Background())
	if err != nil {
		log.Println(err.Error())
	}

	return
}

// ESDeleteIndex 删除es表
func ESDeleteIndex(index, esType string) (ok bool, err error) {
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
