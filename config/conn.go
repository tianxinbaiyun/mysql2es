package config

import (
	"io/ioutil"
	"log"
	"os"

	yaml "gopkg.in/yaml.v2"
)

// Config 配置信息 yaml 结构体
type Config struct {
	Version string      `yaml:"version"`
	Mysql   MysqlConn   `yaml:"mysql"`
	ES      EsConn      `yaml:"es"`
	Table   []TableInfo `yaml:"table"`
}

// MysqlConn 数据库连接结构体
type MysqlConn struct {
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Pass     string `yaml:"pwd"`
	Database string `yaml:"dbname"`
	Port     string `yaml:"port"`
}

// EsConn es连接结构体
type EsConn struct {
	Host  string `yaml:"host"`
	Port  string `yaml:"port"`
	Index string `yaml:"index"`
}

//TableInfo 表结构体
type TableInfo struct {
	Name    string   `yaml:"name"`
	Rebuild bool     `yaml:"rebuild"`
	Batch   int64    `yaml:"batch"`
	Where   []string `yaml:"where"`
}

// C 全局配置信息
var C = Config{}

// InitConfig 初始化配置
func InitConfig() {
	fileName := "./config.yaml"
	//目录不存在，从指定的目录找
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		fileName = "../config.yaml"
	}
	ret, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Println(err)
	}
	err = yaml.Unmarshal(ret, &C)
	if err != nil {
		panic(err)
	}
}
