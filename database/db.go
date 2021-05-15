package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/tianxinbaiyun/mysql2es/config"

	_ "github.com/go-sql-driver/mysql" //mysql
)

// DB 数据库定义
var db *sql.DB

// InitDB 初始化连接
func InitDB() {
	db = GetDB(config.C.Mysql)
}

// GetDBName 获取库名
func GetDBName() string {
	return config.C.Mysql.Database
}

// GetDB 获取连接
func GetDB(conn config.MysqlConn) *sql.DB {
	if db != nil {
		return db
	}
	//root:root@tcp(127.0.0.1:3306)/test
	dsn := conn.User + ":" + conn.Pass + "@tcp(" + conn.Host + ":" + conn.Port + ")/" + conn.Database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// GetFieldList 获取数据库表字段列表
func GetFieldList(tableName string) (fields []string, err error) {
	sqlStr := "SELECT column_name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
	sqlStr = fmt.Sprintf(sqlStr, GetDBName(), tableName)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
		field := ""
		err = rows.Scan(&field)
		if err != nil {
			return
		}
		fields = append(fields, field)
	}
	return
}

// FetchRows 查询mysql数据
func FetchRows(table config.TableInfo, offset int64, size int64) (ret [][]string, newOffset int64, err error) {
	var rows *sql.Rows

	// 条件字符串拼接
	sl := make([]string, 0)
	sl = append(sl, table.Where...)
	andWhere := strings.Join(sl[:], " and ")
	andWhere = strings.Trim(andWhere, "and")
	var sqlStr = fmt.Sprintf("select * from %s", table.Name)
	if andWhere != "" && strings.Trim(andWhere, " ") != "1" {
		sqlStr = fmt.Sprintf("%s where %s", sqlStr, andWhere)
	}
	sqlStr = fmt.Sprintf("%s limit %d,%d", sqlStr, offset, size)

	log.Println(sqlStr)

	// 查询数据
	rows, err = db.Query(sqlStr)
	if err != nil {
		return nil, 0, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	colSize := len(columns)
	pts := make([]interface{}, colSize)
	container := make([]interface{}, colSize)
	for i := range pts {
		pts[i] = &container[i]
	}

	for rows.Next() {
		err = rows.Scan(pts...)
		if err != nil {
			return nil, 0, err
		}
		sl := toString(container)
		ret = append(ret, sl)
	}
	err = rows.Close()
	if err != nil {
		return nil, 0, err
	}
	if newOffset == 0 {
		newOffset = offset + size
	}
	log.Println("Fetched offset:", offset, " - size:", size)
	return ret, newOffset, nil
}

// FetchCount 获取mysql库表记录数量
func FetchCount(db *sql.DB, table config.TableInfo) (count int64, err error) {
	var rows *sql.Rows
	var sqlStr = "select count(*) as count from " + table.Name
	rows, err = db.Query(sqlStr)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	err = rows.Close()
	if err != nil {
		return 0, err
	}
	return count, nil
}

// toString 转成字符串
func toString(columns []interface{}) []string {
	var strCln []string
	for _, column := range columns {
		if column == nil {
			column = []uint8{'n', 'i', 'l'}
		}
		strCln = append(strCln, string(column.([]uint8)))
	}
	return strCln
}

// truncateTable 清空表数据
func truncateTable(db *sql.DB, table config.TableInfo) (err error) {
	var sqlStr = "truncate table " + table.Name
	log.Println(sqlStr)
	_, err = db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

// convertString 字段内容单引号转换
func convertString(arg string) string {
	var buf strings.Builder
	buf.WriteRune('\'')
	for _, c := range arg {
		if c == '\\' || c == '\'' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(c)
	}
	buf.WriteRune('\'')
	return buf.String()
}
