package sqlhelper

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type database struct {
	db *sql.DB
}

type Database interface {
	GetAll(sql string, args ...interface{}) ([]map[string]interface{}, error)
}

// const (
// 	DRIVER_NAME = "mysql"
// 	USER_NAME   = "root"
// 	PASS_WORD   = "123456"
// 	HOST        = "localhost"
// 	PORT        = "3306"
// 	DATABASE    = "imooc"
// 	CHARSET     = "utf8"
// )

// func NewMysqlConn(username string, password string, host string, port string, database string, charset string) (*Dblib, error) {
//dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True&loc=Local", username, password, host, port, database, charset)

// 初始化链接
func OpenDatabase(conn string) (Database, error) {

	// 打开连接失败
	db, err := sql.Open("mysql", conn)
	//defer MysqlDb.Close();
	if err != nil {
		log.Println("dbDSN: " + conn)
		return nil, err
		//panic("数据源配置不正确: " + MysqlDbErr.Error())
	}

	// 最大连接数
	db.SetMaxOpenConns(100)
	// 闲置连接数
	db.SetMaxIdleConns(20)
	// 最大连接周期
	db.SetConnMaxLifetime(100 * time.Second)

	if err = db.Ping(); nil != err {
		// panic("数据库链接失败: " + err.Error())
		return nil, err
	}

	p := new(database)
	p.db = db
	return p, nil
}

// type dbRow map[string]interface{}

func scanRow(rows *sql.Rows) (map[string]interface{}, error) {
	columns, _ := rows.ColumnTypes()
	// columns, _ := rows.Columns()

	vals := make([]interface{}, len(columns))
	valsPtr := make([]interface{}, len(columns))

	for i := range vals {
		valsPtr[i] = &vals[i]
	}

	err := rows.Scan(valsPtr...)

	if err != nil {
		return nil, err
	}

	r := make(map[string]interface{})

	for i, col := range columns {

		switch col.DatabaseTypeName() {
		case "INT", "DECIMAL":
			if vals[i] == nil {
				r[col.Name()] = nil
			} else {
				r[col.Name()], _ = strconv.ParseFloat(string(vals[i].([]byte)), 64)
			}
		case "TINYINT":
			if vals[i] == nil {
				r[col.Name()] = nil
			} else {
				if string(vals[i].([]byte)) == "1" {
					r[col.Name()] = true
				} else {
					r[col.Name()] = false
				}
			}
		case "DATETIME":
			if vals[i] == nil {
				r[col.Name()] = nil
			} else {
				r[col.Name()] = vals[i].(time.Time).Format("2006-01-02 15:04:05")
			}
		case "VARCHAR", "TEXT":
			if vals[i] == nil {
				r[col.Name()] = nil
			} else {
				r[col.Name()] = string(vals[i].([]byte))
			}
		default:
			r[col.Name()] = vals[i]
			fmt.Printf("sqlhelper未指定数据类型:%+v\n", col)
		}
	}

	return r, nil

}

// 获取一行记录
func (d *database) GetOne(sql string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := d.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	rows.Next()
	result, err := scanRow(rows)
	return result, err
}

// 获取多行记录
func (d *database) GetAll(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			continue
		}

		result = append(result, r)
	}

	return result, nil

}

// 写入记录
func (d *database) Insert(table string, data map[string]interface{}) (int64, error) {
	fields := make([]string, 0)
	vals := make([]interface{}, 0)
	placeHolder := make([]string, 0)

	for f, v := range data {
		fields = append(fields, f)
		vals = append(vals, v)
		placeHolder = append(placeHolder, "?")
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) ", table, strings.Join(fields, ","), strings.Join(placeHolder, ","))
	result, err := d.db.Exec(sql, vals...)
	if err != nil {
		return 0, err
	}

	lID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lID, nil
}

// 更新记录
func (d *database) Update(table, condition string, data map[string]interface{}, args ...interface{}) (int64, error) {
	params := make([]string, 0)
	vals := make([]interface{}, 0)

	for f, v := range data {
		params = append(params, f+"=?")
		vals = append(vals, v)
	}

	sql := "UPDATE %s SET %s"
	if condition != "" {
		sql += " WHERE %s"
		sql = fmt.Sprintf(sql, table, strings.Join(params, ","), condition)
		vals = append(vals, args...)
	} else {
		sql = fmt.Sprintf(sql, table, strings.Join(params, ","))
	}

	result, err := d.db.Exec(sql, vals...)
	if err != nil {
		return 0, err
	}

	aID, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return aID, nil
}

// 删除记录
func (d *database) Delete(table, condition string, args ...interface{}) (int64, error) {
	sql := "DELETE FROM %s "
	if condition != "" {
		sql += "WHERE %s"
		sql = fmt.Sprintf(sql, table, condition)
	} else {
		sql = fmt.Sprintf(sql, table)
	}

	result, err := d.db.Exec(sql, args...)
	if err != nil {
		return 0, err
	}

	aID, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return aID, nil

}
