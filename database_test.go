package sqlhelper

import (
	"fmt"
	"testing"
)

func TestQuery(t *testing.T) {
	// t.Error("hello world")
	username := ""
	password := ""
	server := ""
	db, _ := OpenDatabase(fmt.Sprintf("%s:%s@tcp(%s)/cmpdb?charset=utf8&parseTime=True&loc=Local", username, password, server))
	rows, _ := db.GetAll("select * from cmp_shop")
	t.Errorf("%+v\n", rows[0])
}
