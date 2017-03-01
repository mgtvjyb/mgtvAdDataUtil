package mgtvAdDataUtil

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"sync"
	"time"
)

var host = "ac1c27a32073d50c494243b4b7e33f56"
var user = "29307463389dbc8e53c75b69b5b5d325"
var passwd = "d1393a5e5f6329d8ae04c21daddb1211"
var port = "3c2902cc457e4dcc40e39ca2c7c64c18"
var dbname = "53547354c18777d63fee16738ba7fbf7"

var extract_hids = make([]string, 0)
var lock sync.RWMutex
var sa_hids = make([]string, 0)
var imp_hids = make([]string, 0)

func GetHidGroup(aeskey, date, hid string) string {
	if len(extract_hids) == 0 {
		lock.Lock()
		initCollectionIds(aeskey, date)
		lock.Unlock()
	}
	if Contains(sa_hids, hid) {
		return "SA"
	}
	if Contains(imp_hids, hid) {
		return "IMP"
	}
	return "B"
}

// var program_ids = []string{""}
func isLineNeed(aeskey, line string, hid_index int, date string) bool {
	_list := strings.Split(line, ",")
	if len(_list) > hid_index && hid_index >= 0 {
		return IsCollectionIdNeed(aeskey, _list[hid_index], date)
	}
	return false
}

/*
	初始化需要提取的合集id
	例如：分类的节目id，SA级，运营重点关注的几个合集，以及top100合集
*/
func IsCollectionIdNeed(aeskey, hid string, date string) bool {
	if len(extract_hids) == 0 {
		lock.Lock()
		initCollectionIds(date, aeskey)
		lock.Unlock()
	}
	if Contains(extract_hids, hid) {
		return true
	}
	return false
}
func initCollectionIds(date string, aeskey string) {
	if len(extract_hids) != 0 {
		return
	}
	dec_user, _ := AesCBCDecrypte(user, aeskey)
	dec_host, _ := AesCBCDecrypte(host, aeskey)
	dec_passwd, _ := AesCBCDecrypte(passwd, aeskey)
	dec_dbname, _ := AesCBCDecrypte(dbname, aeskey)
	dec_port, _ := AesCBCDecrypte(port, aeskey)

	db, err := sql.Open("mysql", dec_user+":"+dec_passwd+"@tcp("+dec_host+":"+dec_port+")/"+dec_dbname)
	if err != nil {
		fmt.Println("open mysql error")
		return
	}

	defer db.Close()

	loc, _ := time.LoadLocation("Local")
	cur_time, _ := time.ParseInLocation("20060102", date, loc)
	unix_cur := cur_time.Unix()
	sab_sql := fmt.Sprintf("select c_name,c_id,grade from amp.collection_grade where index_time=%d", unix_cur)
	rows, err := db.Query(sab_sql)
	if err != nil {
		fmt.Println("get sabinfo error date:", date, " error: ", err)
		return
	}
	result := getResult(rows)
	if len(result) == 0 {
		fmt.Println("get null sabinfo error date:", date)
	}
	for _, row := range result {
		cid := row[1]
		if !Contains(sa_hids, cid) {
			sa_hids = append(sa_hids, cid)
		}
		if Contains(extract_hids, cid) {
			continue
		}
		extract_hids = append(extract_hids, cid)
	}
	// 查top100的合集id
	top_sql := fmt.Sprintf("select hid  from stock_history.hid_req_count group by hid order by sum(count) desc limit 100")
	rows, err = db.Query(top_sql)
	if err != nil {
		fmt.Println("get top hid 100 error date:", date, " error: ", err)
		return
	}
	result = getResult(rows)
	if len(result) == 0 {
		fmt.Println("get null top 100 error date:", date)
	}
	for _, row := range result {
		hid := row[0]
		if !Contains(imp_hids, hid) {
			imp_hids = append(imp_hids, hid)
		}
		if Contains(extract_hids, hid) {
			continue
		}
		extract_hids = append(extract_hids, hid)
	}
	// TODO 查询重点资源合集
}

func getResult(rows *sql.Rows) [][]string {
	cols, err := rows.Columns()
	if err != nil {
		fmt.Println("query mysql error get rows columns", err)
		return nil
	}
	values := make([]interface{}, len(cols))
	for i, _ := range cols {
		var ii interface{}
		values[i] = &ii // scan的每一个元素都必须是个引用
	}
	var result = make([][]string, 0)
	for rows.Next() {
		rows.Scan(values...)
		row := make([]string, 0)
		for _, v := range values {
			raw_value := *v.(*interface{})
			b, _ := raw_value.([]uint8)
			row = append(row, string(b))
		}
		result = append(result, row)
	}
	return result
}

func Contains(strList []string, str string) bool {
	for _, strv := range strList {
		if strv == str {
			return true
		}
	}
	return false
}
