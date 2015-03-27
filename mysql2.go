package mysql2

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type MySQL struct {
	*sql.DB
}

func Create(connStr string) (db MySQL, err error) {
	dt, err := sql.Open("mysql", connStr)
	db = MySQL{dt}
	return
}

func (db MySQL) SyncQuery(sql string) map[int]map[string]string {
	rows, err := db.Query(sql)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var cols, _ = rows.Columns()
	var others = make([]interface{}, len(cols))
	var data = make([][]byte, len(cols))
	for i, _ := range others {
		others[i] = &data[i]
	}

	var results = make(map[int]map[string]string)
	var i int = 0
	for rows.Next() {
		err = rows.Scan(others...)
		if err != nil {
			panic(err.Error())
		}
		results[i] = make(map[string]string)
		for k, v := range data {
			results[i][cols[k]] = string(v)
		}
		i++
	}
	return results
}

func (db MySQL) AsyncQuery(sql string, timeout time.Duration) <-chan map[string]string {
	var resultChan = make(chan map[string]string)
	go func() {
		defer close(resultChan)
		var results = db.SyncQuery(sql)
		if timeout == 0 {
			timeout = 500 * time.Millisecond
		}
		var timer = time.NewTimer(timeout)
		for _, info := range results {
			select {
			case resultChan <- info:
				timer.Stop()
				continue
			case <-timer.C:
				panic("TimeoutNew")
				break
			}
		}
	}()
	return resultChan
}
