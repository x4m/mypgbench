package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

var (
	queriesCount       int64 = 0
	clientsCount       int64 = 0
	failedClientsCount int64 = 0
	waiting            int64 = 0
)

func main() {
	var queriesObserved int64 = 0
	var failedQueriesObserved int64 = 0
	ts := time.Now()
	for i := 0; ; i++ {
		go runConnectionLoop(i)
		time.Sleep(time.Second / 200)

		if time.Now().Sub(ts) > time.Second {
			ts = time.Now()
			fmt.Print(" QPS ", queriesCount-queriesObserved)
			queriesObserved = queriesCount
			fmt.Print(" FailPS ", failedClientsCount-failedQueriesObserved)
			failedQueriesObserved = failedClientsCount
			fmt.Print(" Conns ", clientsCount)
			fmt.Println(" Waiting ", waiting)
		}
	}
}

func runConnectionLoop(loopNumber int) {
	defer func() {
		atomic.AddInt64(&clientsCount, -1)
		atomic.AddInt64(&failedClientsCount, 1)

		if r := recover(); r != nil {
			//fmt.Println("RECOVER   ",r)
		}
	}()
	db := openConnection()

	atomic.AddInt64(&clientsCount, 1)
	for {
		doBegin(db)
		time.Sleep(time.Millisecond)
		doQuery(db)
		time.Sleep(5 * time.Millisecond)
		doCommit(db)

		var n = time.Duration(rand.Int31n(1000))
		time.Sleep(n * time.Millisecond)

		if rand.Intn(100) == 42 {
			doBegin(db)
			time.Sleep(time.Millisecond)
			doError(db)
			time.Sleep(time.Millisecond)
			doRollback(db)
		}

		if rand.Intn(1000) == 42 {
			fmt.Println(loopNumber, " is waiting")
			atomic.AddInt64(&waiting, 1)
			time.Sleep(time.Hour)
			fmt.Println(loopNumber, " go on")
			atomic.AddInt64(&waiting, -1)
		}

		if rand.Intn(50000) == 42 {
			fmt.Println(loopNumber, " quit")
			db.Close()
			return
		}
	}
}

func doBegin(db *sql.DB) {
	executeQuery(db, "begin;", false)
}

func doRollback(db *sql.DB) {
	executeQuery(db, "rollback;", false)
}

func doCommit(db *sql.DB) {
	executeQuery(db, "commit;", false)
}

func doQuery(db *sql.DB) {
	atomic.AddInt64(&queriesCount, 1)
	executeQuery(db, "select 1;", false)
}

func doError(db *sql.DB) {
	executeQuery(db, "DO $$BEGIN RAISE EXCEPTION 'asdf'; END $$;", true)
}

func executeQuery(db *sql.DB, query string, expectError bool) {
	_, err := db.Exec(query)
	if (err != nil) != expectError {
		panic(err)
	}
}

func openConnection() *sql.DB {
	db, err := sql.Open("postgres", "port=6432 sslmode=disable dbname=postgres")
	if err != nil {
		panic(err)
	}
	return db
}
