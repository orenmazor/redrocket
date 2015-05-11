package main

import "database/sql"

import "fmt"

func PING(db *sql.DB) {
	fmt.Print("PING?...")
	var result string
	err := db.QueryRow("SELECT 1").Scan(&result)
	check(err)

	if result == "1" {
		fmt.Println("PONG")
	}
}
