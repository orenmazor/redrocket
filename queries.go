package main

import "database/sql"

import "fmt"

func PING(db *sql.DB) {
	var result string
	err := db.QueryRow("select version();").Scan(&result)

	check(err)

	fmt.Println(result)
}

func report_on_cache_hits(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tCache usage")
	fmt.Println("--------------------------------------")

	query := "SELECT 'index hit rate' AS name,(sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read),0) AS ratio FROM pg_statio_user_indexes UNION ALL SELECT 'table hit rate' AS name, sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read),0) AS ratio FROM pg_statio_user_tables;"
}

func report_on_index_usage(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tIndex usage")
	fmt.Println("--------------------------------------")

	query := "SELECT relname, CASE idx_scan WHEN 0 THEN 'Insufficient data' ELSE (100 * idx_scan / (seq_scan + idx_scan))::text END percent_of_times_index_used, n_live_tup rows_in_table FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"
}

func report_on_seq_scans(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tSeq Scans")
	fmt.Println("--------------------------------------")

	query := "SELECT relname AS name, seq_scan as count FROM pg_stat_user_tables ORDER BY seq_scan DESC;"
}
