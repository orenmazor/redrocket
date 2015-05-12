package main

import "database/sql"
import "strings"
import "fmt"

func PING(db *sql.DB) {
	var result string
	err := db.QueryRow("select version();").Scan(&result)

	check(err)

	fmt.Println(result)
}

func report_on_most_time_consuming(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tMost Time Consuming")
	fmt.Println("--------------------------------------")

	query := `
	select trim(database) as db, count(query) as times_called,
	max(substring (qrytext,1,80)) as qrytext, 
	min(run_minutes) as "min_minutes" ,
	max(run_minutes) as "max_minutes",
	avg(run_minutes) as "avg_minutes", sum(run_minutes) as total_minutes
	from (select userid, label, stl_query.query, 
	trim(database) as database, 
	trim(querytxt) as qrytext, 
	md5(trim(querytxt)) as qry_md5, 
	starttime, endtime, 
	(datediff(seconds, starttime,endtime)::numeric(12,2))/60 as run_minutes,     
	alrt.num_events as alerts, aborted 
	from stl_query 
	left outer join 
	(select query, 1 as num_events from stl_alert_event_log group by query ) as alrt 
	on alrt.query = stl_query.query
	where userid <> 1 and starttime >=  dateadd(day, -7, current_date)) 
	group by database, label, qry_md5, aborted
	order by total_minutes desc limit 10;
	`

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var database, times_called, qrytext, min_minutes, max_minutes, avg_minutes, total_minutes string
		err := rows.Scan(&database, &times_called, &qrytext, &min_minutes, &max_minutes, &avg_minutes, &total_minutes)
		check(err)
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n", database, times_called, min_minutes, max_minutes, avg_minutes, total_minutes, qrytext)
	}

	check(rows.Err())
}

func report_on_cache_hits(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tCache usage")
	fmt.Println("--------------------------------------")

	query := "SELECT 'index hit rate' AS name,(sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read),0) AS ratio FROM pg_statio_user_indexes UNION ALL SELECT 'table hit rate' AS name, sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read),0) AS ratio FROM pg_statio_user_tables;"
	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var username, query, duration string
		err := rows.Scan(&username, &query, &duration)
		check(err)
		fmt.Printf("%s\t%s\t%s\n", duration, username, query)
	}

	check(rows.Err())
}

func report_on_index_usage(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tIndex usage")
	fmt.Println("--------------------------------------")

	query := "SELECT relname, CASE idx_scan WHEN 0 THEN 'Insufficient data' ELSE (100 * idx_scan / (seq_scan + idx_scan))::text END percent_of_times_index_used, n_live_tup rows_in_table FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"
	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var username, query, duration string
		err := rows.Scan(&username, &query, &duration)
		check(err)
		fmt.Printf("%s\t%s\t%s\n", duration, username, query)
	}

	check(rows.Err())
}

func report_on_seq_scans(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tSeq Scans")
	fmt.Println("--------------------------------------")

	query := "SELECT relname AS name, seq_scan as count FROM pg_stat_user_tables ORDER BY seq_scan DESC;"
	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var username, query, duration string
		err := rows.Scan(&username, &query, &duration)
		check(err)
		fmt.Printf("%s\t%s\t%s\n", duration, username, query)
	}

	check(rows.Err())
}

func report_on_diskbased_queries(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tDiskbased Queries")
	fmt.Println("--------------------------------------")

	query := "select pg_user.usename, stl_querytext.text, svl_query_summary.rows, svl_query_summary.workmem/(1024*1024*1024) as workmem_mb, svl_query_summary.label from svl_query_summary left join STL_QUERYTEXT on svl_query_summary.query=stl_querytext.query left join pg_user on pg_user.usesysid=stl_querytext.userid where is_diskbased='t' order by workmem_mb desc, svl_query_summary.rows desc;"

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var username, query, rows_affected, workmem, label string

		err := rows.Scan(&username, &query, &rows_affected, &workmem, &label)
		check(err)
		fmt.Printf("%s\t%s\t%s\t%s\t%s\n", strings.TrimSpace(username), query, rows_affected, workmem, label)
	}

	check(rows.Err())
}

func report_on_inflight(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tActive Queries")
	fmt.Println("--------------------------------------")

	query := "select pg_user.usename, stv_inflight.text, (stv_inflight.starttime - getdate()) as duration from stv_inflight LEFT OUTER JOIN pg_user ON pg_user.usesysid = stv_inflight.userid order by duration desc;"

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var username, query, duration string
		err := rows.Scan(&username, &query, &duration)
		check(err)
		fmt.Printf("%s\t%s\t%s\n", duration, strings.TrimSpace(username), strings.TrimSpace(query))
	}

	check(rows.Err())
}
