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

func report_on_queued_queries(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tLong queued queries")
	fmt.Println("--------------------------------------")

	query := `
	select trim(database) as DB , w.query, 
	substring(q.querytxt, 1, 100) as querytxt, 
	w.service_class as class, 
	w.total_queue_time/1000000 as queue_seconds, 
	w.total_exec_time/1000000 exec_seconds, (w.total_queue_time+w.total_Exec_time)/1000000 as total_seconds 
	from stl_wlm_query w 
	left join stl_query q on q.query = w.query and q.userid = w.userid 
	where w.queue_start_Time >= dateadd(day, -7, current_Date) 
	and w.total_queue_Time > 0  and w.userid >1   
	and q.starttime >= dateadd(day, -7, current_Date) 
	order by w.total_queue_time desc, w.queue_start_time desc limit 35;
	`

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var db, query, querytxt, class, queue_seconds, exec_seconds, total_seconds string
		err := rows.Scan(&db, &query, &querytxt, &class, &queue_seconds, &exec_seconds, &total_seconds)
		check(err)
		fmt.Printf("%10s\t%10s\t%5s\t%5s\t%5\t%5s\t%5s\t%s\n", db, query, class, queue_seconds, exec_seconds, total_seconds, querytxt)
	}

	check(rows.Err())

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

func report_on_data_dist(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tData Distribution")
	fmt.Println("--------------------------------------")

	query := `
	select trim(pgn.nspname) as schema,
	trim(a.name) as table,
	decode(pgc.reldiststyle,0, 'even',1,det.distkey ,8,'all') as distkey, dist_ratio.ratio::decimal(10,4) as skew,
	isnull(det.head_sort, 'null') as "sortkey", b.mbytes,
	decode(b.mbytes,0,0,((b.mbytes/part.total::decimal)*100)::decimal(5,2)) as pct_of_total, a.rows
	from (select db_id, id, name, sum(rows) as rows,
	sum(rows)-sum(sorted_rows) as unsorted_rows
	from stv_tbl_perm a 
	group by db_id, id, name) as a 
	join pg_class as pgc on pgc.oid = a.id
	join pg_namespace as pgn on pgn.oid = pgc.relnamespace
	left outer join (select tbl, count(*) as mbytes 
	from stv_blocklist group by tbl) b on a.id=b.tbl
	inner join (select attrelid,
	min(case attisdistkey when 't' then attname else null end) as "distkey",
	min(case attsortkeyord when 1 then attname  else null end ) as head_sort ,
	max(attsortkeyord) as n_sortkeys,
	max(attencodingtype) as max_enc
	from pg_attribute group by 1) as det
	on det.attrelid = a.id
	inner join ( select tbl, max(mbytes)::decimal(32)/min(mbytes) as ratio 
	from (select tbl, trim(name) as name, slice, count(*) as mbytes
	from svv_diskusage group by tbl, name, slice ) 
	group by tbl, name ) as dist_ratio on a.id = dist_ratio.tbl
	join ( select sum(capacity) as  total
	from stv_partitions where part_begin=0 ) as part on 1=1
	where mbytes is not null 
	order by  mbytes desc;
	`

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var schema, table, distkey, skew, sortkey, mbytes, pct_of_total, rowcount string
		err := rows.Scan(&schema, &table, &distkey, &skew, &sortkey, &mbytes, &pct_of_total, &rowcount)
		check(err)
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n", schema, table, distkey, skew, sortkey, mbytes, pct_of_total, rowcount)

	}

	check(rows.Err())
}

func report_on_query_queues(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tClass backlog report")
	fmt.Println("--------------------------------------")

	query := `
	select service_class as svc_class, count(*),
	avg(datediff(microseconds, queue_start_time, queue_end_time)) as avg_queue_time,
	avg(datediff(microseconds, exec_start_time, exec_end_time )) as avg_exec_time
	from stl_wlm_query
	where service_class > 4
	group by service_class
	order by service_class;
	`

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var svc, count, avg_queue_time, avg_exec_time string
		err := rows.Scan(&svc, &count, &avg_queue_time, &avg_exec_time)
		check(err)
		fmt.Printf("%5s\t%10s\t%15s\t%15s\n", svc, count, avg_queue_time, avg_exec_time)
	}

	check(rows.Err())
}

func report_on_inflight(db *sql.DB) {
	fmt.Println("--------------------------------------")
	fmt.Println("\t\tActive Queries")
	fmt.Println("--------------------------------------")

	query := "select pg_user.usename, stv_inflight.text, to_char(stv_inflight.starttime, 'HH24:MI:SS') as starttime from stv_inflight LEFT OUTER JOIN pg_user ON pg_user.usesysid = stv_inflight.userid order by starttime asc;"

	rows, err := db.Query(query)
	check(err)

	defer rows.Close()

	for rows.Next() {
		var username, query, starttime string
		err := rows.Scan(&username, &query, &starttime)
		check(err)
		fmt.Printf("%10s\t%16s\t%s\n", starttime, strings.TrimSpace(username), strings.TrimSpace(query))
	}

	check(rows.Err())
}
