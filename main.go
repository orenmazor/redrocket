package main

/* import "fmt" */
import "os"
import "github.com/AlekSi/pq"
import "database/sql"
import "flag"

func main() {
	inflight := flag.Bool("inflight", false, "report on currently running queries")
	diskbased := flag.Bool("diskbased", false, "report on queries that went to disk")
	most_time_consuming := flag.Bool("time-consuming", false, "report on most time consuming queries")
	data_dist := flag.Bool("data-dist", false, "report on data disk distribution")
	query_queues := flag.Bool("query-queues", false, "report on service query queues")
	queued_queries := flag.Bool("queued-queries", false, "report on queries that live in the queue too much")
	active_sessions := flag.Bool("sessions", false, "report on active sessions")
	flag.Parse()

	// this respects all of the postgres environment vars:
	// http://www.postgresql.org/docs/9.3/static/libpq-envars.html
	connection_string, err := pq.ParseURL(os.Getenv("PGCONNECTIONSTR"))
	check(err)

	db, err := sql.Open("postgres", connection_string)
	check(err)

	// fail on connection early
	PING(db)

	if *active_sessions {
		report_on_active_sessions(db)
	}

	if *queued_queries {
		report_on_queued_queries(db)
	}

	if *query_queues {
		report_on_query_queues(db)
	}
	if *data_dist {
		report_on_data_dist(db)
	}

	if *most_time_consuming {
		report_on_most_time_consuming(db)
	}

	if *diskbased {
		report_on_diskbased_queries(db)
	}

	if *inflight {
		report_on_inflight(db)
	}
}
