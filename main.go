package main

/* import "fmt" */
import "os"
import "github.com/AlekSi/pq"
import "database/sql"
import "flag"

func main() {
	cache := flag.Bool("cache-hit", false, "report on pg cache hit")
	index_usage := flag.Bool("index-usage", false, "report on pg index usage")
	seq_scans := flag.Bool("seq-scans", false, "report on pg seq scans")
	inflight := flag.Bool("inflight", false, "report on currently running queries")
	diskbased := flag.Bool("diskbased", false, "report on queries that went to disk")
	flag.Parse()

	// this respects all of the postgres environment vars:
	// http://www.postgresql.org/docs/9.3/static/libpq-envars.html
	connection_string, err := pq.ParseURL(os.Getenv("PGCONNECTIONSTR"))
	check(err)

	db, err := sql.Open("postgres", connection_string)
	check(err)

	// fail on connection early
	PING(db)

	if *diskbased {
		report_on_diskbased_queries(db)
	}

	if *cache {
		report_on_cache_hits(db)
	}

	if *index_usage {
		report_on_index_usage(db)
	}

	if *seq_scans {
		report_on_seq_scans(db)
	}

	if *inflight {
		report_on_inflight(db)
	}
}
