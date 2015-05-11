package main

/* import "fmt" */
import "os"
import "github.com/AlekSi/pq"
import "database/sql"

func main() {
	// this respects all of the postgres environment vars:
	// http://www.postgresql.org/docs/9.3/static/libpq-envars.html
	connection_string, err := pq.ParseURL(os.Getenv("PGCONNECTIONSTR"))
	check(err)

	db, err := sql.Open("postgres", connection_string)
	check(err)

	// fail on connection early
	PING(db)
}
