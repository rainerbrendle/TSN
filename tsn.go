// TSN
//
// Package for creating transaction sequence numbers
//
// The package offers a function to obtain a monotonically increasing "timestamp" from a "wall clock"
//
// Implementation is a PostgreSQL database servicing as the host of the wall clock.
// The PostgreSQL implementation is using a SEQUENCE there to be bound to a database instance there.
// The database name is used as the identifier of the wallclock
// The related database SEQUENCE object is implemented within the "clock" schema of the database
//
package tsn

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"strings"
	"sync"
)

// WallClock
//
// The object representing a Wallclock with a given name. There is only one Wallclock per one name.
//
// Technically this object serves as an anchor point to the database connection
// to be managed in a map with the clock "name" as a key
// Maintained in an internal map structure.
//
// to be checked, do we need to keep the database connection or is it good enough to just do the name
// mapping. Some performance considerations and test about the behaviour of the sql.DB object may be needed
//
//
type WallClock struct {
	db     *sql.DB // database connection (from database/sql, is pooled)
	name   string
	dbname string
}

// the global list of wallclocks in the process
var wallclocks = map[string]*WallClock{}

// a Read-write mutex to protect it`
var wallclocksRWLock sync.RWMutex

// database connect string,
// to be read from configuration
//
// We read from the environment at first (secure store needed eventually)
var dbTemplate string = ""

// helper function for tracing (some better idea needed)
func checkString(name, value string) {

	fmt.Printf("CHECK: %v %v\n", name, value)

}

// get the database connection string template (from environment for now)
func getDbTemplate() string {
	if dbTemplate == "" {
		dbTemplate = os.Getenv("WALLCLOCK_DB")
	}
	return dbTemplate
}

// translate clock name into db connection string
// conncetion string needs to have a $database$ variable to be replaced by the clock name
//
func dbname(name string) string {
	dn := strings.Replace(getDbTemplate(), "$database$", name, 1)

	checkString("generated database name", dn)
	return dn
}

// get a Wallclock object for a given name
func get(name string) *WallClock {

	// lock global map for reading
	wallclocksRWLock.RLock()
	wc, ok := wallclocks[name]
	wallclocksRWLock.RUnlock()

	if ok {
		checkString("get wallclock ok", name)
		return wc
	}

	wc = add(name)
	return wc
}

// helper function for error handling (go panic!)
func checkErr(trace string, err error) {

	if err != nil {
		fmt.Printf("ERROR: %#v\n", err)
		panic(err)
	}

}

// helper function for tracing a SQL return row
// some better idea needed eventually (->tracing)
func checkRow(row *sql.Row) {

	// fmt.Printf( "ROW: %#v\n", row )

}

// Test database connections
//
// Initial test for live database connection`
func ping(db *sql.DB) {
	var err error

	err = db.Ping()

	checkErr("ping", err)
}

// Main function (internal)
//
// Retrieve a new TSN from database as int64
func newTSN(db *sql.DB) int64 {

	var tsn int64

	row := db.QueryRow("select clock.new_tsn()")
	checkRow(row)

	err := row.Scan(&tsn)
	checkErr("newTSN", err)

	return tsn
}

// Add a new database connection
func add(name string) *WallClock {

	wc := new(WallClock)

	wc.name = name
	wc.dbname = dbname(name)
	db, err := sql.Open("postgres", wc.dbname)
	checkErr("add", err)

	wc.db = db

	// lock for writing
	wallclocksRWLock.Lock()
	wallclocks[name] = wc
	wallclocksRWLock.Unlock()

	// ping
	ping(db)

	return wc
}

// Get the wallclock for a given name
//
// Package export`
func GetWallClock(name string) (wc *WallClock, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("cannot create wall clock")

		}

	}()

	wc = get(name)

	return wc, err
}

// From a given wallclock object retrieve the next TSN
//
// Package Export
func (wc *WallClock) NewTSN() (tsn int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while reading TSN")

		}

	}()

	tsn = newTSN(wc.db)
	return
}
