package main

import (
	sql "database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// sequence vector maintain logical clocks of last received operation from every peers
// including itself. Assumming all logical clocks start at 0.
// the content of the seqVector will need to be stored to disk. (what happens if crashed without saving?)
// (Ans: can be by operation log and the Sync protocol)
// Moved from connection.go into this file
// TODO: may need to cleanup the initilization in connection.go
// Currently, seqVector is initialized in connection.go
var seqVector map[string]uint64

// operations database handle
var opsdb *sql.DB // from "database/sql"

// long-lived Statement
var Stmt *sql.Stmt

// This only init Ops storage
func InitStorage() {
	//createSeqVStorage()
	CreateOpsStorage()
}

// Used localClient and peerAddresses in connection.go
// CAUTION: This function is deprecated and should not be used
func createSeqVector() {

	seqVector = make(map[string]uint64) // seqVector global

	// global var localClient
	seqVector[localClient] = 0 // clock starts at 0

	// global slices peerAddresses
	for i := range peerAddresses {
		seqVector[peerAddresses[i]] = 0 // intialize to 0
	}

}

// This function creates operations storage and prepares two statements (insert/delete)
// Each operation in the table is a tuple <Atom, operation, clock, Pos>
// clock is the primary key
// Current implementation assumes each client is with a single ops table
// It only supports a single file. Note that opsdb will remain open
func CreateOpsStorage() {
	//Open is used to create a database handle
	path := "./ops" + clientID + ".db"
	var err error
	opsdb, err = sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	//It is rare to Close a DB, as the DB handle is meant to be long-lived and shared between many goroutines.
	//defer db.Close()

	sqlStmt := `
	create table ops (
		 clock integer not null primary key,
		 atom text,
		 operation integer,
		 posIdentifier blob
		 );
	delete from ops;
	`
	_, err = opsdb.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	Stmt, err = opsdb.Prepare("insert into ops(clock, atom, operation, posIdentifier) values(?, ?, ?, ?)")

	if err != nil {
		log.Fatal(err)
	}

	// do not close the Stmt yet, as it will be used over and over again
}

func writeOpToStorage(value string, OpType bool, clock uint64, pos []Identifier) error {

	// convert pos to bytes array
	posBytes := PosBytes(pos)

	_, err := Stmt.Exec(clock, value, OpType, posBytes)
	if err != nil {
		log.Fatal(err)
		return errors.New("unable to write to ops table")
	}

	return nil
}

// Given seqVector in runtime, the function creates seqV storage in file system
// If the table does not exist and that
// If the file currently being edited has no name, _seqV.db will be created.
// If the file has name <filename>, <filename>_seqV.db will be created.
// For now, let's be simple, just name it to be seqV.db
// Note: This func has not been tested
func createSeqVStorage() {

	// suffix := "_seqV.db"
	// prefix := "./"

	// path := prefix + CurView().Buf.name + suffix
	path := "./seqV" + clientID + ".db"

	// need to check whether the file exists, return immediately if so
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		// file does exist, load the table into SeqVector
		loadStorageIntoSeqVector() // TODO: table may not be fully created.
		return
	}

	// The file does not exist, this is a new file
	// create a seqVector table associated with the file
	db, err := sql.Open("sqlite3", path) // a potential issues is corruption with the database
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// creating the table
	sqlStmt := `
	create table seqV (
		 clientID text not null primary key,
		 clock integer
		 );
	delete from seqV;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	//loadStorageIntoSeqVector()
	// This is the first time using the seqVector, set to 0s
	resetSeqVector()
	// update storage
	SeqVectorToStorage()
}

// This function saves seqVector back to storage
func SeqVectorToStorage() {
	path := "./seqV" + clientID + ".db"

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin() // transaction
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("insert into seqV(clientID, clock) values(?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// iterating over the map
	// Note: it is assumed seqVector DS has been created
	for clientK, clockV := range seqVector {
		_, err = stmt.Exec(clientK, clockV)
		if err != nil {
			log.Fatal(err)
		}
	}

	tx.Commit()
}

// This function resets SeqVector to 0s
func resetSeqVector() {
	// initialize peerAddresses and seqVector first
	for i := range peerAddresses {
		seqVector[peerAddresses[i]] = 0 // intialize to 0
	}

}

// This function loads the storage into runtime seqVector.
// This should be called once when entangleText launchs
// Pre: seqVector must exist.
// Note: This func has not been tested
func loadStorageIntoSeqVector() {
	path := "./seqV" + clientID + ".db"
	//os.Remove(path) // TODO:

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// query
	rows, err := db.Query("select clientID, clock from seqV")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	counter := uint8(0)

	for rows.Next() {
		var clientID string
		var clock uint64
		err = rows.Scan(&clientID, &clock)
		if err != nil {
			log.Fatal(err)
		}
		seqVector[clientID] = clock // assignment
		counter = counter + 1
	}

	if counter != numPeers {
		err := errors.New("SeqVector table corrupted")
		fmt.Print(err)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

}

// This function select operations between receiverClock and localClock
// In this minimum where they are equal, the return value contains one operation
func ExtractOperationsBetween(ReceiverClock, localClock uint64) (patch []Operation) {
	// path := "./seqV" + clientID + ".db"

	// db, err := sql.Open("sqlite3", path)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer db.Close()
	// query on opsdb directly
	rows, err := opsdb.Query("select * from ops where clock between ? and ?", ReceiverClock, localClock) // select by range
	if err != nil {
		log.Fatal(err)
	} // as long as there’s an open result set (represented by rows), the underlying connection is busy and can’t be used for any other query.
	defer rows.Close() //We defer rows.Close(). This is very important.

	patch = make([]Operation, localClock-ReceiverClock+1)
	index := 0

	for rows.Next() {
		err = rows.Scan(&patch[index].Clock,
			&patch[index].Atom,
			&patch[index].OpType,
			&patch[index].Pos) // this obtains data
		if err != nil { // If there’s an error during the loop, you need to know about it.
			log.Fatal(err)
		}
		index = index + 1
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return patch
}
