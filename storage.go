package main

import (
	"database/sql"
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

// Init tables
func InitStorage() {
	createSeqVStorage()

}

// Used localClient and peerAddresses in connection.go
// CAUTION:This function is deprecated and should not be used
func createSeqVector() {

	seqVector = make(map[string]uint64) // seqVector global

	// global var localClient
	seqVector[localClient] = 0 // clock starts at 0

	// global slices peerAddresses
	for i := range peerAddresses {
		seqVector[peerAddresses[i]] = 0 // intialize to 0
	}

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
