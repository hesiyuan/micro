package main

import (
	"database/sql"
	"log"
	"os"
)

// sequence vector maintain logical clocks of last received operation from every peers
// including itself. Assumming all logical clocks start at 0.
// the content of the seqVector will need to be stored to disk. (what happens if crashed without saving?)
// (Ans: can be by operation log and the Sync protocol)
// Moved from connection.go into this file
// TODO: may need to cleanup the initilization in connection.go
// Currently, seqVector is initialized in connection.go
var seqVector map[string]uint64

// Used localClient and peerAddresses in connection.go
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
func createSeqVStorage() {

	// suffix := "_seqV.db"
	// prefix := "./"

	// path := prefix + CurView().Buf.name + suffix
	path := "./seqV.db"
	os.Remove(path)

	db, err := sql.Open("sqlite3", path)
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

// This function loads the storage into runtime seqVector
// Pre: seqVector must exist
func loadStorageIntoSeqVector() {
	path := "./seqV.db"
	os.Remove(path)

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
	for rows.Next() {
		var clientID string
		var clock uint64
		err = rows.Scan(&clientID, &clock)
		if err != nil {
			log.Fatal(err)
		}
		seqVector[clientID] = clock // assignment
		//fmt.Println(clientID, clock)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

}
