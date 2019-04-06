package main

// This is the connection code with other peers for now.
import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// args in insert(args)
type InsertArgs struct {
	Pair     pair   // pair as defined in document.go
	Clock    uint64 // value of logical clock at the issuing client
	Clientid string // ip:port
}

// args in put(args)
type DeleteArgs struct {
	Pair     pair   // pair as defined in document.go
	Clock    uint64 // value of logical clock at the issuing client
	Clientid string // ip:port
}

// args in disconnect(args)
type ConnectArgs struct { // later need to have more fields
	Clientid string // client id who asks to connection
}

//SyncPhaseOneArgs
type SyncPhaseOneArgs struct {
	Clientid      string // requester
	SenderClock   uint64 // sender clock
	ReceiverClock uint64 // sender view of receiver clock
}

// args in disconnect(args)
type DisconnectArgs struct {
	Clientid string // client id who voluntarilly quit the editor
}

// Reply from service for all the API calls above.
// This is useful for ensuring delivery success
type ValReply struct {
	Val string // value; depends on the call
}

type EntangleClient int

// Command line arg. Can be based on a config file
var numPeers uint8

// local ip:port for the peer
var localClient string

// clientID
var clientID string

//a slice holding peer ip addresses
var peerAddresses []string

// a slice hoding rpc service of peers
var peerServices []*rpc.Client

// a insert char message from a peer.
func (ec *EntangleClient) Insert(args *InsertArgs, reply *ValReply) error {
	// decompose InsertArgs
	posIdentifier := args.Pair.Pos
	atom := []byte(args.Pair.Atom)
	peerClock := args.Clock
	peer := args.Clientid

	if string(atom) == "" || len(posIdentifier) == 0 {
		return nil
	}

	buf := CurView().Buf // buffer pointer, supports one tab currently
	// the CRDTIndex is the index for the atom to be inserted in the document
	CRDTIndex, _ := buf.Document.Index(posIdentifier)
	// converting CRDTIndex to lineArray pos
	LinePos := FromCharPos(CRDTIndex-1, buf) // off by 1
	// This directly insert to document and lineArray directly bypassing the eventsQueue
	// Let's insert to lineArray first
	buf.LineArray.insert(LinePos, atom)

	// now insert to document
	buf.Document.insert(posIdentifier, args.Pair.Atom)

	// update numoflines in lineArray
	buf.Update()

	RedrawAll()
	// set clock for the peer, don't need to increment
	seqVector[peer] = peerClock

	return nil
}

// a delete char message from a peer. Note: this is to delete only a single char
func (ec *EntangleClient) Delete(args *DeleteArgs, reply *ValReply) error {
	posIdentifier := args.Pair.Pos
	atom := []byte(args.Pair.Atom)
	peerClock := args.Clock
	peer := args.Clientid

	if string(atom) == "" || len(posIdentifier) == 0 {
		return nil
	}

	buf := CurView().Buf // buffer pointer, supports one tab currently
	// the CRDTIndex is the index for the atom to be deleted in the document
	CRDTIndex, _ := buf.Document.Index(posIdentifier)

	// converting CRDTIndex to lineArray pos
	LinePos := FromCharPos(CRDTIndex-1, buf) // CRDT_index is one index higher
	// This directly delet to document and lineArray directly bypassing the eventsQueue
	buf.LineArray.remove(LinePos, LinePos.right(buf)) // removing one char at LinePos

	// given position identifier, delete directly
	buf.Document.delete(posIdentifier)

	// update numoflines in lineArray
	buf.Update()

	RedrawAll()
	// set clock for the peer, don't need to increment
	seqVector[peer] = peerClock

	return nil
}

// Received connection request from a peer
func (ec *EntangleClient) Connect(args *ConnectArgs, reply *ValReply) error {
	// This set the global slice peerServices
	// currently only one peer, later need to be changed. TODO:
	peerServices[0], _ = rpc.Dial("tcp", peerAddresses[1])

	// the above may fail as well
	if peerServices[0] == nil {
		return errors.New("unable to connect to the requester")
	}
	// now, connected redraw the status line
	RedrawAll()

	// should not initiating pair-wise sync protocol here (this is receiver), just return
	// however, we may send the logical clock if we want.

	return nil
}

// received SyncPhaseOne from a peer
func (ec *EntangleClient) SyncPhaseOne(args *SyncPhaseOneArgs, reply *ValReply) error {
	//TODO

	return nil
}

// DISCONNECT from a peer.
func (ec *EntangleClient) Disconnect(args *DisconnectArgs, reply *ValReply) error {
	//TODO

	return nil
}

// write a init function here
// currently hardcoding stuff, but peers later may be given by a config file.
func InitConnections() {

	args := flag.Args() // args has been used by micro.go as filenames
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [local:port] [remote:port] [filenames]\n")

	if len(args) < 2 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	localClient = args[0] // local ip:port global
	clientID = assembleClientID(localClient)
	numPeers = 2 // including itself

	seqVector = make(map[string]uint64) // seqVector global

	peerAddresses = make([]string, 2)
	// initialize peerAddresses first
	for i := range peerAddresses {
		peerAddresses[i] = args[i]
	}

	// This fills in seqVector based on storage
	createSeqVStorage()

	// By now, seqVector and its storage has been created.

	// Setup and register service.
	entangleClient := new(EntangleClient)
	rpc.Register(entangleClient)

	// listen first
	l, err := net.Listen("tcp", localClient)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	// then dial
	peerServices = make([]*rpc.Client, 1) // must use "="" to assign global variables
	for i := range peerAddresses {
		if i == 0 { // peerAddresses[0] is now itself
			continue
		}
		// Connect to other peers via RPC.
		peerServices[i-1], err = rpc.Dial("tcp", peerAddresses[i]) // can dial periodically
		// based on the err, do not have to quit in checkError
		checkErrorAndConnect(err)
	}

	// this can also reside in the micro.go
	go func() {
		for {
			conn, _ := l.Accept()
			go rpc.ServeConn(conn)
		}
	}()

}

// // check
// func checkError (err error) {

// }

// If error is non-nil, print it out and halt.
// This function is augmented with a RPC call to connect, the remote peer
// will be requested to dial to this client as well, if no err.
func checkErrorAndConnect(err error) {
	if err != nil {
		//fmt.Fprintf(os.Stderr, "MyError: ", err.Error())
		fmt.Println("Error", err.Error())
		//os.Exit(1) // Let's do not exit, when in production
	} else { // if no error, RPC connect
		// now send to peers
		ConnectArgs := ConnectArgs{
			Clientid: localClient, // TODO
		}
		var kvVal ValReply
		// asynchronously, does not matter if synchronous. TODO: change invokee
		go func() {
			err := peerServices[0].Call("EntangleClient.Connect", ConnectArgs, &kvVal)
			// let's follow the original protocol
			// initiating pair-wise sync protocol here
			if err == nil {
				pairWiseSync(peerAddresses[0]) // TODO: change argument
			} else {
				fmt.Println("Error", err.Error())
			}
		}()

	}
}

// The pair-wise synchronization protocol here
func pairWiseSync(peer string) {
	// Phase one: requester sending <local clock, peer clock>
	// type SyncPhaseOneArgs struct {
	// 	Clientid      string // requester
	// 	SenderClock   uint64 // sender clock
	// 	ReceiverClock uint64 // sender view of receiver clock
	// }
	SyncPhaseOneArgs := SyncPhaseOneArgs{
		Clientid:      localClient,
		SenderClock:   seqVector[localClient],
		ReceiverClock: seqVector[peer],
	}
	var kvVal ValReply
	peerServices[0].Call("EntangleClient.SyncPhaseOne", SyncPhaseOneArgs, &kvVal)

	// get some results back from the requestee

	// based on the results, send patch in return to the requestee

}
