package main

// This is the connection code with other peers for now.
import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
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

// This can be a little redundant and can be refactored later
type Operation struct {
	Atom   string // content
	OpType bool   // true for insert, false for delete
	Pos    []byte // a serilized position in bytes for sending and receiving
	Clock  uint64 // logical clock
}

//SyncPhaseOneReply.
type SyncPhaseOneReply struct {
	PhaseTwo       bool        // set to true, if second phase is required
	RequesterClock uint64      // receiver's view of requester's clock
	Patch          []Operation // operations
}

//SyncPhaseOneArgs
type SyncPhaseTwoArgs struct {
	Clientid string      // sender
	Patch    []Operation // operations
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

type peerInfo struct {
	IP_PORT string
	Share   bool // sharing or not at the moment
}

type EntangleClient int

// Command line arg. Can be based on a config file
var numPeers uint8

// local ip:port for the peer
var localClient string

// clientID
var clientID string

//a slice holding peer ip addresses
var peerAddresses []peerInfo

// a slice hoding rpc service of peers
var peerServices []*rpc.Client

// remote insertion to Lines lock
var linesLock = &sync.Mutex{}

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

	linesLock.Lock() // locking
	// first insert to document.
	dbID := NextDocID() // blocking
	buf.Document.insert(posIdentifier, args.Pair.Atom, dbID)
	// compute CRDTIndex after inserting to the document
	// the CRDTIndex is the index for the atom to be inserted in the document
	CRDTIndex, _ := buf.Document.Index(posIdentifier)

	// converting CRDTIndex to lineArray pos
	LinePos := FromCharPos(CRDTIndex-1, buf) // off by 1
	// This directly insert to document and lineArray directly bypassing the eventsQueue
	// Let's insert to lineArray first
	buf.LineArray.insert(LinePos, atom)
	// update numoflines in lineArray
	buf.Update()
	linesLock.Unlock()

	RedrawAll()
	// set clock for the peer, don't need to increment
	if seqVector[peer].Clock < peerClock {
		seqVector[peer].Clock = peerClock
		seqVector[peer].Dirty = true
	}

	// id passed into the anonymous function to resolve race conditions.
	// passed into arguments shouldnt matter actually over here. same applies to delete
	// can also do something similar in the delete function
	go func(id uint64, atom string, pos []Identifier) {
		err := InsertCharToDocDB(id, atom, pos)
		if err != nil {
			fmt.Println("Error", err.Error())
		}
	}(dbID, string(atom), posIdentifier)

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

	linesLock.Lock()
	buf := CurView().Buf // buffer pointer, supports one tab currently
	// the CRDTIndex is the index for the atom to be deleted in the document
	CRDTIndex, _ := buf.Document.Index(posIdentifier)
	// given position identifier, delete directly
	_, dbID := buf.Document.delete(posIdentifier)

	// converting CRDTIndex to lineArray pos
	LinePos := FromCharPos(CRDTIndex-1, buf) // CRDT_index is one index higher
	// This directly delet to document and lineArray directly bypassing the eventsQueue
	buf.LineArray.remove(LinePos, LinePos.right(buf)) // removing one char at LinePos
	// update numoflines in lineArray
	buf.Update()
	linesLock.Unlock()

	RedrawAll()
	// set clock for the peer, don't need to increment
	if seqVector[peer].Clock < peerClock { // only record the max clock
		seqVector[peer].Clock = peerClock
		seqVector[peer].Dirty = true
	}

	go func(id uint64) { // do this in a separate go routine, note it is set to false
		err := DeleteCharFromDocDB(id)
		if err != nil {
			fmt.Println("Error", err.Error())
		}
	}(dbID)

	return nil
}

// Received connection request from a peer
func (ec *EntangleClient) Connect(args *ConnectArgs, reply *ValReply) error {
	// This set the global slice peerServices
	// currently only one peer, later need to be changed. TODO:
	index, status := GetPeerServicesIndex(args.Clientid)

	if !status {
		return errors.New("peer not in my peerAddresses")
	}

	peerServices[index], _ = rpc.Dial("tcp", args.Clientid)

	// the above may fail as well
	if peerServices[index] == nil {
		return errors.New("unable to connect to the requester")
	}
	// now, connected redraw the status line
	RedrawAll()

	// should not initiating pair-wise sync protocol here (this is receiver), just return
	// however, we may send the logical clock if we want.

	return nil
}

// received SyncPhaseOne from a peer
func (ec *EntangleClient) SyncPhaseOne(args *SyncPhaseOneArgs, reply *SyncPhaseOneReply) error {
	//TODO
	//SyncPhaseOneArgs
	// type SyncPhaseOneArgs struct {
	// 	Clientid      string // requester
	// 	SenderClock   uint64 // sender clock
	// 	ReceiverClock uint64 // sender view of receiver clock
	// }

	// Requestee and Sender are synonyms, receiver is *this* client.
	// extract the current view of the requestee's clock.
	// This extracts from runtime DS
	requesterClock := seqVector[args.Clientid].Clock

	// if requesterClock == SenderClock, then we have the most updated ops from the sender
	// no need to proceed to the second phase os sync
	if requesterClock == args.SenderClock {
		reply.PhaseTwo = false
	} else if requesterClock < args.SenderClock {
		// then we need to request the sender as well for new ops
		// setting PhaseTwo to true and indicate in the reply.RequesterClock
		reply.RequesterClock = requesterClock
		reply.PhaseTwo = true
		// 	requesterClock update on seqVector to be done in the sync phase two

	} else {
		// if requesterClock > SenderClock, this case is unusual but may happen
		// for example, the storage on the sender side has corrupted and hence reset.
		// for now in this case, just return an error
		return errors.New("requesterClock > SenderClock")

	}

	localClock := seqVector[localClient].Clock
	if localClock == args.ReceiverClock {
		// the requester's view is up to date. no need to send patch

	} else if localClock > args.ReceiverClock {
		//then need to prepare patches to be sent to the requester
		// this will need to ask from storage, but we can have a buffered operations for efficiency
		// Currently, we assume every local operation is immediately write-back
		reply.Patch = ExtractOperationsBetween(args.ReceiverClock+1, localClock) // notice the plus one

	} else {
		// if localClock < ReceiverClock, this case is unusual but could happen
		// for instance, the local storage has corrupted and hence reset.
		// for now in this case, just return an error
		return errors.New("localClock < ReceiverClock")
	}
	// return no error
	return nil
}

// The second phase of the pair-wise Sync protocol
func (ec *EntangleClient) SyncPhaseTwo(args *SyncPhaseTwoArgs, reply *ValReply) error {

	//  just call insertPatch and update seqVector
	insertPatch(args.Patch)

	// update seqVector based on the last operation from the patch.
	// assuming patch contains in increasing clock values.
	seqVector[args.Clientid].Clock = args.Patch[len(args.Patch)-1].Clock
	seqVector[args.Clientid].Dirty = true

	return nil

}

// DISCONNECT from a peer.
func (ec *EntangleClient) Disconnect(args *DisconnectArgs, reply *ValReply) error {
	//TODO

	return nil
}

// This function inits all peers information based on
// currrently the arguments
func InitPeersInfo() {

	args := flag.Args() // args has been used by micro.go as filenames
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [local:port] [remote:port] [filenames]\n")

	//peerAddresses = make([]string, 2)

	if len(args) < 2 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	ReadConnectionConfig(args)

	localClient = peerAddresses[0].IP_PORT // local ip:port global
	clientID = assembleClientID(localClient)
	numPeers = uint8(len(peerAddresses)) // including itself

}

// read configuration file, intializes peerAddresses essentially
func ReadConnectionConfig(args []string) {
	// first argument is the config file
	file, err := os.Open(args[0])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var line, ip_port string
	var share bool
	for scanner.Scan() {
		line = scanner.Text()
		s := strings.Split(line, " ")
		ip_port = s[0]

		if s[1] == "S" {
			share = true
		} else {
			share = false
		}

		peer := peerInfo{
			ip_port,
			share,
		}

		peerAddresses = append(peerAddresses, peer)

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}

// write a init function here
// currently hardcoding stuff, but peers later may be given by a config file.
func InitConnections() {

	seqVector = make(map[string]*seqVEntry) // seqVector global

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
	peerServices = make([]*rpc.Client, numPeers-1) // must use "="" to assign global variables
	for i := range peerAddresses {
		if i == 0 { // peerAddresses[0] is now itself
			continue
		}

		if peerAddresses[i].Share == false {
			peerServices[i-1] = nil
			continue
		}
		// Connect to other peers via RPC.
		peerServices[i-1], err = rpc.Dial("tcp", peerAddresses[i].IP_PORT) // can dial periodically
		// based on the err, do not have to quit in checkError
		checkErrorAndConnect(i, err)
	}

	// this can also reside in the micro.go
	go func() {
		for {
			conn, _ := l.Accept()
			go rpc.ServeConn(conn)
		}
	}()

}

// If error is non-nil, print it out and halt.
// This function is augmented with a RPC call to connect, the remote peer
// will be requested to dial to this client as well, if no err.
func checkErrorAndConnect(index int, err error) {
	if err != nil {
		fmt.Println("Error", err.Error())
	} else { // if no error, RPC connect
		// now send to peers
		ConnectArgs := ConnectArgs{
			Clientid: localClient, // TODO
		}
		var kvVal ValReply
		// asynchronously, does not matter if synchronous.
		go func() {
			err := peerServices[index-1].Call("EntangleClient.Connect", ConnectArgs, &kvVal)
			// let's follow the original protocol
			// initiating pair-wise sync protocol here
			if err == nil {
				pairWiseSync(peerAddresses[index].IP_PORT, index-1)
			} else {
				fmt.Println("Error", err.Error())
			}
		}()

	}
}

// check whether local peer is not connected to any peers
// Returns true if offline
func IsOffline() bool {

	for _, e := range peerServices {
		if e != nil {
			return false
		}
	}

	return true
}

// Given a IP_PORT string, find the index of peerServices
func GetPeerServicesIndex(IP_PORT string) (uint8, bool) {

	for i, e := range peerAddresses {
		if e.IP_PORT == IP_PORT {
			return uint8(i - 1), true
		}
	}

	return 0, false
}

// The pair-wise synchronization protocol here
func pairWiseSync(peer string, serviceIndex int) {
	// Phase one: requester sending <local clock, peer clock>
	// type SyncPhaseOneArgs struct {
	// 	Clientid      string // requester
	// 	SenderClock   uint64 // sender clock
	// 	ReceiverClock uint64 // sender view of receiver clock
	// }
	SyncPhaseOneArgs := SyncPhaseOneArgs{
		Clientid:      localClient,
		SenderClock:   seqVector[localClient].Clock,
		ReceiverClock: seqVector[peer].Clock,
	}
	var reply SyncPhaseOneReply
	peerServices[serviceIndex].Call("EntangleClient.SyncPhaseOne", SyncPhaseOneArgs, &reply)

	// get some results back from the receiver
	// type SyncPhaseOneReply struct {
	// 	PhaseTwo       bool        // set to true, if second phase is required
	// 	RequesterClock uint64      // receiver's view of requester's clock
	// 	Patch          []Operation // operations
	// }
	// the patch should already sorted in increasing clock values
	if len(reply.Patch) > 0 {
		insertPatch(reply.Patch)

		// update seqVector based on the last operation from the patch.
		// assuming patch contains in increasing clock values.
		seqVector[peer].Clock = reply.Patch[len(reply.Patch)-1].Clock
		seqVector[peer].Dirty = true
		RedrawAll() // TODO: highlighting
		//RedrawAllWithPatchHighlight(reply.Patch)
	}

	if reply.PhaseTwo == false {
		// not need to do phase two
		return
	} else {
		// using RequesterClock to determine the patch to be sent over
		// type SyncPhaseOneReply struct {
		// 	PhaseTwo       bool        // set to true, if second phase is required
		// 	RequesterClock uint64      // receiver's view of requester's clock
		// 	Patch          []Operation // operations
		// }
		//SyncPhaseOneArgs
		// type SyncPhaseTwoArgs struct {
		// 	Clientid string // sender
		// 	Patch []Operation // operations
		// }

		// this will need to ask from storage, but we can have a buffered operations for efficiency
		// Currently, we assume every local operation is immediately write-back
		patch := ExtractOperationsBetween(reply.RequesterClock+1, seqVector[localClient].Clock) // notice the plus one

		SyncPhaseTwoArgs := SyncPhaseTwoArgs{
			Clientid: localClient,
			Patch:    patch,
		}

		var reply ValReply
		peerServices[serviceIndex].Call("EntangleClient.SyncPhaseTwo", SyncPhaseTwoArgs, &reply)

	}

}

// Insert a patch to the local document and storage
func insertPatch(patch []Operation) {

	if patch != nil {
		// going to insert this patch to the document
		// EntangleClient.insert()
		// buffer pointer, supports one tab currently
		buf := CurView().Buf
		for _, op := range patch { // TODO: refactor
			if op.OpType == true { // insert operation
				// the CRDTIndex is the index for the atom to be inserted in the document
				posIdentifier := NewPos(op.Pos)
				CRDTIndex, exists := buf.Document.Index(posIdentifier)
				if exists == true { // if exists, don't insert
					continue
				}
				// converting CRDTIndex to lineArray pos
				LinePos := FromCharPos(CRDTIndex-1, buf) // off by 1
				// This directly insert to document and lineArray directly bypassing the eventsQueue
				// Let's insert to lineArray first
				buf.LineArray.insert(LinePos, []byte(op.Atom))
				// now insert to document
				dbID := NextDocID()
				buf.Document.insert(posIdentifier, op.Atom, dbID)
				// update numoflines in lineArray
				buf.Update()

				// arguments must passed in but may still has race condition
				go func(id uint64, atom string, pos []Identifier) {
					err := InsertCharToDocDB(id, atom, pos)
					if err != nil {
						fmt.Println("Error", err.Error())
					}
				}(dbID, op.Atom, posIdentifier)

			} else { // delete operation
				// the CRDTIndex is the index for the atom to be deleted in the document
				posIdentifier := NewPos(op.Pos)
				CRDTIndex, exists := buf.Document.Index(posIdentifier)
				if exists == false { // don't delete something not exited
					continue
				}
				// converting CRDTIndex to lineArray pos
				LinePos := FromCharPos(CRDTIndex-1, buf) // CRDT_index is one index higher
				// This directly delet to document and lineArray directly bypassing the eventsQueue
				buf.LineArray.remove(LinePos, LinePos.right(buf)) // removing one char at LinePos

				// given position identifier, delete directly
				_, dbID := buf.Document.delete(posIdentifier)
				// update numoflines in lineArray
				buf.Update()

				// still unsafe concurrently
				go func(id uint64) { // do this in a separate go routine, note it is set to false
					err := DeleteCharFromDocDB(id)
					if err != nil {
						fmt.Println("Error", err.Error())
					}
				}(dbID)

			}
		}

	}

}
