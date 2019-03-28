package main

// This is the connection code with other peers for now.
import (
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
	Clientid uint8
}

// args in put(args)
type DeleteArgs struct {
	Pair     pair   // pair as defined in document.go
	Clock    uint64 // value of logical clock at the issuing client
	Clientid uint8
}

// args in disconnect(args)
type DisconnectArgs struct {
	Clientid uint8 // client id who voluntarilly quit the editor
}

// Reply from service for all the API calls above.
// This is useful for ensuring delivery success
type ValReply struct {
	Val string // value; depends on the call
}

type EntangleClient int

// Command line arg. Can be based on a config file
var numPeers uint8

//a slice holding peer ip addresses
var peerAddresses []string

// a slice hoding rpc service of peers
var peerServices []*rpc.Client

// a insert char message from a peer.
func (ec *EntangleClient) Insert(args *InsertArgs, reply *ValReply) error {
	// decompose InsertArgs
	posIdentifier := args.Pair.Pos
	atom := []byte(args.Pair.Atom)
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

	//testing
	//fmt.Printf(args.Pair.Atom)
	//fmt.Println("remote insert")
	// update numoflines in lineArray
	buf.Update()

	CurView().Display() // update current tab
	screen.Show()       // commit

	return nil
}

// a delete char message from a peer
func (ec *EntangleClient) Delete(args *DeleteArgs, reply *ValReply) error {
	// TODO

	return nil
}

// DISCONNECT from a peer.
func (ec *EntangleClient) Disconnect(args *DisconnectArgs, reply *ValReply) error {
	// TODO

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

	ip_port := args[0] // local ip

	numPeers = 2 // including itself

	// Setup and register service.
	entangleClient := new(EntangleClient)
	rpc.Register(entangleClient)

	// listen first
	l, err := net.Listen("tcp", ip_port)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	// then dial
	peerAddresses = make([]string, 1)
	peerServices = make([]*rpc.Client, 1) // must use "="" to assign global variables
	for i := range peerAddresses {
		peerAddresses[i] = args[i+1]
		// Connect to other peers via RPC.
		peerServices[i], err = rpc.Dial("tcp", peerAddresses[i]) // can dial periodically
		// based on the err, do not have to quit in checkError
		checkError(err)
	}

	// some testing code below
	// var kvVal ValReply
	// InsertArgs := InsertArgs{
	// 	Clientid: 1,
	// 	Clock:    123,
	// 	Pair: pair{
	// 		Pos: []Identifier{{13627, 1},
	// 			{65036, 1},
	// 			{24224, 1}},
	// 		Atom: "$",
	// 	},
	// }

	// ticker := time.NewTicker(10 * time.Second) // send in 10s
	// quit := make(chan bool)
	// go func() {
	// 	for {
	// 		select {
	// 		case <-ticker.C:
	// 			val := peerServices[0].Call("EntangleClient.Insert", InsertArgs, &kvVal)
	// 			checkError(val)
	// 			quit <- true // only send the above once and quit
	// 		case <-quit:
	// 			ticker.Stop()
	// 			close(quit)
	// 			return
	// 		}
	// 	}
	// }()

	// this can also reside in the micro.go
	go func() {
		for {
			conn, _ := l.Accept()
			go rpc.ServeConn(conn)
		}
	}()

}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		//fmt.Fprintf(os.Stderr, "MyError: ", err.Error())
		fmt.Println("Error", err.Error())
		os.Exit(1) // Let's do not exit, when in production
	}
}
