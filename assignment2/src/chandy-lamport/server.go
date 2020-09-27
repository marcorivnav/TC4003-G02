package chandy_lamport

import (
	"log"
	"strconv"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	snapshots     map[int]*SnapshotState
	storeMessages map[string]bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotState),
		make(map[string]bool),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME

	// Check the message type
	switch message := message.(type) {
	case MarkerMessage:
		snapshotID := message.snapshotId

		// Check if this server already has a snapshot for the received snapshotID
		_, snapshotExists := server.snapshots[snapshotID]

		if !snapshotExists {
			// Perform the snapshot
			server.performSnapshot(snapshotID)
		}

		// Close the saving of messages for that snapshot (BUT only for that channel)
		storeMessagesKey := strconv.Itoa(snapshotID) + "-" + src
		server.storeMessages[storeMessagesKey] = false

	case TokenMessage:
		numTokens := message.numTokens

		// Add the tokens to the server
		server.Tokens += numTokens

		// If the server already has snapshots
		for _, snapshot := range server.snapshots {

			// Add the messages to each one of the snapshots that are open (Those that are not in the storeMessages map)
			storeMessagesKey := strconv.Itoa(snapshot.id) + "-" + src
			if _, ok := server.storeMessages[storeMessagesKey]; !ok {
				snapshotMessage := SnapshotMessage{src, server.Id, message}
				snapshot.messages = append(snapshot.messages, &snapshotMessage)
			}
		}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	server.performSnapshot(snapshotId)
}

/* performSnapshot creates an entry of SnapshotState and stores it in the server snapshots map. Also sends a MarkerMessage to the
   server neighbors and notifies the Simulator that the snapshot in the server is complete */
func (server *Server) performSnapshot(snapshotID int) {
	// Build the tokens map (Even if it will only have this server tokens)
	serverTokens := make(map[string]int)
	serverTokens[server.Id] = server.Tokens

	// Build the messages array
	serverMessages := make([]*SnapshotMessage, 0)

	// Store the own snapshot in the server snapshots map
	server.snapshots[snapshotID] = &SnapshotState{snapshotID, serverTokens, serverMessages}

	// Send a marker message to all the server channels asking to perform their snapshots
	server.SendToNeighbors(MarkerMessage{snapshotID})

	// Notify the simulator
	server.sim.NotifySnapshotComplete(server.Id, snapshotID)
}
