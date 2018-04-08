/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);
	updateMemberListEntry(id,port,memberNode->heartbeat);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	memberNode->memberList.clear();
	return SUCCESS;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	 if (size < (int)sizeof(MessageHdr)) {
#ifdef DEBUGLOG
			log->LOG(&((Member*)env)->addr, "Message received with size less than MessageHdr. Ignored.");
#endif
		return false;
	 }

	 MessageHdr* msg = (MessageHdr*)data;
	 #ifdef DEBUGLOG
	 	//log->LOG(&((Member *)env)->addr, "Received message type %d with %d B payload", msg->msgType, size - sizeof(MessageHdr));
		log->LOG(&((Member *)env)->addr, "Received message type %d", msg->msgType);
	 #endif

	 char *packet = data + sizeof(MessageHdr);
	 int packetSize = size - sizeof(MessageHdr);
	 switch (msg->msgType) {
	 	case JOINREQ: {
			return processJoinReq(env, packet, packetSize);
		}
		case JOINREP: {
			return processJoinRep(env, packet, packetSize);
		}
		case UPDATEREQ: {
			return processUpdateReq(env, packet, packetSize);
		}
		case UPDATEREP: {
			return processUpdateRep(env, packet, packetSize);
		}
		default:{
			return false;
		}
	 }
	 return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	// first delete timeout nodes
	delTimeoutNodes();

	// increment heartbeat
	memberNode->heartbeat++;
	int id;
	short port;
	memcpy(&id, &(memberNode->addr.addr[0]), sizeof(int));
	memcpy(&port, &(memberNode->addr.addr[4]), sizeof(short));
	for (auto& elm: memberNode->memberList) {
		if ((id == elm.id) && (port == elm.port)) {
			elm.setheartbeat(memberNode->heartbeat);
			break;
		}
	}

	if (0 == --(memberNode->pingCounter)) {
		for (auto& elm: memberNode->memberList) {
			Address addr;
			memcpy(&addr.addr[0], &elm.id, sizeof(int));
			memcpy(&addr.addr[4], &elm.port, sizeof(short));
			if (memcmp(addr.addr, memberNode->addr.addr, sizeof(addr.addr)) != 0) {
				sendMemberList(UPDATEREP, &addr);
			}
		}
		memberNode->pingCounter = TFAIL;
	}
  return;
}

/**
 * FUNCTION NAME: processJoinReq
 *
 * DESCRIPTION: process JOINREQ requests by introducer.
 *
 */
bool MP1Node::processJoinReq(void *env, char *data, int size)
{
	if (size < (int)(sizeof(memberNode->addr.addr) + sizeof(long) + 1)) {
  	#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Message JOINREQ received with wrong size. Ingored");
		#endif
		return false;
	}

	Address addr;
	long heartbeat;
	memcpy(&addr, data, sizeof(Address));
	memcpy(&heartbeat, data + 1 + sizeof(Address), sizeof(long));
	#ifdef DEBUGLOG
			//log->LOG(&memberNode->addr, "Received message JOINREQ from %s",
		  //         addr.getAddress().c_str());
	#endif
	sendMemberList(JOINREP, &addr);

	int id = *(int *)(&addr.addr[0]);
	short port = *(short *)(&addr.addr[4]);
	updateMemberListEntry(id, port, heartbeat);

	return true;
}

/**
 * FUNCTION NAME: processJoinRep
 *
 * DESCRIPTION: process JOINREP response
 *
 */
bool MP1Node::processJoinRep(void *env, char *data, int size)
{
	memberNode->inGroup = true;

	// get address
	Address addr;
	memcpy(addr.addr, data, sizeof(memberNode->addr.addr));
	data += sizeof(memberNode->addr.addr);
	size -= sizeof(memberNode->addr.addr);
#ifdef DEBUGLOG
	//log->LOG(&memberNode->addr, "Received message JOINREP from %s",
  //         addr.getAddress().c_str());
#endif

  // get memberList number
	if (recvMemberList(env,data,size)) {
		return true;
	}

	return false;
}

/**
 * FUNCTION NAME: processUpdateRep
 *
 * DESCRIPTION: process UPDATEREP requests
 *
 */
bool MP1Node::processUpdateRep(void *env, char *data, int size)
{
	if (size < (int)(sizeof(memberNode->addr.addr))) {
#ifdef DEBUGLOG
	log->LOG(&memberNode->addr, "Message UPDATEREP received with wrong size. Ingored.");
#endif
		return false;
	}

	// get address
	Address addr;
	memcpy(addr.addr, data, sizeof(memberNode->addr.addr));
	data += sizeof(memberNode->addr.addr);
	size -= sizeof(memberNode->addr.addr);
	#ifdef DEBUGLOG
		//log->LOG(&memberNode->addr, "Received message UPDATEREP from %s",
	  //         addr.getAddress().c_str());
	#endif

  // get memberList number
	if (recvMemberList(env,data,size)) {
		return true;
	}

	return false;
	//int id;
	//short port;
	//memcpy(&id, &addr.addr[0], sizeof(int));
	//memcpy(&port, &addr.addr[4], sizeof(short));

	//for (auto& elm: memberNode->memberList) {
	//	if ((id == elm.id) && (port == elm.port)) {
	//		elm.heartbeat ++;
	//		elm.timestamp = par->getcurrtime();
	//#ifdef DEBUGLOG
	//				log->LOG(&memberNode->addr, "Heartbeat: %i timestamp: %i",
	//				         elm.heartbeat, elm.timestamp);
	//#endif
	//		return true;
	//	}
	//}

	//#ifdef DEBUGLOG
	//		log->LOG(&memberNode->addr, "Message UPDATEREP not found in memberList.");
	//#endif
	//	return false;
}

/**
 * FUNCTION NAME: processUpdateReq
 *
 * DESCRIPTION: process UPDATEREQ requests
 *
 */
bool MP1Node::processUpdateReq(void *env, char *data, int size)
{
	if (size < (int)(sizeof(memberNode->addr.addr))) {
#ifdef DEBUGLOG
	log->LOG(&memberNode->addr, "Message UPDATEREQ received with wrong size. Ingored.");
#endif
		return false;
	}

	// get address
	Address addr;
	memcpy(addr.addr, data, sizeof(memberNode->addr.addr));
	data += sizeof(memberNode->addr.addr);
	size -= sizeof(memberNode->addr.addr);
#ifdef DEBUGLOG
	log->LOG(&memberNode->addr, "Received message UPDATEREQ from %s",
           addr.getAddress().c_str());
#endif

  // get memberList number
	if (!recvMemberList(env,data,size)) {
		return false;
	}

	size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
	MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
	msg->msgType = UPDATEREP;
	memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
	free(msg);

	return true;
}

	/**
	 * FUNCTION NAME: updateMemberListEntry
	 *
	 * DESCRIPTION: if the MemberListEntry has already be in memberList, update its
	 *              heartbeat and timestamp, else add into memberList
	 *
	 */
void MP1Node::updateMemberListEntry(int id, short port, long heartbeat, bool update)
{
	// MemberListEntry(id,port) in memberList, update heartbeat and timestamp
	for (auto& elm: memberNode->memberList) {
		if ((elm.getid() == id) && (elm.getport() == port)) {
			if (update) {
				if (elm.getheartbeat() < heartbeat) {
					elm.setheartbeat(heartbeat);
					elm.settimestamp(par->getcurrtime());
				}
			}
			return;
		}
	}

	// MemberListEntry(id,port) not in memberList, add into memberList
	MemberListEntry entry(id,port,heartbeat,par->getcurrtime());
	memberNode->memberList.push_back(entry);
	memberNode->nnb++;
	if (memberNode->nnb > par->MAX_NNB) {
		#ifdef DEBUGLOG
			log->LOG(&memberNode->addr, "Exceeds membership capacity.");
		#endif
	}

	Address joinAddr;
	memcpy(&joinAddr.addr[0], &id,   sizeof(int));
	memcpy(&joinAddr.addr[4], &port, sizeof(short));
	log->logNodeAdd(&memberNode->addr, &joinAddr);
}

	/**
	 * FUNCTION NAME: sendMemberList
	 *
	 * DESCRIPTION: send all entries except:
	 *                a. entry with TREMOVE, which will be deleted;
	 *                b. entry with TFAIL
	 *
	 */
void MP1Node::sendMemberList(MsgTypes msgType, Address *to)
{
	long members = memberNode->memberList.size();
	size_t msgsize = sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+sizeof(long)
	                 + members*(sizeof(int)+sizeof(short)+sizeof(long));
	// MessageHdr
	MessageHdr* msg = (MessageHdr *)malloc(msgsize * sizeof(char));
	msg->msgType = msgType;
  // addr
	char* data = (char*)(msg + 1);
	memcpy(data, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	data += sizeof(memberNode->addr.addr);
	// members number
	//char* membersPos = data;
	memcpy(data, &members, sizeof(long));
	data += sizeof(long);
  // memberList entries
  auto it = memberNode->memberList.begin();
	while (it != memberNode->memberList.end()) {
		// copy entry
		memcpy(data, &(it->id), sizeof(int));
		data += sizeof(int);
		memcpy(data, &(it->port), sizeof(short));
		data += sizeof(short);
		memcpy(data, &(it->heartbeat), sizeof(long));
		data += sizeof(long);

		it++;
	}

	//memcpy(membersPos, &members, sizeof(long));
	//msgsize = sizeof(MessageHdr)+sizeof(memberNode->addr.addr)+sizeof(long)
	//                 + members*(sizeof(int)+sizeof(short)+sizeof(long));
	emulNet->ENsend(&memberNode->addr, to, (char *)msg, msgsize);

	free(msg);
}

/**
 * FUNCTION NAME: recvMemberList
 *
 * DESCRIPTION: receive member list
 *
 */
bool MP1Node::recvMemberList(void *env, char *data, int size)
{
	// get memberList number
  long members;
	memcpy(&members, data, sizeof(long));
	data += sizeof(long);
	size -= sizeof(long);

	if (size < (int)(members * (sizeof(int) + sizeof(short) + sizeof(long)))) {
#ifdef DEBUGLOG
	log->LOG(&memberNode->addr,"Message received with wrong size. Ingored.");
#endif
		return false;
	}

	// get entry
	for (long i=0; i<members; i++) {
		// get id
		int id;
		memcpy(&id, data, sizeof(int));
		data += sizeof(int);
		// get port
		short port;
		memcpy(&port, data, sizeof(short));
		data += sizeof(short);
		// get heartbeat
		long heartbeat;
		memcpy(&heartbeat, data, sizeof(long));
		data += sizeof(long);
		// update memberList entry
		updateMemberListEntry(id, port, heartbeat, true);
	}
	return true;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
void MP1Node::delTimeoutNodes()
{
	if (memberNode->memberList.empty()) {
		return ;
	}

	int id = *(int *)(&memberNode->addr.addr);
	auto it = memberNode->memberList.begin();
	while (it != memberNode->memberList.end()) {
		if ((id != it->id) && (par->getcurrtime() - it->gettimestamp()) > TREMOVE) {
			Address delAddr;
			memcpy(&delAddr.addr[0], &(it->id), sizeof(int));
			memcpy(&delAddr.addr[4], &(it->port), sizeof(int));
			log->logNodeRemove(&memberNode->addr, &delAddr);
			memberNode->nnb--;

			it = memberNode->memberList.erase(it);
			continue;
		}
		it++;
	}
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
