import xmlrpc
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from threading import Timer,Thread,Event
import hashlib
import datetime
import threading
import time
import sched
import random
import argparse
import asyncio
import traceback

block_list = {}
file_info_map = {}
lock = threading.Lock()
lock_heartbeat = threading.Lock()

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"
HEARTBEAT_INTERVAL = 0.1
CRASH_ERROR = -1
rpc_locks = []
LOCK_TIMEOUT = 2

server_state = None


class ServerState:
    current_term = 0
    #TODO reset when heartbeat is received
    voted_for = None
    log = []
    state = FOLLOWER
    self_id = 0
    num_servers = 0
    election_event = None
    config_map = {}
    election_scheduler = None
    election_scheduler = None
    election_timeout = 0
    election_timeout = 0
    leader_id = None
    vote_count = 0
    rpc_clients = None
    voted_term = 0
    is_crashed = 0

    commit_index = 0
    last_applied = 0
    next_index = []
    match_index = []

    append_entries_success_count = 0

    start_election_flag = 1


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

# A simple ping, returns true
def ping():
    """A simple ping method"""
    ###print("Ping()")
    return True


# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    ####print("GetBlock(" + h + ")")
    if h in block_list:
        return block_list[h]
    return False


# Puts a block
def putblock(b):
    """Puts a block"""
    ####print("PutBlock() at ", datetime.datetime.now())
    hash_value = hashlib.sha256(b.data).hexdigest()
    if hash_value not in block_list:
        block_list[hash_value] = b
    return True


# Given a list of blocks, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    ####print("HasBlocks()")
    existing_hashes = []
    for hash_val in hashlist:
        if hash_val in block_list:
            existing_hashes.append(hash_val)
    return existing_hashes


# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    global server_state
    ###print("GetFileInfoMap()")

    if not isLeader():
        ###print("Not Leader -  cannot reply to getfileinfomap")
        raise Exception("Not leader")

    if server_state.is_crashed:
        return Exception("Is Crashed")

    #Check that majority of the nodes are up and block if not
    count = 0
    while True:
        for i in server_state.rpc_clients:
            if not i.surfstore.isCrashed():
                count += 1
        if count >= (int(server_state.num_servers/2) + 1):
            break
        count = 0

    return file_info_map



# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    global server_state

    ###print("UpdateFile() for ", filename, " at: ", datetime.datetime.now())

    if not isLeader():
        raise Exception("Not leader")

    if server_state.is_crashed:
        return Exception("Is Crashed")
    
    # #Check version
    # if (filename in file_info_map) and version != (file_info_map[filename][0]+1):
    #     return False

    # Add command to local log
    prev_log_index = 0
    prev_log_term = 0
    log_len = 0
    if lock.acquire():
        ###print("Appending entry in self: ", [server_state.current_term, filename, version, hashlist])
        prev_log_index = 0 if len(server_state.log) == 0 else len(server_state.log)
        prev_log_term = 0 if len(server_state.log) == 0 else server_state.log[prev_log_index-1][0]
        server_state.log.append([server_state.current_term, filename, version, hashlist])
        log_len = len(server_state.log)
        server_state.append_entries_success_count = 1
        lock.release()

        send_append_entries(prev_log_index, prev_log_term, log_len)
        while True:
            #print("AppendEntries success count", server_state.append_entries_success_count)
            if server_state.append_entries_success_count >= (int(server_state.num_servers/2) + 1):
                break
            
        #TODO Add locking
        lock.acquire()
        server_state.commit_index = log_len
        server_state.append_entries_success_count = 0
        lock.release()
        return True

    return False

# PROJECT 3 APIs below

def send_append_entry(id, prev_log_index, prev_log_term, log_len, commit_index, is_hb):
    global server_state
    
    if is_hb:
        try:
            #print("Sending heartbeat to id: ", id)
            log_entries_to_send = server_state.log[server_state.next_index[id]-1:log_len]
            rpc_locks[id].acquire()
            response = server_state.rpc_clients[id].surfstore.appendEntries(server_state.current_term, server_state.self_id, prev_log_index, prev_log_term, [], commit_index)
            rpc_locks[id].release()
            if not response[0]:
                if response[1] > server_state.current_term:
                    if lock.acquire(timeout=LOCK_TIMEOUT):
                        server_state.state = FOLLOWER
                        reset_term(response[1])
                        server_state.start_election_flag = 1
                        lock.release()


        except Exception as e:
            #print(e)
            if rpc_locks[id].locked():
                rpc_locks[id].release()

    else:
        while True:
            try:
                ##print("sending logs to id ", id, "from next index ", server_state.next_index[id])
                log_entries_to_send = server_state.log[server_state.next_index[id]-1:log_len]
                print("Sending append entries []", log_entries_to_send, " to id: ", id)
                rpc_locks[id].acquire()
                response = server_state.rpc_clients[id].surfstore.appendEntries(server_state.current_term, server_state.self_id, prev_log_index, prev_log_term, log_entries_to_send, commit_index)
                print("Response for appendEntries from : ", id, " ", response)
                rpc_locks[id].release()
                if response[0]:
                    if lock.acquire(timeout = LOCK_TIMEOUT):
                        #print("updating success count")
                        server_state.append_entries_success_count += 1
                        server_state.next_index[id] = prev_log_index + len(log_entries_to_send) + 1
                        server_state.match_index[id] = prev_log_index + len(log_entries_to_send)
                        lock.release()
                        break
                    else:
                        ###print("Could not acquire lock to update append entries response for id: ", id, " response: ", response)
                        continue
                elif response[1] == CRASH_ERROR:
                    print("got crash response from id: ", id)
                    continue
                elif response[1] > server_state.current_term:
                    if lock.acquire(timeout=LOCK_TIMEOUT):
                        server_state.state = FOLLOWER
                        reset_term(response[1])
                        server_state.start_election_flag = 1
                        lock.release()
                else:
                    #If append entries was false, decrement nextIndex and try again
                    ###print("prev_log_index_for_id: ", prev_log_index_for_id)
                    server_state.next_index[id] -= 1
                    prev_log_index_for_id -= 1
                    prev_log_term_for_id = server_state.log[prev_log_index_for_id][0]
            except Exception as e:
                #traceback.print_exc()
                #print(e)
                if rpc_locks[id].locked():
                    rpc_locks[id].release()
                break

async def await_append_entry_func(prev_log_index, prev_log_term, log_len, commit_index):
    await asyncio.wait([send_append_entry(i, prev_log_index, prev_log_term, log_len, commit_index) for i in range(server_state.num_servers) if i != server_state.self_id])


def send_append_entries(prev_log_index, prev_log_term, log_len):
    # loop = asyncio.new_event_loop()
    # loop.run_until_complete(await_append_entry_func(prev_log_index, prev_log_term, log_len, server_state.commit_index))
    for i in range(server_state.num_servers):
        if i != server_state.self_id:
            threading.Thread(target=send_append_entry, args=(i, prev_log_index, prev_log_term, log_len, server_state.commit_index, 0)).start()


def appendEntries(leaderTerm, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
    global server_state
    global raftTimer
    server_state.start_election_flag = 0
    try:
        # if entries:
        #     print("AppendEntries from :", leaderId, " at term: ", leaderTerm, " ", datetime.datetime.now(), " Self term ", server_state.current_term, " entries ", entries)
        if server_state.is_crashed:
            return False, CRASH_ERROR
        if leaderTerm < server_state.current_term:
            return False, server_state.current_term

        ###print("Adding new entries to log: ", entries)

        if lock.acquire(timeout=LOCK_TIMEOUT):
            server_state.current_term  = leaderTerm
            server_state.state = FOLLOWER
            server_state.vote_count = -1
            ###print("Acquired lock to store entries: ", entries)
            
            if len(entries) > 0:
                ###print("Adding new entries to log acquired lock: ", entries)
                #Check that prevLogIndex and prevLogTerm match
                if prevLogIndex > 0 and (len(server_state.log) >= prevLogIndex and server_state.log[prevLogIndex-1][0] != prevLogTerm):
                    print (prevLogIndex, len(server_state.log), prevLogTerm, server_state.log) #2 1 1 [[1, 'd', 1, [123]]]
                    return False, server_state.current_term
                #If there exist some conflicting entries, delete them
                elif len(server_state.log) > prevLogIndex:
                    del server_state.log[prevLogIndex+1:]
                #Add new entries
                #print("Adding entries to log: ", entries)
                server_state.log.extend(entries)
            ##print("FOLLOWER CI is", server_state.commit_index, " but got CI  ", leaderCommit)

            if leaderCommit > server_state.commit_index:
                server_state.commit_index = min(leaderCommit, len(server_state.log))
                ##print("FOLLOWER updating commit index to ", server_state.commit_index, " applied index is ", server_state.last_applied)
            
            lock.release()
            #print("Replying true, at term ", server_state.current_term)
            return True, server_state.current_term
        else: 
            return False, server_state.current_term
    except Exception as e:
        #traceback.print_exc()
        print(e)


def applyCommits():
    global server_state
    while True:
        if not server_state.is_crashed:
            ##print("applyCommits() CI: ", server_state.commit_index, " LA: ", server_state.last_applied)
            ##print(server_state.log)
            while server_state.commit_index > server_state.last_applied:
                commited = server_state.commit_index
                term, name, version, hashlist = server_state.log[server_state.last_applied]
                ###print("Applying index: ", server_state.last_applied, " upto ", server_state.commit_index, " new version ", version)

                if lock.acquire():
                    server_state.last_applied += 1
                    file_info_map[name] = [version, hashlist]
                    lock.release()
            # To update commitIndex of leader if leader crashed and restored in same term
            if server_state.state == LEADER:
                for N in range(server_state.commit_index, len(server_state.log)):
                    count = 0
                    flag = False
                    for j in range(server_state.num_servers):
                        if server_state.match_index[j] >= N:
                            count += 1
                    if count >= (int(server_state.num_servers/2) + 1) and server_state.log[N][0] == server_state.current_term:
                        lock.acquire()
                        server_state.commit_index = N
                        lock.release()
                        flag = True
                    if flag:
                        break
        time.sleep(0.5)


def requestVote(candidateId, candidateTerm, lastLogIndex, lastLogTerm):
    ##print("requestVote() from ", candidateId, " in term ", candidateTerm , " with last log index ", lastLogIndex, " and last log term ", lastLogTerm)
    global server_state
    if server_state.is_crashed:
        return False, CRASH_ERROR
    try:
        if candidateTerm > server_state.current_term:

            if (server_state.voted_term < candidateTerm)  or (server_state.voted_term == candidateTerm and server_state.voted_for is None):
                if not server_state.log:
                    return set_voted_for(candidateId, candidateTerm), server_state.current_term
                elif lastLogTerm > server_state.log[-1][0]:
                    return set_voted_for(candidateId, candidateTerm), server_state.current_term
                elif lastLogTerm == server_state.log[-1][0] and lastLogIndex >= len(server_state.log):
                    return set_voted_for(candidateId, candidateTerm), server_state.current_term
    except Exception as e:
        ###print("Exception while granting vote in term: ", server_state.current_term, e)
        pass
    return False, server_state.current_term



#@asyncio.coroutine
def request_vote(id):
    global server_state
    global raftTimer

    if not server_state.log:
        last_log_index = 0
        last_log_term = 0
    else:
        last_log_index = len(server_state.log)
        last_log_term = server_state.log[last_log_index-1][0]
    #print("requesting vote from ", id, server_state.current_term)

    ####print("Acquired lock for requestVote")
    try:
        rpc_locks[id].acquire()
        response = server_state.rpc_clients[id].surfstore.requestVote(server_state.self_id, server_state.current_term, last_log_index, last_log_term)
        rpc_locks[id].release()
        
        if lock.acquire(timeout=LOCK_TIMEOUT):
            if response[0]: # (boolean, term)
                if server_state.vote_count != -1:
                    server_state.vote_count += 1
                    #print("Got vote from :", id, " term ", server_state.current_term)
                if server_state.vote_count >= (int(server_state.num_servers/2) + 1):
                    server_state.state = LEADER;
                    server_state.vote_count = -1
                    #print("I AM THE LEADER AT TERM:", server_state.current_term)
                    server_state.next_index = [len(server_state.log) + 1] * server_state.num_servers
                    server_state.match_index = [0] * server_state.num_servers
                    server_state.start_election_flag = 0

            else:
                if server_state.current_term < response[1]:
                    server_state.current_term = response[1]
                    server_state.state = FOLLOWER
                    server_state.start_election_flag = 1
            lock.release()
    except Exception as e:
        if rpc_locks[id].locked():
            rpc_locks[id].release()
        ####print("Could not send request for vote to id: ", id, " : ", e )
    ####print("Released lock for requestVote")

async def await_request_vote_func():
    await asyncio.wait([request_vote(i) for i in range(server_state.num_servers) if i != server_state.self_id])



def start_election():
    global server_state
    #print("STATE: ", server_state.state, " TERM: ", server_state.current_term, " ", datetime.datetime.now())
    while True: 
        time.sleep(server_state.election_timeout)
        ###print("flag ", server_state.start_election_flag, " State: ", server_state.state)
        if server_state.start_election_flag and not server_state.is_crashed and server_state.state != LEADER:
            if lock.acquire(timeout=LOCK_TIMEOUT):
                #print("Acquired lock for starting eleciton")
                server_state.state = CANDIDATE
                server_state.current_term += 1
                server_state.voted_for = server_state.self_id
                server_state.voted_term = server_state.current_term
                server_state.vote_count = 1
                lock.release()
                # loop = asyncio.new_event_loop()
                # loop.run_until_complete(await_request_vote_func())
                for i in range(server_state.num_servers):
                    if i != server_state.self_id:
                        threading.Thread(target=request_vote, args=[i]).start()
        server_state.start_election_flag = 1

#async
async def send_heartbeat(id):
    global raftTimer
    global server_state
    #lock_heartbeat.acquire()
    if server_state.state == LEADER:
        lock.acquire()
        prev_log_index = 0 if len(server_state.log) == 0 else len(server_state.log)
        prev_log_term = 0 if len(server_state.log) == 0 else server_state.log[prev_log_index-1][0]
        commit_index = server_state.commit_index
        lock.release()
        #print("Sending heartbeat to: ", id, " at term: ", server_state.current_term)
        try:
            rpc_locks[id].acquire()
            response = server_state.rpc_clients[id].surfstore.appendEntries(server_state.current_term, server_state.self_id, prev_log_index, prev_log_term, [], commit_index)
            rpc_locks[id].release()
            if not response[0]:
                if response[1] > server_state.current_term:
                    if lock.acquire(timeout=LOCK_TIMEOUT):
                        server_state.state = FOLLOWER
                        reset_term(response[1])
                        server_state.start_election_flag = 1
                        lock.release()
            #print("Heartbeat response from ", id, " ", response)
        except Exception as e:
            pass
            ####print("Could not send heartbeat to", id, " : ", e)
    #lock_heartbeat.release()


async def await_heartbeat_func(prev_log_index, prev_log_term, log_len, commit_index):
    await asyncio.wait([send_append_entry(i, prev_log_index, prev_log_term, log_len, commit_index) for i in range(server_state.num_servers) if i != server_state.self_id])

def send_heartbeats():
    global server_state
    while True:
        if server_state.state == LEADER and not server_state.is_crashed:
            lock.acquire()
            prev_log_index = 0 if len(server_state.log) == 0 else len(server_state.log)
            prev_log_term = 0 if len(server_state.log) == 0 else server_state.log[prev_log_index-1][0]
            log_len = len(server_state.log)
            commit_index = server_state.commit_index
            lock.release()
            #print("Sending heartbeats")
            # loop = asyncio.new_event_loop()
            # loop.run_until_complete(await_heartbeat_func(prev_log_index, prev_log_term, log_len, commit_index))
            for i in range(server_state.num_servers):
                if i != server_state.self_id:
                    threading.Thread(target=send_append_entry, args=(i, prev_log_index, prev_log_term, log_len, commit_index, 1)).start()
        time.sleep(HEARTBEAT_INTERVAL)


def reset_term(term):
    global server_state
    server_state.current_term = term 


def set_voted_for(id, term):
    global server_state
    if lock.acquire(timeout=LOCK_TIMEOUT):
        server_state.voted_for = id
        server_state.voted_term = term
        reset_term(term)
        lock.release()
        return True
    return False
    ####print("Voted for: ", id, " at term ", term)

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    global server_state
    ###print("IsLeader()", server_state.state)
    return (server_state.state == LEADER and not server_state.is_crashed)

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    print("Crash()")
    global server_state
    if lock.acquire(timeout=LOCK_TIMEOUT):
        server_state.is_crashed = 1
        lock.release()
        return True
    return False

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    global server_state
    if lock.acquire(timeout=LOCK_TIMEOUT):
        server_state.is_crashed = 0
        lock.release()
        return True
    return False

# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    ###print("IsCrashed()")
    return bool(server_state.is_crashed)


def tester_get_version(filename):
    if filename in file_info_map:
        return file_info_map[filename][0]
    else:
        return 0


def build_config(file):
    global server_state
    global rpc_locks

    data = []
    config_map = {}
    rpc_locks_creation = {}
    with open(file) as fp:
        data = fp.readlines()
    server_state.num_servers = int(data[0].split()[1])
    server_state.rpc_clients = [None] * server_state.num_servers
    for d in data[1:]:
        metadata = d.split(": ")
        server_id = int(metadata[0][8])
        config_map[server_id] = metadata[1].strip()
        server_state.rpc_clients[server_id] = xmlrpc.client.ServerProxy(uri="http://" + config_map[server_id])
        rpc_locks_creation[server_id] = threading.Lock()
    server_state.config_map = config_map
    rpc_locks = rpc_locks_creation
    server_state.match_index = [0] * server_state.num_servers
    server_state.next_index = [1] * server_state.num_servers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SurfStore Server")
    parser.add_argument('config', help='Path of config file')
    parser.add_argument('server_id', help='Self Id')
    args = parser.parse_args()

    server_state = ServerState()
    server_state.self_id = int(args.server_id)
    server_state.election_timeout = random.uniform(0.8, 2.0)
    build_config(args.config)
    apply_commits_thread = threading.Thread(target = applyCommits)
    send_heartbeats_thread = threading.Thread(target = send_heartbeats)
    start_election_thread = threading.Thread(target = start_election)

    try:
        print("Attempting to start XML-RPC Server...")
        hostport = server_state.config_map[server_state.self_id].split(":")
        server = threadedXMLRPCServer((hostport[0], int(hostport[1])), requestHandler=RequestHandler, logRequests=False)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")

        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries, "surfstore.appendEntries")
        server.register_function(tester_get_version, "surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        apply_commits_thread.start()
        send_heartbeats_thread.start()
        start_election_thread.start()
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
