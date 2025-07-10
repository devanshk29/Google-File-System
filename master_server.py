import struct
import socket
import os
import re
import time
import sys
import math
import json
import threading
import heapq
import random
import asyncio
from requestTracker import RequestTracker
available_chunk_servers = None
chunkHandleToChunkServer = None
fileToChunk = None
chunkNumberTotal = None
chunkhandletoport = None
totalload_port = None
portLoadQueue = None
portLoadMap = None
initial_replicas = 2
request_queue = None
chunkLoadMap = None
chunkServerPortAndConnection = None
tracker = None
chunkToLeaseServer = None


def sendJsonMessage(socket, message):
    dataToSend = json.dumps(message).encode('utf-8')
    socket.send(str(len(dataToSend)).encode('utf-8').ljust(10))
    socket.sendall(dataToSend)


def push_to_queue(priority_queue, key, value):
    # Push (value, key) to the heap
    heapq.heappush(priority_queue, (value, key))


def pushneg_to_queue(priority_queue, key, value):
    # Push (value, key) to the heap
    heapq.heappush(priority_queue, (-1*value, key))

# Function to pop the element with the smallest value from the priority queue


def pop_from_queue(priority_queue):
    if priority_queue:
        # Pop and return (value, key) with the smallest value
        return heapq.heappop(priority_queue)
    else:
        raise IndexError("pop from an empty priority queue")

# Function to peek at the smallest element without removing it


def peek_queue(priority_queue):
    if priority_queue:
        # Return the smallest element
        return priority_queue[0]
    else:
        raise IndexError("peek into an empty priority queue")


loadToStore = []
# function to store, load and no of chunks in output.txt evry 1 second
def storeInFile():
    while True:
        time.sleep(1)  # Simulate periodic check
        if 1 in chunkLoadMap and 1 in chunkHandleToChunkServer:
            # Debugging: Print the values being written
            x1 = chunkLoadMap[1].get_count()
            x2 = len(chunkHandleToChunkServer[1])
            print("writing", x1, x2)

            # Convert data to string format and write to the file
            with open('output.txt', 'a') as f:
                f.write(f"{{{x1},{x2}}}\n")  # Format: {x1,x2}
        else:
            print("not writing")


def receiveJsonMessage(socket):
    length = socket.recv(10)
    length = length.decode("utf-8").strip()
    length = int(length)
    message = bytearray()
    while len(message) < length:
        torecLen = min(2048, length - len(message))
        message.extend(socket.recv(torecLen))

    message = message.decode("utf-8")
    message = json.loads(message)
    return message


def ConnectToChunkServer(conn):

    port = receiveJsonMessage(conn)
    port = port["port"]
    chunkServerPortAndConnection[port] = conn
    available_chunk_servers.add(port)
    print(f"Connected to chunk server on port {port}")
    request_queue[port] = asyncio.Queue()

    # ping chunk server
    while True:
        data = receiveJsonMessage(conn)
        for chunk in data["available_chunks"]:
            if chunk not in chunkHandleToChunkServer:
                chunkHandleToChunkServer[chunk] = set()
            chunkHandleToChunkServer[chunk].add(port)

        if port in portLoadMap:
            totalload_port[port] = data["total_load"]
        else:
            totalload_port[port] = data["total_load"]
            print(port, totalload_port[port])
        time.sleep(1)


maxThreshold = 5
minThreshold = 1


def adjustReplicas():
    while True:
        available_chunk_handles = set(chunkhandletoport.keys())
        for chunk in available_chunk_handles:
            if chunk not in chunkHandleToChunkServer:
                continue
            load = chunkLoadMap[chunk].get_request_count()
            # print(chunk,load)
            print(f"load for chunk {chunk} is {load}")
            if load/len(chunkHandleToChunkServer[chunk]) > maxThreshold:
                new_number_of_replicas = min(math.ceil(
                    load/maxThreshold), len(available_chunk_servers)) - len(chunkHandleToChunkServer[chunk])
                if new_number_of_replicas > 0:
                    all_set = set(available_chunk_servers)
                    av_set = set(chunkhandletoport[chunk])
                    final_to_send_ports = []
                    queue1 = []
                    for server in av_set:
                        print(server, end=" ")
                    print()
                    for server in all_set:
                        print(server, end=" ")
                    print()
                    for server in all_set:
                        if server not in av_set:
                            print(server, end=" ")
                    print()
                    for server in all_set:
                        if server not in av_set:
                            push_to_queue(queue1, server,
                                          totalload_port[server])
                    for i in range(new_number_of_replicas):
                        if len(queue1) == 0:
                            break
                        value, server = pop_from_queue(queue1)
                        final_to_send_ports.append(server)
                    new_request = {
                        "type": "Message",
                        "role": "Transfer",
                        "chunkhandle": chunk,
                        "ports": final_to_send_ports
                    }
                    print("sending request to ", final_to_send_ports)
                    # print("sending request to ",final_to_send_ports)
                    sendJsonMessage(
                        chunkServerPortAndConnection[chunkhandletoport[chunk][0]], new_request)

            elif load/len(chunkHandleToChunkServer[chunk]) < minThreshold:
                remove_number_of_replicas = len(
                    chunkHandleToChunkServer[chunk]) - max(math.ceil(load/minThreshold), initial_replicas)
                if remove_number_of_replicas > 0:
                    rest_servers = chunkHandleToChunkServer[chunk]
                    rest_server_load = []
                    for server in rest_servers:
                        pushneg_to_queue(rest_server_load,
                                         server, totalload_port[server])
                    final_to_send_ports = []
                    for i in range(remove_number_of_replicas):
                        value, server = pop_from_queue(rest_server_load)
                        final_to_send_ports.append(server)
                        new_request = {
                            "type": "Message",
                            "role": "Delete",
                            "chunkhandle": chunk,
                        }
                        print(f"sending request {new_request} to {server} ")
                        chunkHandleToChunkServer[chunk].remove(server)

                        sendJsonMessage(
                            chunkServerPortAndConnection[server], new_request)
        time.sleep(1)


async def sendPingToChunkServer(port):
    """
    Continuously monitor the request queue for the chunk server on the specified port.
    Send pings to the chunk server whenever there's a message in the queue.
    """
    while True:
        try:
            # Wait for an item to be available in the queue
            queue = request_queue[port]
            request = await queue.get()
            sendJsonMessage(chunkServerPortAndConnection[port], request)
            await asyncio.sleep(0.5)

        except Exception as e:
            print(
                f"Error while processing request for server on port {port}: {e}")


def listenToChunkServers():
    chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    chunk_socket.bind((socket.gethostbyname('localhost'),
                      int(variables["master_chunk"])))
    chunk_socket.listen(100)
    while True:
        conn, addr = chunk_socket.accept()

        # data = conn.recv(1024)
        receivePing_thread = threading.Thread(
            target=ConnectToChunkServer, args=(conn,))
        receivePing_thread.start()
        # sendPing_thread.start()
        # new_thread.join()
        # conn.close()


def getPorts(replicas):
    # select 2 random ports from available_chunk_servers
    available_list = list(available_chunk_servers)
    if len(available_list) < replicas:
        return available_list
    ports = random.sample(available_list, replicas)
    return ports


def getPortForDownload(chunk):
    load = 10000000000
    if chunk not in chunkHandleToChunkServer:
        return None
    list1 = list(chunkHandleToChunkServer[chunk])
    finalPort = list1[0]
    # port = chunkhandletoport[chunk][0]
    for port in list1:
        if port in available_chunk_servers:
            # return port
            # load = max(load,totalload_chunk[port])
            if load > totalload_port[port]:
                load = totalload_port[port]
                finalPort = port
    return finalPort
    # return chunkhandletoport[chunk][0]


def listenToClients():
    global tracker
    global chunkNumberTotal
    global chunkToLeaseServer
    global chunkLoadMap
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((socket.gethostbyname('localhost'),
                       int(variables["master_client"])))
    client_socket.listen(100)
    while True:
        conn, addr = client_socket.accept()
        message = receiveJsonMessage(conn)
        if message["type"] == 'Message' and message["role"] == 'Upload':
            # print("yes",message)
            # chunkToLeaseServer[chunk]
            numberOfChunks = math.ceil(
                message["filesize"] / int(variables["chunk_size"]))
            print("Number of chunks ", numberOfChunks)
            total_ports = []
            chunkhandles = []
            for i in range(numberOfChunks):
                chunkNumberTotal += 1
                chunkHandle = chunkNumberTotal
                if message["filename"] not in fileToChunk:
                    fileToChunk[message["filename"]] = []
                fileToChunk[message["filename"]].append(chunkHandle)
                replicas = initial_replicas
                ports = getPorts(replicas)
                total_ports.append(ports)
                chunkhandles.append(chunkHandle)
                if chunkHandle not in chunkLoadMap:
                    chunkLoadMap[chunkHandle] = RequestTracker()
                if chunkHandle not in chunkhandletoport:
                    chunkhandletoport[chunkHandle] = []
                chunkhandletoport[chunkHandle] = ports
            message1 = {
                "type": "Message",
                "role": "Ports",
                "chunkhandle": chunkhandles,
                "ports": total_ports
            }
            print("msg to send to  client : ", message1)
            sendJsonMessage(conn, message1)
        elif message["type"] == 'Message' and message["role"] == 'Download':
            filename = message["filename"]
            if filename in fileToChunk:
                portforeachchunk = []
                for chunkHandle in fileToChunk[filename]:
                    chunkLoadMap[chunkHandle].add_request()
                    # tracker.add_request()
                    portforeachchunk.append([getPortForDownload(chunkHandle)])

                message1 = {
                    "type": "Message",
                    "role": "Ports",
                    "chunkhandles": fileToChunk[filename],
                    "ports": portforeachchunk
                }
                sendJsonMessage(conn, message1)

            else:
                message1 = {
                    "type": "Message",
                    "role": "Error",
                    "message": "File not found"
                }
                sendJsonMessage(conn, message1)
            pass
        elif message["type"] == 'Message' and message["role"] == 'Update':
            filename = message["filename"]
            if filename in fileToChunk:
                portforeachchunk = []

                for chunk in fileToChunk[filename]:
                    chunkLoadMap[chunk].add_request()
                    # tracker.add_request()
                    portforeachchunk.append(chunkhandletoport[chunk])

                message1 = {
                    "type": "Message",
                    "role": "Ports",
                    "chunkhandles": fileToChunk[filename],
                    "ports": portforeachchunk
                }
                sendJsonMessage(conn, message1)

            else:
                message1 = {
                    "type": "Message",
                    "role": "Error",
                    "message": "File not found"
                }
                sendJsonMessage(conn, message1)
        elif message["type"] == 'Chunk' and message["role"] == 'Create':

            print("message ", message)
            filename = message["filename"]
            chunkNumberTotal += 1
            current_chunk = chunkNumberTotal
            fileToChunk[filename].append(current_chunk)
            replicas = initial_replicas
            ports = getPorts(replicas)
            chunkhandletoport[current_chunk] = ports
            message1 = {
                "type": "Message",
                "role": "Portsfornewchunk",
                "chunkhandle": current_chunk,
                "ports": ports,
                # "chunk": message["chunk"]
            }
            sendJsonMessage(conn, message1)
            print("message sent ", message1)

            pass


def loadBalancer():
    while True:
        if len(portLoadQueue) > 0:
            port = portLoadQueue[0]
            if port in available_chunk_servers:
                portLoadQueue.pop(0)
                portLoadQueue.append(port)
                request = {
                    "type": "Message",
                    "role": "Ping"
                }
                request_queue[port].put(request)
            else:
                portLoadQueue.pop(0)
        time.sleep(1)
    pass


if __name__ == "__main__":
    # global variables
    available_chunk_servers = set()
    chunkHandleToChunkServer = {}
    fileToChunk = {}
    # loadToPlot = {}
    chunkhandletoport = {}
    chunkNumberTotal = 0
    totalload_chunk = {}
    portLoadQueue = []
    portLoadMap = {}
    request_queue = {}
    chunkLoadMap = {}
    total_load = 0
    chunkServerPortAndConnection = {}
    totalload_port = {}
    chunkToLeaseServer = {}
    with open('infoFile.json') as json_file:
        variables = json.load(json_file)
    # start threads for listening
    # tracker = RequestTracker()
    chunk_thread = threading.Thread(target=listenToChunkServers)
    client_thread = threading.Thread(target=listenToClients)
    adjustReplicas_thread = threading.Thread(target=adjustReplicas)
    store=threading.Thread(target=storeInFile)
    # load_thread = threading.Thread(target=loadBalancer)
    # ping_thread = threading.Thread(target=startPingListeners)

    chunk_thread.start()
    client_thread.start()
    adjustReplicas_thread.start()
    store.start()
    # load_thread.start()
    # ping_thread.start()
    
    store.join()
    chunk_thread.join()
    client_thread.join()
    adjustReplicas_thread.join()
    # load_thread.join()
    # ping_thread.join()
