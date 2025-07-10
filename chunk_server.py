import struct
import socket
import os
import re
import time

import sys
import math
import json
import threading
client_chunk_port = None
avaialble_chunks = None
total_load = None


def sendJsonMessage(socket, message):
    dataToSend = json.dumps(message).encode('utf-8')
    socket.send(str(len(dataToSend)).encode('utf-8').ljust(10))
    socket.sendall(dataToSend)


def receiveJsonMessage(socket):
    length = socket.recv(10)
    length = length.decode("utf-8").strip()
    length = int(length)
    message = bytearray()
    while len(message) < length:
        torecLen = min(2048, length - len(message))
        message.extend(socket.recv(torecLen))

    message = message.decode("utf-8")
    print("message ", message)
    message = json.loads(message)
    return message

# start threads to listen to master and client


def sendPingToMaster(master_socket):
    # send all the available chunks to the master
    while (1):
        print("Sending ping to master")
        new_msg = {
            "type": "Ping",
            "chunk_port": client_chunk_port,
            "available_chunks": list(avaialble_chunks),
            "total_load": total_load,
        }
        sendJsonMessage(master_socket, new_msg)
        time.sleep(1)


def listenMaster(master_socket):
    print("tried to listen to master")
    while True:
        # print("Listening to master")
        new_request = receiveJsonMessage(master_socket)
        print("Received request from master", new_request)
        if new_request["type"] == "Message" and new_request["role"] == "Transfer":
            chunk_handle = new_request["chunkhandle"]
            old_file = f"chunk_directory_{client_chunk_port}/chunk_{chunk_handle}"
            with open(old_file, "r") as file:
                chunk = file.read()
            nr = {
                "type": "Chunk",
                "role": "Upload",
                "chunkhandle": new_request["chunkhandle"],
                "ports": new_request["ports"],
                "chunk": chunk
            }
            print("Sending chunk to next chunk server", new_request["ports"])
            sendToNextChunkServer(nr)
        if new_request["type"] == "Message" and new_request["role"] == "Delete":
            chunk_handle = new_request["chunkhandle"]

            old_file = f"{current_directory}/chunk_directory_{client_chunk_port}/chunk_{chunk_handle}"
            os.remove(old_file)
            avaialble_chunks.remove(chunk_handle)


def listenToMaster():
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((socket.gethostbyname(
        'localhost'), int(variables["master_chunk"])))
    # master_socket.sendall(str(client_chunk_port).encode())
    new_msg = {
        "port": client_chunk_port
    }
    sendJsonMessage(master_socket, new_msg)
    ping_thread = threading.Thread(
        target=sendPingToMaster, args=(master_socket,))
    listen_thread = threading.Thread(
        target=listenMaster, args=(master_socket,)
    )
    ping_thread.start()
    listen_thread.start()
    ping_thread.join()
    listen_thread.join()

    # ping code


def sendToNextChunkServer(data):
    if data["ports"] == []:
        return
    # send the chunk to the next chunk server
    next_chunk_server = data["ports"][0]
    chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    chunk_socket.connect(
        (socket.gethostbyname('localhost'), int(next_chunk_server)))
    ports = data["ports"]
    updated_data = {
        "type": "Chunk",
        "role": "Upload",
        "chunkhandle": data["chunkhandle"],
        "ports": ports[1:],
        "chunk": data["chunk"]
    }
    sendJsonMessage(chunk_socket, updated_data)
    chunk_socket.close()


def sendToNextChunkServerOffset(data):
    if data["ports"] == []:
        return
    # send the chunk to the next chunk server
    next_chunk_server = data["ports"][0]
    chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    chunk_socket.connect(
        (socket.gethostbyname('localhost'), int(next_chunk_server)))
    ports = data["ports"]
    updated_data = {
        "type": "Chunk",
        "role": "Update",
        "startoffset": data["startoffset"],
        "endoffset": data["endoffset"],
        "chunkhandle": data["chunkhandle"],
        "ports": ports[1:],
        "chunk": data["chunk"]
    }
    sendJsonMessage(chunk_socket, updated_data)
    chunk_socket.close()


def write_to_file(chunk_handle, chunk):
    fileName = f"chunk_{chunk_handle}"
    # Replace with your desired folder path
    folder_path = f"chunk_directory_{client_chunk_port}"
    # Replace with your desired file name
    new_file = os.path.join(folder_path, fileName)
    os.makedirs(folder_path, exist_ok=True)
    with open(new_file, "w") as file:
        file.write(chunk)

# def write_to_file_offset(chunk_handle, chunk,startoffset,endoffset):
#     fileName = f"chunk_{chunk_handle}"
#     # Replace with your desired folder path
#     folder_path = f"chunk_directory_{client_chunk_port}"
#     # Replace with your desired file name
#     new_file = os.path.join(folder_path, fileName)
#     os.makedirs(folder_path, exist_ok=True)
#     with open(new_file, "r+") as file:
#         file.seek(startoffset)
#         file.write(chunk)


def listenToClient():
    global avaialble_chunks
    global total_load
    # global client_chunk_port
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((socket.gethostbyname(
        'localhost'), int(client_chunk_port)))
    client_socket.listen(100)
    while True:
        conn, addr = client_socket.accept()
        message = receiveJsonMessage(conn)
        print(message)
        if message["type"] == "Chunk" and message["role"] == "Upload":
            chunk_handle = message["chunkhandle"]
            new_file = "chunk_" + str(chunk_handle)
            print(f"writing chunk to {new_file}")
            write_to_file(chunk_handle, message["chunk"])
            avaialble_chunks.add(chunk_handle)
            # send the chunk to the next chunk server
            sendToNextChunkServer(message)
        if message["type"] == "Chunk" and message["role"] == "Download":
            chunk_handle = message["chunkhandle"]
            old_file = f"chunk_directory_{client_chunk_port}/chunk_{chunk_handle}"
            # new_file = "chunk_" + str(chunk_handle)
            startoffset = message["startoffset"]
            endoffset = message["endoffset"]
            print(f"reading chunk from {old_file}")
            total_load += 1
            with open(old_file, "r") as file:
                file.seek(startoffset)
                chunk = file.read(endoffset-startoffset)
                print("chunk", chunk)
            new_msg = {
                "type": "Chunk",
                "role": "Download",
                "chunkhandle": chunk_handle,
                "chunk": chunk
            }
            sendJsonMessage(conn, new_msg)
        if message["type"] == "Chunk" and message["role"] == "Update":
            chunk_handle = message["chunkhandle"]
            old_file = f"chunk_directory_{client_chunk_port}/chunk_{chunk_handle}"
            startoffset = message["startoffset"]
            endoffset = message["endoffset"]
            new_chunk = message["chunk"]
            total_load += 1
            with open(old_file, "r+") as file:
                file.seek(startoffset)
                file.write(new_chunk)

            # if message["ports"] != []:
                # send the chunk to the next chunk server

            sendToNextChunkServerOffset(message)

            pass
        # conn.close()


def loadZero():
    global total_load
    while True:
        total_load = 0
        time.sleep(1)


if __name__ == "__main__":
    # global variables
    current_directory = os.getcwd()
    # global client_chunk_port
    avaialble_chunks = set()
    total_load = 0
    client_chunk_port = int(
        input("Enter the port number for the chunk server: "))
    with open('infoFile.json') as json_file:
        variables = json.load(json_file)

    # start threads for listening
    master_thread = threading.Thread(target=listenToMaster)
    client_thread = threading.Thread(target=listenToClient)
    load_thread = threading.Thread(target=loadZero)
    # start both threads
    master_thread.start()
    client_thread.start()
    load_thread.start()
    # optionally wait for both threads to complete if needed (not necessary for infinite listen)
    master_thread.join()
    client_thread.join()
    load_thread.join()
