import socket
import os
# import pickle
import re
import time
import sys
# import threading
import math
import json
# global variables = {}

import json
import struct


def sendJsonMessage(socket, message):
    dataToSend = json.dumps(message).encode('utf-8')
    socket.send(str(len(dataToSend)).encode('utf-8').ljust(10))
    socket.sendall(dataToSend)


def receiveJsonMessage(socket):

    length = socket.recv(10)
    length = length.decode("utf-8").strip()
    length = int(length)
    message = bytearray()
    # print("yes1")
    while len(message) < length:
        # print("yes2")
        torecLen = min(2048, length - len(message))
        message.extend(socket.recv(torecLen))
    # print("yes3")
    message = message.decode("utf-8")
    # print("message ", message)
    message = json.loads(message)
    return message


def getListFiles():
    pass


def uploadFile(filepath):
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((socket.gethostbyname(
        'localhost'), int(variables["master_client"])))

    filesize = os.path.getsize(filepath)
    chunk_size = int(variables["chunk_size"])
    filename = os.path.basename(filepath)

    message = {
        "type": "Message",
        "role": "Upload",
        "filename": filepath,
        "filesize": filesize
    }
    sendJsonMessage(master_socket, message)
    # print("message sent")
    response = receiveJsonMessage(master_socket)
    # print(response)
    if response["type"] == "Message" and response["role"] == "Ports":

        num_chunks = math.ceil(filesize / chunk_size)
        with open(filepath, "r") as file:
            for i in range(num_chunks):
                chunk = file.read(chunk_size)
                chunkhandle = response["chunkhandle"][i]
                ports = response["ports"][i]
                chunk_server = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                chunk_server.connect(
                    (socket.gethostbyname('localhost'), int(ports[0])))
                msg1 = {
                    "type": "Chunk",
                    "role": "Upload",
                    "chunkhandle": chunkhandle,
                    "ports": ports[1:],
                    "chunk": chunk
                }
                sendJsonMessage(chunk_server, msg1)

    # send
    pass


def downloadFile(filename, startpos, offset):

    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((socket.gethostbyname(
        'localhost'), int(variables["master_client"])))

    message = {
        "type": "Message",
        "role": "Download",
        "filename": filename,
    }

    sendJsonMessage(master_socket, message)
    # print("message sent")
    response = receiveJsonMessage(master_socket)

    # now calculate the chunk to read
    startchunk = int(int(startpos)/int(variables["chunk_size"]))
    startoffsetinstartchunk = int(startpos) % int(variables["chunk_size"])

    endchunk = int((int(startpos)+int(offset)-1)/int(variables["chunk_size"]))
    endoffsetinendchunk = (int(startpos)+int(offset)-1
                           ) % int(variables["chunk_size"])

    if response["type"] == "Message" and response["role"] == "Ports":
        chunkhandles = response["chunkhandles"]
        ports = response["ports"]

        currport = ports[startchunk][0]
        currchunk = chunkhandles[startchunk]
        currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        currsocket.connect(
            (socket.gethostbyname('localhost'), int(currport)))
        msg1 = {
            "type": "Chunk",
            "role": "Download",
            "chunkhandle": currchunk,
            "startoffset": int(startoffsetinstartchunk),
            "endoffset": int(variables["chunk_size"])
        }
        sendJsonMessage(currsocket, msg1)
        response = receiveJsonMessage(currsocket)
        print(response["chunk"])

        for i in range(startchunk+1, endchunk):
            currport = ports[i][0]
            currchunk = chunkhandles[i]
            currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currsocket.connect(
                (socket.gethostbyname('localhost'), int(currport)))
            msg1 = {
                "type": "Chunk",
                "role": "Download",
                "chunkhandle": currchunk,
                "startoffset": 0,
                "endoffset": int(variables["chunk_size"])
            }
            sendJsonMessage(currsocket, msg1)
            response = receiveJsonMessage(currsocket)
            print(response["chunk"])

        if startchunk == endchunk:
            return

        currport = ports[endchunk][0]
        currchunk = chunkhandles[endchunk]
        currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        currsocket.connect(
            (socket.gethostbyname('localhost'), int(currport)))
        msg1 = {
            "type": "Chunk",
            "role": "Download",
            "chunkhandle": currchunk,
            "startoffset": 0,
            "endoffset": endoffsetinendchunk
        }
        sendJsonMessage(currsocket, msg1)
        response = receiveJsonMessage(currsocket)
        print(response["chunk"])

    pass


def updateFile(filename, startpos, datatowrite):
    offset = len(datatowrite)
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((socket.gethostbyname(
        'localhost'), int(variables["master_client"])))

    message = {
        "type": "Message",
        "role": "Update",
        "filename": filename,
    }

    sendJsonMessage(master_socket, message)
    # print("message sent")
    response = receiveJsonMessage(master_socket)

    # now calculate the chunk to read
    startchunk = int(int(startpos)/int(variables["chunk_size"]))
    startoffsetinstartchunk = int(startpos) % int(variables["chunk_size"])

    endchunk = int((int(startpos)+int(offset)-1)/int(variables["chunk_size"]))
    endoffsetinendchunk = (int(startpos)+int(offset)-1
                           ) % int(variables["chunk_size"])

    if response["type"] == "Message" and response["role"] == "Ports":
        chunkhandles = response["chunkhandles"]
        ports = response["ports"]

        if startchunk < len(ports):
            currport = ports[startchunk][0]
            currchunk = chunkhandles[startchunk]
            currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currsocket.connect(
                (socket.gethostbyname('localhost'), int(currport)))
            msg1 = {
                "type": "Chunk",
                "role": "Update",
                "chunkhandle": currchunk,
                "startoffset": int(startoffsetinstartchunk),
                "endoffset": min(int(variables["chunk_size"]), int(len(datatowrite))+int(startoffsetinstartchunk))-1,
                "ports": ports[startchunk][1:],
                "chunk": datatowrite[0:int(variables["chunk_size"])-startoffsetinstartchunk]
            }
            datatowrite = datatowrite[int(
                variables["chunk_size"])-startoffsetinstartchunk:]
            sendJsonMessage(currsocket, msg1)
        # response = receiveJsonMessage(currsocket)
        # print(response["chunk"])

        for i in range(startchunk+1, min(endchunk, len(ports))):
            currport = ports[i][0]
            currchunk = chunkhandles[i]
            currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currsocket.connect(
                (socket.gethostbyname('localhost'), int(currport)))
            msg1 = {
                "type": "Chunk",
                "role": "Update",
                "chunkhandle": currchunk,
                "startoffset": 0,
                "ports": ports[i][1:],
                "endoffset": int(variables["chunk_size"])-1,
                "chunk": datatowrite[0:int(variables["chunk_size"])]
            }
            datatowrite = datatowrite[int(variables["chunk_size"]):]
            sendJsonMessage(currsocket, msg1)
            # response = receiveJsonMessage(currsocket)
            # print(response["chunk"])

        if endchunk >= len(ports):

            for i in range(len(ports), endchunk+1):
                msg1 = {
                    "type": "Chunk",
                    "role": "Create",
                    "filename": filename,
                    "startoffset": 0,
                    "endoffset": int(variables["chunk_size"]),
                }
                # print("sending to master ", msg1)
                nmaster_socket = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                nmaster_socket.connect((socket.gethostbyname(
                    'localhost'), int(variables["master_client"])))

                sendJsonMessage(nmaster_socket, msg1)
                response = receiveJsonMessage(nmaster_socket)

                currport = response["ports"][0]
                currchunk = response["chunkhandle"]
                currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                currsocket.connect(
                    (socket.gethostbyname('localhost'), int(currport)))
                msg1 = {
                    "type": "Chunk",
                    "role": "Upload",
                    "ports": response["ports"][1:],
                    "chunkhandle": currchunk,
                    "chunk": datatowrite[0:min(int(variables["chunk_size"]), int(len(datatowrite)))]
                }
                if i != endchunk:
                    datatowrite = datatowrite[int(variables["chunk_size"]):]
                sendJsonMessage(currsocket, msg1)

        else:
            if startchunk == endchunk:
                return
            currport = ports[endchunk][0]
            currchunk = chunkhandles[endchunk]
            currsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            currsocket.connect(
                (socket.gethostbyname('localhost'), int(currport)))
            msg1 = {
                "type": "Chunk",
                "role": "Update",
                "chunkhandle": currchunk,
                "startoffset": 0,
                "endoffset": int(endoffsetinendchunk),
                "chunk": datatowrite[0:int(endoffsetinendchunk)+1],
                "ports": ports[endchunk][1:]
            }
            datatowrite = datatowrite[int(endoffsetinendchunk):]
            sendJsonMessage(currsocket, msg1)
            # response = receiveJsonMessage(currsocket)
            # print(response["chunk"])


if __name__ == "__main__":
    global variables
    with open("infoFile.json", "r") as file:
        variables = json.load(file)

    while True:
        command = int(input(
            "Enter command:\n 1. listfiles \n 2.upload \n 3. download \n 4. update  \n 5. Sleep\n 6. exit\n"))
        if command == 1:
            getListFiles()
        elif command == 2:
            filepath = input("Enter absolute file path: ")
            print(filepath)
            uploadFile(filepath)
        elif command == 3:
            filename = input("Enter file name: ")
            print(filename)
            startpos = input("Enter start position: ")
            print(startpos)
            offset = input("Enter offset: ")
            print(offset)
            downloadFile(filename, startpos, offset)
        elif command == 4:
            filename = input("Enter file name: ")
            print(filename)
            startpos = input("Enter start position: ")
            print(startpos)
            # offset = input("Enter offset: ")
            datatowrite = input("Enter data to write: ")
            print(datatowrite)
            updateFile(filename, startpos, datatowrite)
        elif command == 5:
            sleeptime = float(input("Enter time to sleep"))
            print(sleeptime)
            time.sleep(sleeptime)
        elif command == 6:
            print("Exiting...")
            sys.exit()
        else:
            print("Invalid command")
            continue
