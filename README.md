
# Column Store Databas


## Introduction

mydb_manager.c - the core functions for managing db are
parse.c - core files for parsing DSL functions 
"shutdown" dsl command writes the db to to the DB_FILE defined in mydb_manager.c


## Client-Server code

You can build both the client and server using:

> `make all`

You should spin up the server first before trying to connect the client.

> `./server`
> `./client`

A high-level explanation of what happens is:

1. The server creates a socket to listen for an incoming connection.

2. The client attempts to connect to the server.

3. When the client has successfully connected, it waits for input from stdin.
Once received, it will create a message struct containing the input and
then send it to the server.  It immediately waits for a response to determine
if the server is willing to process the command.

4. When the server notices that a client has connected, it waits for a message
from the client.  When it receives the message from the client, it parses the
input and decides whether it is a valid/invalid query.
It immediately returns the response indicating whether it was valid or not.

5. Once the client receives the response, three things are possible:
1) if the query was invalid, then just go back to waiting for input from stdin.
2) if the query was valid and the server indicates that it will send back the
result, then wait to receive another message from the server.
3) if the query was valid but the server indicates that it will not send back
the result, then go back to waiting for input on stdin.

6. Back on the server side, if the query is a valid query then it should
process it, and then send back the result if it was asked to.

## Logging

I have included a couple useful logging functions in utils.c.
These logging functions depend on #ifdef located within the code.
There are multiple ways to enable logging. One way is by adding your own
definition at the top of the file:

> `#define LOG 1`

The other way is to add it during the compilation process. Instead of running
just `make`, you can run:

> `make CFLAGS+="-DLOG -DLOG_ERR -DLOG_INFO"
