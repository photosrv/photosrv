**Photosrv** (pronounced "photo-serve") - sharded reverse proxy HTTP server

# WHAT IS IT, IN A NUTSHELL?

Photosrv is a consistently hashed HTTP router aimed at distributing static content.

It allows you to configure dozens or even hundreds of individual machines
responsible for storing hundreds of millions of files. The system is best
employed in conjunction with CDN(s).

You can effectively consider it a load balancer for your static content.

# HOW DOES IT WORK?

Clients write (PUT/DELETE) to the storage nodes.
Photosrv reverse proxies the incoming (GET/HEAD) requests to the storage node(s).

## Configuration

You define a small Apache-like config file, with a list of data centers (clusters).
You must have at least one data center, but it could consist of machines
all over the world, for instance.

Each data center contains a list of shards, numbered in order. Each shard
is a list of nodes. If you want 3 copies of every file you can configure
every shard to have 3 nodes. Each node should be a single individual machine.


## Routing

The Photosrv router uses the config file in combination with a *roster* file to determine where to route. The roster file is a (constantly updated) list of active storage hosts.

For every incoming request, the router consistently hashes the incoming path to figure out which shard contains that file. The hashing is configurable; Jump hash is presently recommended.

Photosrv has a couple of configurable routing modes:

* Forward to a random node within the calculated shard
* A 'multi-HEAD timeout' mode which forwards to a successful HEAD node.

    **NOTE:**
    The multi-HEAD timeout results in high read amplification, and requires solid
    networking infrastructure. The idea is that all nodes are asked 'HEAD
    /PATH', and a random "fastest" node within the timeout window will be
    forwarded the request. If every node responds with a 404 or a timeout, then
    we forward the request to a random node anyway.

## Storage

Each storage node is a single logical machine running a WebDAV server that a
Photosrv node can route requests to for reading files. The only current
requirements are HEAD, GET, PUT, and DELETE. Help with implementing PATCH
forwarding would be appreciated, to allow for resumable file upload support via
tus. See [CONTRIBUTING.md][]

Because Photosrv itself relies on the built-in Go http and reverse proxy
packages, it has implicit support for streaming data and resumable downloads.
However, it is up to the client to support resumable downloads as well.

# CLIENT SUPPORT (PUT/DELETE)

Storing files is currently possible using Go and a sample command-line utility
for uploading data via Go is provided in 'cmd/shardsprites'. The Perl client
needs polish before it will be suitable for general-purpose consumption.

Photosrv may eventually handle PUTs and do replication itself, but currently it
is up to the client to do that.

# WHAT IS NOT IMPLEMENTED?

You have to manage your own metadata, i.e. track your own files.

Replication is currently implemented in the clients - not the server/cluster.

There is tooling provided for manual node repair. A gossip-based automated
node repair and metadata storage mechanism is under development.

You can not 'ls somebucket' and get a list of existing files. This works more
like a distributed key-value store for arbitrarily sized blobs, where you just
rely on HTTP and the filesystem/hardware underneath. You only
have set(), get(), and delete(). (Well, HEAD).

Automatic rebalancing and migration are not yet supported. Manual modes
have been executed but are not yet documented.

# GETTING STARTED 

While you could, in theory, do everything on 1 node;
A minimal cluster would be:

- At least 2 WebDAV storage hosts
- At least 1 Photosrv host

For a production setup, nginx is recommended.  For a Gopher or hobbyist setup, you
may prefer pocketdav. (See [github.com/rbastic/pocketdav](https://github.com/rbastic/pocketdav))

<!---(nnuss): not yet
    # From a working Go install:
    $ go get github.com/rbastic/photosrv
    $ photosrv -c=/etc/photosrv.conf -httport=8888 -debugport=8080 -roster=/etc/photosrv.roster -rosterCheck=1s
    ...
    # service running on port 8888
-->

# THE FUTURE

See [ROADMAP][].

# CONTRIBUTING

See [CONTRIBUTING.md][]

Look at [ROADMAP][], look at [TODO][], pick something and see if it exists in the
'Issues' section on github already, and announce that you're intending on
working on it if so or maybe the person who was working on it needs help too!

# ACKNOWLEDGMENT

This software was originally developed at Booking.com. With approval from
Booking.com, this software was released as open source, for which the authors
would like to express their gratitude.

# LICENSE

See [LICENSE][] file.

[CONTRIBUTING.md]: CONTRIBUTING.md
[ROADMAP]: ROADMAP
[LICENSE]: LICENSE
[TODO]: TODO
