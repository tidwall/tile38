<p align="center">
  <a href="http://tile38.com"><img 
    src="/doc/logo1500.png" 
    width="200" height="200" border="0" alt="Tile38"></a>
</p>
<p align="center">
<a href="https://gitter.im/tile38/tile38?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge"><img src="https://badges.gitter.im/Join%20Chat.svg" alt="Gitter"></a>
<a href="https://github.com/tidwall/tile38/releases"><img src="https://img.shields.io/badge/version-1.9.0-green.svg?" alt="Version"></a>
<a href="https://travis-ci.org/tidwall/tile38"><img src="https://travis-ci.org/tidwall/tile38.svg?branch=master" alt="Build Status"></a>
<a href="https://hub.docker.com/r/tile38/tile38"><img src="https://img.shields.io/badge/docker-ready-blue.svg" alt="Docker Ready"></a>
</p>

Tile38 is an open source (MIT licensed), in-memory geolocation data store, spatial index, and realtime geofence. It supports a variety of object types including lat/lon points, bounding boxes, XYZ tiles, Geohashes, and GeoJSON. 

<p align="center">
<i>This README is quick start document. You can find detailed documentation at <a href="http://tile38.com">http://tile38.com</a>.</i><br><br>
<a href="#searching"><img src="/doc/search-nearby.png" alt="Nearby" border="0" width="120" height="120"></a>
<a href="#searching"><img src="/doc/search-within.png" alt="Within" border="0" width="120" height="120"></a>
<a href="#searching"><img src="/doc/search-intersects.png" alt="Intersects" border="0" width="120" height="120"></a>
<a href="http://tile38.com/topics/geofencing"><img src="/doc/geofence.gif" alt="Geofencing" border="0" width="120" height="120"></a>
<a href="http://tile38.com/topics/roaming-geofences"><img src="/doc/roaming.gif" alt="Roaming Geofences" border="0" width="120" height="120"></a>
</p>

## Features

- Spatial index with [search](#searching) methods such as Nearby, Within, and Intersects.
- Realtime [geofencing](#geofencing) through persistent sockets or [webhooks](http://tile38.com/commands/sethook).
- Object types of [lat/lon](#latlon-point), [bbox](#bounding-box), [Geohash](#geohash), [GeoJSON](#geojson), [QuadKey](#quadkey), and [XYZ tile](#xyz-tile).
- Support for lots of [Clients Libraries](#client-libraries) written in many different languages.
- Variety of protocols, including [http](#http) (curl), [websockets](#websockets), [telnet](#telnet), and the [Redis RESP](http://redis.io/topics/protocol).
- Server responses are [RESP](http://redis.io/topics/protocol) or [JSON](http://www.json.org).
- Full [command line interface](#cli).
- Leader / follower [replication](#replication).
- In-memory database that persists on disk.
- All coordinates are in [WGS 84 Web Mercator / EPSG:3857](#coordinate-system)

## Components
- `tile38-server    ` - The server
- `tile38-cli       ` - Command line interface tool
- `tile38-benchmark ` - Server benchmark tool

## Getting Started

### Getting Tile38

The easiest way to get the latest Tile38 is to use one of the pre-built release binaries which are available for OSX, Linux, and Windows. Instructions for using these binaries are on the GitHub [releases page](https://github.com/tidwall/tile38/releases).

Mac users who use Homebrew can install with `brew install tile38`.

Tile38 is also available as a [Docker image](https://hub.docker.com/r/tile38/tile38/) which is built on top of [Alpine Linux](https://alpinelinux.org/).

### Building Tile38 

Tile38 can be compiled and used on Linux, OSX, Windows, FreeBSD, and probably others since the codebase is 100% Go. We support both 32 bit and 64 bit systems. [Go](https://golang.org/dl/) must be installed on the build machine.

To build everything simply:
```
$ make
```

To test:
```
$ make test
```

## Running 
For command line options invoke:
```
$ ./tile38-server -h
```

To run a single server:

```
$ ./tile38-server

# The tile38 shell connects to localhost:9851
$ ./tile38-cli
> help
```

## Coordinate System
It's important to note that the coordinate system Tile38 uses is 
[WGS 84 Web Mercator](https://en.wikipedia.org/wiki/Web_Mercator), also known 
as EPSG:3857. All distance are in meters and all calcuations are done on a spherical surface, 
not a plane.

## <a name="cli"></a>Playing with Tile38

Basic operations:
```
$ ./tile38-cli

# add a couple of points named 'truck1' and 'truck2' to a collection named 'fleet'.
> set fleet truck1 point 33.5123 -112.2693   # on the Loop 101 in Phoenix
> set fleet truck2 point 33.4626 -112.1695   # on the I-10 in Phoenix

# search the 'fleet' collection.
> scan fleet                                 # returns both trucks in 'fleet'
> nearby fleet point 33.462 -112.268 6000    # search 6 kilometers around a point. returns one truck.

# key value operations
> get fleet truck1                           # returns 'truck1'
> del fleet truck2                           # deletes 'truck2'
> drop fleet                                 # removes all 
```

Tile38 has a ton of [great commands](http://tile38.com/commands).

## Fields
Fields are extra data that belongs to an object. A field is always a double precision floating point. There is no limit to the number of fields that an object can have. 

To set a field when setting an object:
```
> set fleet truck1 field speed 90 point 33.5123 -112.2693             
> set fleet truck1 field speed 90 field age 21 point 33.5123 -112.2693
```

To set a field when an object already exists:
```
> fset fleet truck1 speed 90
```

## Searching

Tile38 has support to search for objects and points that are within or intersects other objects. All object types can be searched including Polygons, MultiPolygons, GeometryCollections, etc.

<img src="/doc/search-within.png" width="200" height="200" border="0" alt="Search Within" align="left">

#### Within 
WITHIN searches a collection for objects that are fully contained inside a specified bounding area.
<BR CLEAR="ALL">

<img src="/doc/search-intersects.png" width="200" height="200" border="0" alt="Search Intersects" align="left">

#### Intersects
INTERSECTS searches a collection for objects that intersect a specified bounding area.
<BR CLEAR="ALL">

<img src="/doc/search-nearby.png" width="200" height="200" border="0" alt="Search Nearby" align="left">

#### Nearby
NEARBY searches a collection for objects that intersect a specified radius.
<BR CLEAR="ALL">





### Search options
**SPARSE** - This option will distribute the results of a search evenly across the requested area.  
This is very helpful for example; when you have many (perhaps millions) of objects and do not want them all clustered together on a map. Sparse will limit the number of objects returned and provide them evenly distributed so that your map looks clean.<br><br>
You can choose a value between 1 and 8. The value 1 will result in no more than 4 items. The value 8 will result in no more than 65536. *1=4, 2=16, 3=64, 4=256, 5=1024, 6=4098, 7=16384, 8=65536.*<br><br>
<table>
<td>No Sparsing<img src="/doc/sparse-none.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 1<img src="/doc/sparse-1.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 2<img src="/doc/sparse-2.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 3<img src="/doc/sparse-3.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 4<img src="/doc/sparse-4.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 5<img src="/doc/sparse-5.png" width="100" height="100" border="0" alt="Search Within"></td>
</table>
*Please note that the higher the sparse value, the slower the performance. Also, LIMIT and CURSOR are not available when using SPARSE.* 

**WHERE** - This option allows for filtering out results based on [field](#fields) values. For example<br>```nearby fleet where speed 70 +inf point 33.462 -112.268 6000``` will return only the objects in the 'fleet' collection that are within the 6 km radius **and** have a field named `speed` that is greater than `70`. <br><br>Multiple WHEREs are concatenated as **and** clauses. ```WHERE speed 70 +inf WHERE age -inf 24``` would be interpreted as *speed is over 70 <b>and</b> age is less than 24.*<br><br>The default value for a field is always `0`. Thus if you do a WHERE on the field `speed` and an object does not have that field set, the server will pretend that the object does and that the value is Zero.

**MATCH** - MATCH is similar to WHERE except that it works on the object id instead of fields.<br>```nearby fleet match truck* point 33.462 -112.268 6000``` will return only the objects in the 'fleet' collection that are within the 6 km radius **and** have an object id that starts with `truck`. There can be multiple MATCH options in a single search. The MATCH value is a simple [glob pattern](https://en.wikipedia.org/wiki/Glob_(programming)).

**CURSOR** - CURSOR is used to iterate though many objects from the search results. An iteration begins when the CURSOR is set to Zero or not included with the request, and completes when the cursor returned by the server is Zero.

**NOFIELDS** - NOFIELDS tells the server that you do not want field values returned with the search results.

**LIMIT** - LIMIT can be used to limit the number of objects returned for a single search request.


## Geofencing

<img src="/doc/geofence.gif" width="200" height="200" border="0" alt="Geofence animation" align="left">
A <a href="https://en.wikipedia.org/wiki/Geo-fence">geofence</a> is a virtual boundary that can detect when an object enters or exits the area. This boundary can be a radius, bounding box, or a polygon. Tile38 can turn any standard search into a geofence monitor by adding the FENCE keyword to the search. 

*Tile38 also allows for [Webhooks](http://tile38.com/commands/sethook) to be assigned to Geofences.*

<br clear="all">

A simple example:
```
> nearby fleet fence point 33.462 -112.268 6000
```
This command opens a geofence that monitors the 'fleet' collection. The server will respond with:
```
{"ok":true,"live":true}
```
And the connection will be kept open. If any object enters or exits the 6 km radius around `33.462,-112.268` the server will respond in realtime with a message such as:

```
{"command":"set","detect":"enter","id":"truck02","object":{"type":"Point","coordinates":[-112.2695,33.4626]}}
```

The server will notify the client if the `command` is `del | set | drop`. 

- `del` notifies the client that an object has been deleted from the collection that is being fenced.
- `drop` notifies the client that the entire collection is dropped.
- `set` notifies the client that an object has been added or updated, and when it's position is detected by the fence.

The `detect` may be one of the following values.

- `inside` is when an object is inside the specified area.
- `outside` is when an object is outside the specified area.
- `enter` is when an object that **was not** previously in the fence has entered the area.
- `exit` is when an object that **was** previously in the fence has exited the area.
- `cross` is when an object that **was not** previously in the fence has entered **and** exited the area.

## Object types

All object types except for XYZ Tiles and QuadKeys can be stored in a collection. XYZ Tiles and QuadKeys are reserved for the SEARCH keyword only.

#### Lat/lon point
The most basic object type is a point that is composed of a latitude and a longitude. There is an optional `z` member that may be used for auxiliary data such as elevation or a timestamp.
```
set fleet truck1 point 33.5123 -112.2693     # plain lat/lon
set fleet truck1 point 33.5123 -112.2693 225 # lat/lon with z member
```

#### Bounding box
A bounding box consists of two points. The first being the southwestern most point and the second is the northeastern most point.
```
set fleet truck1 bounds 30 -110 40 -100
```
#### Geohash
A [geohash](https://en.wikipedia.org/wiki/Geohash) is a string respresentation of a point. With the length of the string indicating the precision of the point. 
```
set fleet truck1 hash 9tbnthxzr # this would be equivlent to 'point 33.5123 -112.2693'
```

#### GeoJSON
[GeoJSON](http://geojson.org/) is an industry standard format for representing a variety of object types including a point, multipoint, linestring, multilinestring, polygon, multipolygon, geometrycollection, feature, and featurecollection. Tile38 supports all of the standards with these exceptions.

1. The `crs` member is not supported and will be ignored. The CRS84/WGS84 projection is assumed.
2. Any member that is not recognized (including `crs`) will be ignored.
3. All coordinates can be 2 or 3 axes. Less than 2 axes or more than 3 will result in a parsing error.

<i>* All ignored members will not persist.</i>

**Important to note that all coordinates are in Longitude, Latitude order.**

```
set city tempe object {"type":"Polygon","coordinates":[[[0,0],[10,10],[10,0],[0,0]]]}
```

#### XYZ Tile
An XYZ tile is rectangle bounding area on earth that is represented by an X, Y coordinate and a Z (zoom) level.
Check out [maptiler.org](http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/) for an interactive example.

#### QuadKey
A QuadKey used the same coordinate system as an XYZ tile except that the string representation is a string characters composed of 0, 1, 2, or 3. For a detailed explanation checkout [The Bing Maps Tile System](https://msdn.microsoft.com/en-us/library/bb259689.aspx).


## Network protocols

It's recommended to use a [client library](#client-libraries) or the [Tile38 CLI](#running), but there are times when only HTTP is available or when you need to test from a remote terminal. In those cases we provide an HTTP and telnet options.

#### HTTP
One of the simplest ways to call a tile38 command is to use HTTP. From the command line you can use [curl](https://curl.haxx.se/). For example:

```
# call with request in the body
curl --data "set fleet truck3 point 33.4762 -112.10923" localhost:9851

# call with request in the url path
curl localhost:9851/set+fleet+truck3+point+33.4762+-112.10923
```

#### Websockets
Websockets can be used when you need to Geofence and keep the connection alive. It works just like the HTTP example above, with the exception that the connection stays alive and the data is sent from the server as text websocket messages.

#### Telnet
There is the option to use a plain telnet connection. The default output through telnet is [RESP](http://redis.io/topics/protocol).

```
telnet localhost 9851
set fleet truck3 point 33.4762 -112.10923
+OK

```

The server will respond in [JSON](http://json.org) or [RESP](http://redis.io/topics/protocol) depending on which protocol is used when initiating the first command.

- HTTP and Websockets use JSON. 
- Telnet and RESP clients use RESP.

## Client Libraries

Tile38 uses the [Redis RESP](http://redis.io/topics/protocol) protocol natively. Therefore most clients that support basic Redis commands will in turn support Tile38. Below are a few of the popular clients. 

- C: [hiredis](https://github.com/redis/hiredis)
- C#: [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)
- C++: [redox](https://github.com/hmartiro/redox)
- Clojure: [carmine](https://github.com/ptaoussanis/carmine)
- Common Lisp: [CL-Redis](https://github.com/vseloved/cl-redis)
- Erlang: [Eredis](https://github.com/wooga/eredis)
- Go: [go-redis](https://github.com/go-redis/redis) ([example code](https://github.com/tidwall/tile38/wiki/Go-example-(go-redis)))
- Go: [redigo](https://github.com/garyburd/redigo) ([example code](https://github.com/tidwall/tile38/wiki/Go-example-(redigo)))
- Haskell: [hedis](https://github.com/informatikr/hedis)
- Java: [lettuce](https://github.com/mp911de/lettuce) ([example code](https://github.com/tidwall/tile38/wiki/Java-example-(lettuce)))
- Node.js: [node-tile38](https://github.com/phulst/node-tile38) ([example code](https://github.com/tidwall/tile38/wiki/Node.js-example-(node-tile38)))
- Node.js: [node_redis](https://github.com/NodeRedis/node_redis) ([example code](https://github.com/tidwall/tile38/wiki/Node.js-example-(node-redis)))
- Perl: [perl-redis](https://github.com/PerlRedis/perl-redis)
- PHP: [phpredis](https://github.com/phpredis/phpredis)
- Python: [redis-py](https://github.com/andymccurdy/redis-py) ([example code](https://github.com/tidwall/tile38/wiki/Python-example))
- Ruby: [redic](https://github.com/amakawa/redic) ([example code](https://github.com/tidwall/tile38/wiki/Ruby-example-(redic)))
- Ruby: [redis-rb](https://github.com/redis/redis-rb) ([example code](https://github.com/tidwall/tile38/wiki/Ruby-example-(redis-rb)))
- Rust: [redis-rs](https://github.com/mitsuhiko/redis-rs)
- Scala: [scala-redis](https://github.com/debasishg/scala-redis)
- Swift: [Redbird](https://github.com/czechboy0/Redbird)

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Tile38 source code is available under the MIT [License](/LICENSE).
