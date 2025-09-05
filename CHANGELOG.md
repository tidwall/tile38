# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.36.2] = 2025-09-04
### Fixed
- 653aea6: Add GeoJSON "properties" member for Lua filtering
- 70c244f: Make the String.match() function case insensitive

## [1.36.1] = 2025-08-28
### Updated
- 53bed30: Update lock strategy for faster writes 
- 40f58b0: Kafka Endpoint Improvement #778 (@ifiok)
- 82f4d24: Smaller vector tile sizes for polygons
- 5b21c24: Auto clip geometries at low zoom levels

## [1.36.0] = 2025-07-14
### Added
- 35bde95: Support for Cloudflare Queues endpoint #773 (@tobilg)
- a2afb21: Vector tile support (Mapbox vector tiles) #775

### Updated
- 56c70a1: Bumped github.com/golang-jwt/jwt/v4
- 86b698d: Updated Go dependencies
- f95fcab: Go 1.25

## [1.35.0] = 2025-06-16
### Added
- 4638279: Added NATS Jetstream acks, user credentials, and tls #770 (@VeryStrongFingers)

### Updated
- 3fe57a7; Use atomics for HEALTHZ command to remove contention.
- 3085316: Expose the 'this' property to WHERE clauses.

## [1.34.4] = 2025-05-08
### Fixed
- 977bf25: Fix issue with some startup flags not being read (@salilgupta1)

### Updated
- 86333cd: Bump golang.org/x/net #767

## [1.34.3] = 2025-04-16
### Fixed
- dd98481: Fix channel message delay #763 (@txtsd)

## [1.34.2] = 2025-04-01
### Updated
- a80eaf2: Upgrade to Go 1.24

### Fixed
- 556390e: Fix equality tests with WHERE clause for nested values
- 42e17a1: Fix channel test that sometimes stalls

### Security
- 0ecf097: Bump golang.org/x/net #765

## [1.34.1] = 2025-01-13
### Security
- 927f382: CVE-2024-45338 golang.org/x/net #762 (@tduong2049)
- 07389d8: CVE-2024-45337 golang.org/x/crypto #760

## [1.34.0] = 2024-12-09
### Added 
- 459b3e6: Added fifo support for SQS webhooks #759 (@crankycookie)

### Updated
- bed590b: Upgrade to Alpine 3.20 #757 (@tduong2049)
- 2b09508: Upgrade to Go 1.23

## [1.33.4] = 2024-11-05
### Fixed
- aa1caa6: Use zero for undefined fields in where expressions #754 (@unendingblue)

## [1.33.3] = 2024-09-29
### Fixed
- 2b080f4: Include field to INFO replication command output #752 (@Kilowhisky)

## [1.33.2] = 2024-08-02
### Fixed
- 2e3eaa7: Remove extra quote in ROLE command with JSON output #749 (@Kilowhisky)

## [1.33.1] = 2024-07-02
### Fixed
- 193bce1: Fix followers not receiving channel messages #468 (@hibooboo2, @trendstate, @DoisKoh)

## [1.33.0] = 2024-05-03
## Added
- #726: Add EXIST and FEXIST command (@Kilowhisky)

## Fixed
- #738: Add support for CORS in http requests (@Kilowhisky)
- #741: FSET transforms field names to lowercase (@unendingblue, @iwpnd)
- #736: Fix field floating point parsing misrepresentation (@Kilowhisky)

## Updated
- #733: golang.org/x/net
- #724: google.golang.org/protobuf

## [1.32.2] = 2024-02-14
### Fixed
- #714: Fix crash when mixing z-coord dimensionality in a geometry (@prathik)
- #717: Metric expired_keys never incremented (@undeadcat)
- Updated Go runtime to 1.22

## [1.32.1] = 2023-11-20
### Fixed
- #711: Updated dependencies to address security vulnerabilities (@hcmf-wice)
- #706: Add support for 'none' authentication for kafka while still allowing SSL (@Kilowhisky)
- #702: Fix AWS SQS base domain parsing for China region (@LLluma)

## [1.32.0] = 2023-07-31
### Added
- #686: Support the ROLE command (@Kilowhisky)

### Fixed
- #698: Allow AUTH while loading data (@Kilowhisky)
- #694: Allow PING in pubsub (@Kilowhisky)
- #692: Properly support replica_announce properties (@Kilowhisky)
- #691: HEALTHZ should not be AUTH protected (@Kilowhisky)
- #685: Heap size not coming down after objects are removed (@Mukund2900, @iwpnd)
- 0144ca6: Fix missing lock


## [1.31.0] = 2023-05-09
### Added
- #682: Enables cross platform building and pushing of docker images (arm64/amd64) (@eelcocramer)
- #680: Add hostname, port, output, and password env variables to tile38-cli (@ptsilva)

### Fixed
- #606: Only create AMQP queue and bindings for non-topic exchanges (@pacaj2am, @uwer)
- #672: Add graceful shutdown on SIGTERM (@dmitri-zganiaiko)

### Updated
- e9a0500: Upgrade to Go 1.20
- 05b2fb9: Security updates

## [1.30.2] = 2022-12-29
### Fixed
- #668: Fixed fields not persisting (@DucPhan2997)

## [1.30.1] = 2022-12-14
### Fixed
- a8c92a0: Speed up leader/follower replication
- e60ea70: Fix field names converting to lowercase

## [1.30.0] = 2022-11-22
### Added
- bdc80a7: Add WHERE expressions ([more info](https://tile38.com/topics/filter-expressions))
- f24c251: Allow for multiple MATCH patterns
- #652: Allow WHERE for geofence detection
- #657: Add distance to NEARBY IDS response (@iwpnd)
- #663: Lua Sanitization (@program--)

### Fixed
- 023433a: Fix server hang on shared address
- #655: fix: allow host ca sets for SASL and TLS connections (@iwpnd)

### Updated
- 7f2ce23: Upgrade to Go 1.19
- cbfb271: Updated data structures to use Go generics

## [1.29.2] = 2022-11-11
### Fixed
- #664: Fix bad line in inner ring response

## [1.29.1] = 2022-09-21
### Fixed
- fe180dc: Fix follower not authenticating after aofshink

## [1.29.0] = 2022-07-14
### Added
- b883f35: Add pending_events stat
- #643: Expose config and INFO response for replia-priorty (@rave-eserating)

### Fixed
- 8e61f81: Fixed test on Apple silicon

## [1.28.0] = 2022-04-12
### Added
- 10f8564: Added option to "not found" for DEL
- #633: Added "clear" command in the tile38-cli (@CaioDallaqua)
- #634: Added -x flag to tile38-cli (@sign0)

### Fixed
- #636: Workaround for the RESP3 Java lettuce client (@rave-eserating)
- a1cc8e6: Fix eof error for incomplete commands (Theresa D)

### Updated
- fcdb469: Security updates
- #638: Upgrade alpine in Dockerfile (@bb)
- a124738: Upgrade to Go 1.18
- 38ea913: Upgrade prometheous client
- 45fde6a: Upgraded nats dependencies

## [1.27.1] = 2021-01-04
### Fix
- b6833a2: Auto assign server_id for bootstrapped config files

## [1.27.0] = 2021-12-28
### Added
- #629: JSON logging (@iwpnd)
- 241117c: BUFFER option for WITHIN and INTERSECTS, see #79

## [1.26.4] = 2021-10-25
### Hotfix
- a7592f7: Bump version to match changelog

## [1.26.3] = 2021-10-25
### Updated
- a47443a: Upgrade tidwall modules

## [1.26.2] = 2021-10-22
### Added
- #625: Azure EventHub hook support

### Changed
- 11cea4d: Removed vendor directory

## [1.26.1] = 2021-10-01
### Updated
- 9e552c3: Allow some basic client commands before AOF data loads

## [1.26.0] = 2021-09-29
### Added
- #623: Added SECTOR type to spatial searches (@iwpnd, @gmonk)

### Fixed
- #624: AOFSHRINK causes panic on server (@saques)

## [1.25.5] = 2021-09-26
### Fixed
- 8ebcbeb: Fixed Z not matching on where clause for Feature/Point. (@tomquas)

## [1.25.4] = 2021-09-14
### Added
- a737a78: Add unix socket support

### Updated
- 8829b8f: Change hooks collection type from hashmap to btree
- 83094b2: Update hook expiration logic
- c686b87: Return hook ttl with HOOKS request
- 06a92d8: Increase the precision of TIMEOUT
- Upgrade to Go 1.17.1

## [1.25.3] = 2021-08-23
### Fixed
- #621: Fixed a memory leak (@Morgiflute)

### Updated
- Update B-tree library
- Upgrade to Go 1.17

## [1.25.2] = 2021-08-10
### Fixed
- #620: Fixed kafka authentication methods

### Updated
- Upgraded various dependencies

## [1.25.1] = 2021-07-22
### Fixed
- #618: Fixed NEARBY with SPARSE returning too many results. (@nesjett)

## [1.25.0] = 2021-07-12
### Added
- #504: Added TLS support for Nats webhook provider.
- #552: Add CLIPBY subcommand to INTERSECTS/WITHIN. (@rshura)
- #561: Added geofence webhook for GCP Pubsub. (@mscno)
- #615: Add SASL to Kafka provider. (@mathieux51, @iwpnd)

### Updated
- #551: Optimize field value access. (@mpoindexter)
- #554: Improved kNN using geodesic algorithm for NEARBY command. (@mpoindexter)

### Fixed
- #611: Close follower files before finishing aofshrink. (@mzbrau)
- #613: Fix Memory Leak in Kafka Producer. (@iwpnd)
- #616: Fixed expiration logic issue. (@Neuintown)

## [1.24.3] = 2021-06-09
### Fixed
- af43d5a: Hotfix. Fixed invalid healthz output.

## [1.24.2] = 2021-06-07
### Updated
- b610633: Update Go to 1.16

## [1.24.1] = 2021-06-07
### Added
- #609: Added HEALTHZ command (@iwpnd, @stevelacy)

## [1.24.0] = 2021-05-19
### Added
- #604: Added Prometheus metrics (@oliver006)

### Fixed
- #605: Remove deprecated threads flag (@cep-ter) 

## [1.23.0] = 2021-04-01
### Updated
- #598: Added TLS Config to Kafka (@iwpnd)
- #599: Include "distance" to output when user specifically requests (@iwpnd)
- #597: Allow for all command types for roaming event (@johnpmayer)
- 31a0fbd: Upgraded dependencies and moved to Go 1.16

### Fixed
- #600: Fix invalid queue.db error (@lokisisland)
- #603: Fix tile38-cli output showing protocol size when piping (@bb)

## [1.22.6] = 2021-02-07
### Updated
- 72dfaae: Updated various dependencies
- 016f397: Updated btree library, optimization 
- 4f8bc05: Updated rtree library, optimization

### Fixed
- 6092f73: Better handle connection errors in tile38-cli

## [1.22.5] = 2020-11-09
### Fixed
- 9ce2033: Fixed fields being shuffled after AOFSHRINK

## [1.22.4] = 2020-11-07
### Updated
- 1a7d8d6: Added ENV var for 500 http errors

## [1.22.3] = 2020-10-28
### Updated
- #583: Optimization for non-"cross" based geofence detection (@cliedeman)
- 79bee85: Replaced the underlying B-tree structure.

## [1.22.2] = 2020-10-07
### Fixed
- #230: Fix trailing zeros in AOF at startup

## [1.22.1] = 2020-09-22
### Updated
- 9a34a37: Updated Go version to 1.15
- b1dc463: Updated outdated dependencies (40 in total)

### Added
- #578 Fix "cross" detection not firing in some cases (@feichler-or)

## [1.22.0] = 2020-08-12
### Added
- #571 Added MONITOR command (@tomquas)

### Fixed
- #566: Fixed crash in fenceMatchRoam causing an index out of range panic (@larsw)
- #569: Fixed wrong order for fields with SCAN (@ipsusila)
- #573: Fixed crash with geohash precision above 12 (@superloach)
- 68e2b6d: Updated Kafka client to support (@LeonardoBonacci)

## [1.21.1] = 2020-06-04
### Fixed
- #564: Fix OUTPUT client command requiring authentication. (@LeonardoBonacci)

## [1.20.0] = 2020-05-20
### Updated
- #534: Avoid sorting fields for each written object. (@rshura)
- #544: Match geometry indexing to server config
- b3dc025: Optimize point in ring
- 3718cd7: Added priority option for AMQP endpoints

### Fixed
- #538: DEL geofence notifications are missing the "key" field
- #539: Fixed issue with some features not working with WITHIN (@rshura)
- #540: Fix a concurrent write/read on the server conn map (@mpoindexter)
- #543: Fix clipping empty rings (@rshura)
- #558: Fixed clip test (@mmcloughlin)
- #562: Crashes under go1.14 runtime
- ff48054: Fixed a missing faraway event for roaming geofences
- 5162ac5: Stable sort roam notifications

## [1.19.5] = 2020-02-11
### Fixed
- c567512: Fix packages not vendoring on build

## [1.19.4] = 2020-02-10
### Fixed
- #529: Fix linestring features behave diffrent with CIRCLE (@spierepf)

## [1.19.3] = 2019-12-11
### Fixed
- #513: Fix tile38-cli from freezing with non-quoted geojson (@duartejc)

## [1.19.2] = 2019-11-28
### Fixed
- 6f3716a: Fix false negative for intersecting rings (@thomascoquet)

## [1.19.1] = 2019-11-18
### Updated
- cfc65a1: Refactored repo, moved to Go modules, updated vendor dependencies.

### Fixed
- 9d27533: Fix infinite loop on tile38-cli connection failure.
- #509: Fixed panic on AOFSHRINK. (@jordanferenz)

## [1.19.0] = 2019-11-02
### Added
- #464: Add area expressions TEST command. (@rshura)

### Fixed
- #493: Fix invalid JSON when JSET strings that look like numbers. (@spierepf, @JordanArmstrong)
- #499: Fix invalid PubSub format when output is set to JSON. (@dmvass)
- #500: Fix Tile38-cli not propertly handling quotes. (@vthorsell)
- #502: Fix excessive memory usage for objects with TTLs. commit 23b016d. (@FreakyBytes)
- #503: Fix fprintf type error in stats_cpu.go for non-linux/darwin builds. (@JordanArmstrong)

### Changed
- #505: Update Travi-ci to use Go 1.13.x

## [1.18.0] = 2019-10-09
### Updated
- 639f6e2: Updated the spatial index (R-tree) implementation.

### Fixed
- b092cea: Fixed MQTT blocking on publish/wait.
- #496: Fixed MQTT client ID uniqueness. (@neterror)
- #497: Fixed data race on webhook map with TTLs. (@belek)
- #498: Fixed JSET cancels objects TTL expiry value. (@belek)

## [1.17.6] - 2019-08-22
### Fixed
- 3d96b17: Fixed periodic stop-the-world pauses for systems with large heaps.

## [1.17.5] - 2019-08-22
### Fixed
- #489: Fixed nearby count always one (@jkarjala)

## [1.17.4] - 2019-08-09
### Fixed
- #486: Fixed data condition on connections map (@saltatory)

## [1.17.3] - 2019-08-03
### Fixed
- #483: Fixed lua pool pruning (@rshura)
- f7888c1: Fixed malformed json for chans command

## [1.17.2] - 2019-06-28
### Fixed
- #422: Fixes NEARBY command distance normalization issue (@TrivikrAm-Pamarthi, @melbania)

## [1.17.1] - 2019-05-04
### Fixed
- #448: Fixed missing commands for unsubscribing from active channel (@githubfr)
- #454: Fixed colored output for fatalf (@olevole)
- #453: Fixed nearby json field results showing wrong data (@melbania)

## [1.17.0] - 2019-04-26
### Added
- #446: Added timeouts to allow prepending commands with a TIMEOUT option. (@rshura)

### Fixed
- #440: Fixed crash with fence ROAM (@githubfr)

### Changed
- 3ae5927: Removed experimental evio option

## [1.16.4] - 2019-03-19
### Fixed
- e1a7145: Hotfix. Do not ignore SIGHUP. Use the `--nohup` flag or `nohup` command.

## [1.16.3] - 2019-03-19
### Fixed
- #437: Fixed clients blocking while webook sending. (@tesujiro)

### Added
- #430: Support more SQS credential providers. (@tobilg)
- #435: Added pprof flags for optional memory and cpu diagnostics.
- e47540b: Added auth flag to tile38-benchmark.
- 5335aec: Allow for standard SQS URLs. (@tobilg)

## [1.16.2] - 2019-03-12
### Fixed
- #432: Ignore SIGHUP signals. (@abhit011)
- #433: Fixed nearby inaccuracy with geofence. (@stcktrce)
- #429: Memory optimization, recycle AOF buffer.
- 95a5556: Added periodic yielding to iterators. (@rshura)

## [1.16.1] - 2019-03-01
### Fixed
- #421: Nearby with MATCH is returning invalid results (@nithinkota)

## [1.16.0] - 2019-02-25
### Fixed
- #415: Fixed overlapping geofences sending notifcation to wrong endpoint. (@belek, @s32x)
- #412: Allow SERVER command for Lua scripts. (@1995parham)
- #410: Allow slashes in MQTT Topics (@pstuifzand)
- #409: Fixed bug in polygon clipping. (@rshura)
- 30f903b: Require properties member for geojson features. (@rshura)

### Added
- #409: Added TEST command for executing WITHIN and INTERSECTS on two objects. (@rshura)
- #407: Allow 201 & 202 status code on webhooks. (@s32x)
- #404: Adding more replication data to INFO response. (@s32x)

## [1.15.0] - 2019-01-16
### Fixed
- #403: JSON Output for INFO and CLIENT (@s32x)
- #401: Fixing KEYS command (@s32x)
- #398: Ensuring channel publish order (@s32x)
- d7d0baa: Fix roam fence missing

### Added
- #402: Adding ARM and ARM64 packages (@s32x)
- #399: Add RequireValid and update geojson dependency (@stevelacy)
- #396: Add distance_to function to the tile38 namespace in lua. (@rshura)
- #395: Add RENAME and RENAMENX commands. (@rshura)

## [1.14.4] - 2018-12-03
### Fixed
- #394: Hotfix MultiPolygon intersect failure. (@contra)
- #392: Fix TLS certs missing in Docker. (@vziukas, @s32x)

### Added
- Add extended server stats with SERVER EXT. (@s32x)
- Add Kafka key to match notication key. (@Joey92)
- Add optimized spatial index for fences

## [1.14.3] - 2018-11-20
### Fixed
- Hotfix SCRIPT LOAD not executing from cli. (@rshura)

## [1.14.2] - 2018-11-15
### Fixed
- #386: Fix version not being set at build. (@stevelacy)

## [1.14.1] - 2018-11-15
### Fixed
- #385: Add `version` to SERVER command response (@stevelacy)
- Hotfix replica sync needs flushing (@rshura)
- Fixed a bug where some AOF commands where corrupted during reload

## [1.14.0] - 2018-11-11
### Added
- INTERSECT/WITHIN optimization that may drastically improve searching polygons that have lots of points.
- Faster responses for write operations such as SET/DEL
- NEARBY now always returns objects from nearest to farthest (@rshura)
- kNN haversine distance optimization (@rshura)
- Evio networking beta using the "-evio yes" and "-threads num" flags

### Fixed
- #369: Fix poly in hole query

## [1.13.0] - 2018-08-29
### Added
- eef5f3c: Add geofence notifications over pub/sub channels
- 3a6f366: Add NODWELL keyword to roaming geofences
- #343: Add Nats endpoints (@lennycampino)
- #340: Add MQTT tls/cert options (@tobilg)
- #314: Add CLIP subcommand to INTERSECTS (@rshura)

### Changed
- 3ae26e3: Updated B-tree implementation
- 1d78a41: Updated R-tree implementation

## [1.12.3] - 2018-06-16
### Fixed
- #316: Fix AMQP and AMQPS webhook endpoints to support namespaces (@DeadWisdom)
- #318: Fix aofshrink crash on windows (@abhit011)
- #326: Fix sporadic kNN results when TTL is used (@pmaseberg)

## [1.12.2] - 2018-05-10
### Fixed
- #313: Hotfix intersect returning incorrect results (@stevelacy)

## [1.12.1] - 2018-04-30
### Fixed
- #300: Fix pdelhooks not persisting (@tobilg)
- #293: Fix kafka lockup issue (@Joey92)
- #301: Fix AMQP uri custom params not working (@tobilg)
- #302: Fix tile with zoom level over 63 panics (@rshura)
- b99cd39: Fix Sync hook msg ttl with server time

## [1.12.0] - 2018-04-12
### Added
- 11b42c0: Option to disable AOF or to use a custom path: #220 #223 #297 (@sign0, @umpc, @fmr683, @zhangfeng158)
- #296: Add Meta data to hooks command (@tobilg)

### Changed
- 11b42c0: Updated help menu and show more options

### Fixed
- #295: Intersects returning nothing in some cases (@fils)
- #294: HTTP requests stopped working (@zhangfeng158)
- 0aa04a1: Lotsa package not vendored

## [1.11.1] - 2018-03-16
### Added
- #272: Preserve Docker image tag history (@gechr)
- 9428b84: Added cpu and threads to SERVER stats

### Fixed
- #281: Linestring intersection failure (@contra)
- #280: Filter id match before kNN results (@sweco-semtne)
- #269: Safe atomic ints for arm32 (@gmonk63)
- #267: Optimization for multiploygons intersect queries (@contra)

## [1.11.0] - 2018-03-05
### Added
- #221: Add WHEREEVAL clause to scan/search commands (@rshura)

### Fixed
- #254: Add maxmemory protection to FSET (@rshura)
- #258: Clear expires on reset (@zycbobby)
- #268: Avoid bbox intersect for non-bbox objects (@contra)

## [1.10.1] - 2018-01-17
### Fixed
- #244: Fix issue with points not being detected inside MultiPolygons (@fazlul3003)
- #245: Precalculate and store bboxes for complex objects (@huangpeizhi)
- #246: Fix server crash when receiving zero arg commands (@behrad)

## [1.10.0] - 2017-12-18
### Added
- #221: Sqs endpoint (@lennycampino)
- #226: Lua scripting (@rshura) 
- #231: Allow setting multiple fields in a single fset command (@rshura)
- #235: Add json library (encode/decode methods) to lua. (@rshura)
- 26d0083: Update vendoring to use golang/dep 
- c8ed7ca: Add WHEREIN command (@rshura)
- d817814: Optimized network pipelining

### Fixed
- #237: Flush to file periodically (@rshura)
- #241: Point match on interior hole (@genesor)
- 920dc3a: Use atomic ints/bools
- 730502d: Set keepalive default to 300 seconds
- 1084c60: Apply limit on top of cursor (@rshura)

## [1.9.1] - 2017-08-16
### Added
- cd05708: Spatial index optimizations
- #208: Debug message for failed webhook notifications (@karnivas)
- #201: New ECHO command (@yorkxiao)
- #183: Include tile38-cli in Docker image (@jchamberlain)
- #121: Allow reads for disconnected followers (@octete)

### Fixed
- 3fae3f7: Allow cursors for kNN queries
- #211: Crash when shrinking AOF on Windows (@icewukong)
- #203: Lifted LIMIT restriction all queries and COUNT keyword (@yorkxiao, @FX-HAO)
- #207: Send empty results for queries on nonexistent keys (@FX-HAO)
- #195: Added kNN overscan ordering (@rshura)
- #199: Apply LIMIT after WHERE clause (@rshura)
- #199: Require Go 1.7 (@rshura)
- #198: Omit fields for Resp when NOFIELDS is used (@rshura)

## [1.9.0] - 2017-04-13
### Added
- #159: AMQP/RabbitMQ webhook support (@m1ome, @paavalan)
- #152: Kafka webhook support (@m1ome)
- #141: Add distances to Geofence notifications
- #54: New benchmark tool (@literadix, @Lars-Meijer, @m1ome)
- #20: Ability to specify pidfile via args (@olevole)

### Fixed
- b1c76d7: tile38-cli auto doesn't auto reconnect
- #156: Use redis-style TTL implementation (@Lars-Meijer, @m1ome)
- #150: Live "inside" fence event not triggering for new object (@phulst)

## [1.8.0] - 2017-02-21
### Added
- #145: TCP Keepalives option (@UriHendler)
- #136: K nearest neighbors for NEARBY command (@m1ome, @tomquas, @joernroeder)
- #139: Added CLIENT command (@UriHendler)
- #133: AutoGC config option (@m1ome, @amorskoy)

### Fixed
- #147: Leaking http hook connections (@mkabischev)
- #143: Duplicate data in hook data (@mkabischev)

## [1.7.5] - 2017-01-13
### Added
- Performance bump for all SET commands, ~10% faster
- Lower memory footprint for large datasets
- #112: Added distance to NEARBY command (@m1ome, @auselen)
- #123: Redis endpoint for webhooks (@m1ome)
- #128: Allow disabling HTTP & WebSocket transport (@m1ome)

### Fixed
- #116: Missing response in TTL json command (@phulst)
- #117: Error in command documentation (@juanpabloaj)
- #118: Unexpected EOF bug with websockets (@m1ome)
- #122: Disque typo timeout handling (@m1ome)
- #127: 3d object searches with 2d geojson area (@damariei)

## [1.7.0] - 2016-12-29
### Added
- #104: PDEL command - Selete objects that match a pattern (@GameFreedom)
- #99: COMMAND keyword for masking geofences by command type (@amorskoy)
- #96: SCAN keyword for roaming geofences
- fba34a9: JSET, JGET, JDEL commands

### Fixed
- #107: Memory leak (@amorskoy)
- #98: Output json fix

## [1.6.0] - 2016-12-11
### Added
- #87: Fencing event grouping (@huangpeizhi)

### Fixed
- #91: Wrong winding order for CirclePolygon function (@antonioromano)
- #73: Corruption for AOFSHRINK (@huangpeizhi)
- #71: Lower memory usage. About 25% savings (@thisisaaronland, @umpc)
- Polygon raycast bug. tidwall/poly#1 (@drewlesueur)
- Added black-box testing

## [1.5.4] - 2016-11-17
### Fixed
- #84: Hotfix - roaming fence deadlock (@tomquas)

## [1.5.3] - 2016-11-16
### Added
- #4: Official docker support (@gordysc)

### Fixed
- #77: NX/XX bug (@damariei)
- #76: Match on prefix star (@GameFreedom, @icewukong)
- #82: Allow for precise search for strings (@GameFreedom)
- #83: Faster congruent modulo for points (@icewukong, @umpc)

## [1.5.2] - 2016-10-20
### Fixed
- #70: Invalid results for INTERSECTS query (@thisisaaronland)

## [1.5.1] - 2016-10-19
### Fixed
- #67: Call the EXPIRE command hangs the server (@PapaStifflera)
- #64: Missing points in 'Nearby' queries (@umpc)

## [1.5.0] - 2016-10-03
### Added
- #61: Optimized queries on 3d objects (@damariei)
- #60: Added [NX|XX] keywords to SET command (@damariei)
- #29: Generalized hook interface (@jeremytregunna)
- GRPC geofence hook support 

### Fixed
- #62: Potential Replace Bug Corrupting the Index (@umpc)
- #57: CRLF codes in info after bump from 1.3.0 to 1.4.2 (@olevole)

## [1.4.2] - 2016-08-26
### Fixed
- #49. Allow fragmented pipeline requests (@owaaa)
- #51: Allow multispace delim in native proto (@huangpeizhi)
- #50: MATCH with slashes (@huangpeizhi)
- #43: Linestring nearby search correction (@owaaa)

## [1.4.1] - 2016-08-26
### Added
- #34: Added "BOUNDS key" command (@icewukong)

### Fixed
- #38: Allow for nginx support (@GameFreedom)
- #39: Reset requirepass (@GameFreedom)

## [1.3.0] - 2016-07-22
### Added
- New EXPIRE, PERSISTS, TTL commands. New EX keyword to SET command
- Support for plain strings using `SET ... STRING value.` syntax
- New SEARCH command for finding strings
- Scans can now order descending

### Fixed
- #28: fix windows cli issue (@zhangkaizhao)

## [1.2.0] - 2016-05-24
### Added
- #17: Roaming Geofences for NEARBY command (@ElectroCamel, @davidxv)
- #15: maxmemory config setting (@jrots)

## [1.1.4] - 2016-04-19
### Fixed
- #12: Issue where a newline was being added to HTTP POST requests (@davidxv)
- #13: OBJECT keyword not accepted for WITHIN command (@ray93)
- Panic on missing key for search requests

## [1.1.2] - 2016-04-12
### Fixed
- A glob suffix wildcard can result in extra hits
- The native live geofence sometimes fails connections

## [1.1.0] - 2016-04-02
### Added
- Resp client support. All major programming languages now supported
- Added WITHFIELDS option to GET
- Added OUTPUT command to allow for outputing JSON when using RESP
- Added DETECT option to geofences

### Changed
- New AOF file structure.
- Quicker and safer AOFSHRINK.

### Deprecation Warning
- Native protocol support is being deprecated in a future release in favor of RESP
