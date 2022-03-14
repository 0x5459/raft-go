# Raft
Raft consensus algorithm implemented in Go.

----

## Example
### Run example server
```shell
# node 1
go run example/server/server.go -local="http://127.0.0.1:2000" -peers="http://127.0.0.1:2001" -peers="http://127.0.0.1:2002"
# node 2
go run example/server/server.go -local="http://127.0.0.1:2001" -peers="http://127.0.0.1:2000" -peers="http://127.0.0.1:2002"
# node 3
go run example/server/server.go -local="http://127.0.0.1:2002" -peers="http://127.0.0.1:2000" -peers="http://127.0.0.1:2001"
```

### Run client
#### Set key
```shell
go run .\example\client\client.go --url "http://127.0.0.1:2001" set hello raft
```
output:
```

--------------------
latency: <0.03 sec>
```

### Get key
```shell
go run .\example\client\client.go --url "http://127.0.0.1:2001" get hello
```
output:
```
raft

--------------------
latency: <0.00 sec>
```

### Remove key
```shell
go run .\example\client\client.go --url "http://127.0.0.1:2001" rm hello
```
output:
```

--------------------
latency: <0.00 sec>
```
