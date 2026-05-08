# Diagnostics Proxy

`casper-devnet start` runs a diagnostics proxy on `127.0.0.1:32000`. The proxy
forwards websocket or HTTP requests to each node's diagnostics Unix socket.

Node-1 endpoint:

```text
ws://127.0.0.1:32000/diagnostics/node-1/
http://127.0.0.1:32000/diagnostics/node-1/
```

The Docker image exposes port `32000` for this proxy.

## Websocket Usage

Use websocket mode for interactive diagnostics so a single connection can send
commands and receive responses:

```bash
wscat -c ws://127.0.0.1:32000/diagnostics/node-1/
```

## HTTP Usage

HTTP POST mode is useful for automation or environments where keeping a
websocket open is inconvenient. The response is newline-delimited JSON.

Set a failure point:

```bash
curl -v -XPOST --data 'stop --at block:250' http://127.0.0.1:32000/diagnostics/node-1/
```

Dump network info:

```bash
curl -v -XPOST --data 'net-info' http://127.0.0.1:32000/diagnostics/node-1/
```

Dump queues:

```bash
curl -v -XPOST --data 'dump-queues' http://127.0.0.1:32000/diagnostics/node-1/
```

## Related CLI

Print a random diagnostics socket path for a running node:

```bash
casper-devnet network casper-dev port --diagnostics
```

For other endpoint selectors, see the [CLI reference](cli-reference.md).
