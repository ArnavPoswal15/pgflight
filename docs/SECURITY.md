# Security notes (local Query Flight Recorder)

**Control socket.** `~/.pgflight/control.sock` is created mode **0600** under a **0700** directory. Subscribe streams canonical SQL. Treat the socket path like a credential. The control socket is never bound to the network.

**Peer gate.** On each accept the daemon reads the client UID with `getpeereid` (macOS) or `SO_PEERCRED` (Linux). By default only the daemon's own UID may connect. `--control-peer-uid <uid>` allows one extra local user. `--allow-any-control-peer` (alias `--allow-any-peer`) skips the check. A mismatch is logged and the connection is closed. Mode **0600** remains the primary boundary; the peer check is defense in depth if the path is reachable.

**Shadow URL.** Point `--shadow-url` at the dedicated SELECT-only shadow role on `:5434`, never at the primary writer.
