<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# Operating a federated instance

How to turn federation on, peer with another instance, and check it is working.

**Status:** pads replicate. Two peered instances can share a pad, and at
conformance level L2 users on **both** can edit it concurrently and converge.
Outstanding: checkpoint dedup on federated pads, `TRIM`, `RECONCILE`, and the
operational layer (quotas, budgets, admin tooling). See
[`federation-status.md`](federation-status.md) for exactly what is built,
[`federation-design.md`](federation-design.md) §9 for the milestones and
[`federation-spec.md`](federation-spec.md) for what the protocol must guarantee.

## Trying it without configuring anything

There is a ready-made two-instance rig at
`/home/ludovic/dev/cryptpad/experiments/federation` — instance A on
`http://localhost:3000`, instance B on `http://localhost:4000`, already peered
with each other. `node setup.js` builds it, `./start.sh` runs it. See its README.
The rest of this document is for setting federation up on a real instance.

## Federation is off by default

An instance with no federation nodes opens no federation listener, generates no
instance key, and behaves exactly as it did before the feature existed. That is
the default, and it is what `infra.federation: []` means.

## Turning it on

### 1. Add a federation node

In `config/infra.js`:

```js
"federation": [
    {
        url: "",
        host: "localhost",
        port: 3050,
        serverId: '',
        // omit only if a proxy in front of this port terminates TLS
        tls: { cert: '/etc/letsencrypt/live/example/fullchain.pem',
               key:  '/etc/letsencrypt/live/example/privkey.pem' }
    }
]
```

Then `npm run build && npm start`. Peer sessions terminate on that port at the
path `/federation`. Only that port needs to be reachable by peers — never by
browsers.

**TLS is not optional in production** (spec R-24). The listener will start
without it, and will log `FEDERATION_NO_TLS` when it does, but the handshake only
authenticates the *instance key*: confidentiality and integrity of the session
are TLS's job. Terminate it here or in a proxy directly in front.

### 2. Find your instance key

On first start the federation node generates a persistent ed25519 keypair in
`data/identity.json` and logs:

```
FEDERATION_IDENTITY_CREATED {"originId":"lBauvBdWnxh...","path":"data/identity.json"}
```

That `originId` is your instance's name on the wire. It is also published at:

```sh
curl https://your-instance.example/api/federation
```
```json
{
    "federation": true,
    "version": 1,
    "originId": "lBauvBdWnxh59iq422PCoSzvwEv808wdjLZ/FYUwCAw=",
    "origin": "https://your-instance.example"
}
```

An instance that does not federate answers `404 {"federation": false}`.

> **Back up `data/identity.json`, and never copy it to a second instance.** It is
> your instance's identity: lose it and every peer must re-add you; share it and
> two servers claim to be the same instance. It is deliberately kept outside the
> object storage backend so that resharding storage cannot move or duplicate it.

### 3. List your peers

Peering is an allowlist, and it is mutual: **both** operators must add the other
before anything connects. Create `data/peers.json`:

```json
{
    "version": 1,
    "peers": [
        {
            "originId": "<the other instance's originId>",
            "url": "wss://other.example/federation",
            "name": "other.example",
            "dial": true
        }
    ]
}
```

* `originId` — required, and the only thing that identifies a peer. Get it from
  their `/api/federation`.
* `url` — where to dial them. Omit it to accept their connections without ever
  dialling out; a peer with no `url` is inbound-only.
* `dial` — set `false` to keep the entry but stop dialling.

Peers are named by key, never by hostname, so an attacker who takes over a peer's
DNS or certificate still cannot pass the handshake.

A missing `peers.json` is not an error — it is how an instance says "I federate
with nobody", which stays the easy default.

## Checking it works

Both sides log when a session comes up, then ping each other every 2 seconds:

```
FEDERATION_SESSION_UP  {"peer":"other.example","role":"dialler"}
FEDERATION_PING_OK     {"peer":"other.example","rtt":31,"storage":true}
```

`storage: true` is the useful part: the ping travels the peer's federation node →
its core → its storage node and back, so it says the far instance can still reach
its own storage tier, not merely that a socket is open.

## When it does not

Every refusal is logged with a specific reason rather than a dropped socket.

| Log / error | Meaning |
| --- | --- |
| `FEDERATION_REFUSED`, `ENOTPEERED` | The peer proved its key, but it is not in your `peers.json`. Usually one side has not added the other yet. |
| `EWRONGPEER` | You dialled a URL expecting one instance and a different key answered. Check the `url`/`originId` pairing; do not "fix" it by changing the key you expect. |
| `EBADSIG` | The handshake proof did not verify. A replayed or tampered handshake. |
| `ECLOCKSKEW` | The two clocks differ by more than 5 minutes. Run NTP. |
| `EHANDSHAKETIMEOUT` | The peer connected and did not finish the handshake within 20s. |
| `FEDERATION_POLICY_BAD_PEER` | An entry in `peers.json` has a malformed `originId`; that entry is ignored, the rest still load. |
| `FEDERATION_NO_TLS` | The listener is running in plaintext. Fine for a local test, not for production. |

Dialling backs off from 2s to a 5-minute ceiling with jitter, so a peer that is
down does not need intervention — it reconnects when it returns.

## Scaling

Add more entries to `infra.federation` to add capacity. Each peer is owned by
exactly one federation node, chosen by a jump hash over the peer's `originId`, so
sessions are not duplicated across nodes. If both instances dial each other at
once, both ends independently keep the same one of the two sessions and drop the
other.
