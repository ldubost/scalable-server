module.exports = {
    "public": {
        // Public URL of the instance (used for Content-Security-Policy)
        origin: "http://localhost:3000",
        // Sandbox URL of the instance
        sandboxOrigin: "http://localhost:3001",
        // Address and port of the nodejs HTTP server
        httpHost: "localhost",
        httpPort: 3000,
        httpSafePort: 3001,
        // (Optional) API server URL if hosted on a different domain (ws and http)
        externalWebsocketURL: undefined,
        fileHost: undefined,
        // (Optional) httpServerId only useful for multi-server cases
        httpServerId: '',
    },
    // Configure the topology here. Add or remove nodes on each level
    // depending on your instance usage.
    // "host" and "port" correspond to the nodejs HTTP server of each node
    // "url" can be set if your nodes are on different machines. They must
    // be able to reach each other from this URL
    // "serverId" is optional and is only useful for multi-server cases
    "front": [
        {
            url: "", // e.g. "https://node1.my-cryptpad-domain.net"
            host: "localhost",
            port: 3010, // Public http and websocket port
            serverId: ''
        },
        {
            url: "", // "https://node2.my-cryptpad-domain.net"
            host: "localhost",
            port: 3011,
            serverId: ''
        }
    ],
    "core": [
        {
            url: "",
            host: "localhost",
            port: 3020, // Internal websocket betwene all nodes
            serverId: ''
        },
        {
            url: "",
            host: "localhost",
            port: 3021,
            serverId: ''
        }
    ],
    "storage": [
        {
            url: "",
            host: "localhost",
            port: 3030, // Public port to serve "blob", "block", etc.
            wsPort: 3040, // Internal websocket between storage nodes
            serverId: ''
        },
        {
            url: "",
            host: "localhost",
            port: 3031,
            wsPort: 3041,
            serverId: ''
        }
    ],

    /*  CryptPad-to-CryptPad federation (docs/federation-design.md).

        Leave empty to disable federation entirely — no listener is opened and
        the instance behaves exactly as it did before the feature existed. Peers
        are listed separately, by instance public key, in data/peers.json.

        Each node terminates peer sessions on `port` at the path /federation.
        Give it TLS in production, either here or with a terminating proxy:

            {
                url: "",
                host: "localhost",
                port: 3050,
                serverId: '',
                tls: { cert: '/path/fullchain.pem', key: '/path/privkey.pem' }
            }
    */
    "federation": []
};
