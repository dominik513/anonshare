const home = document.getElementById("home");
const room = document.getElementById("room");

const createBtn = document.getElementById("createBtn");
const joinBtn = document.getElementById("joinBtn");

const roomInput = document.getElementById("roomInput");
const link = document.getElementById("link");
const copyBtn = document.getElementById("copyBtn");

const status = document.getElementById("status");
const fileInput = document.getElementById("fileInput");

const progressBox = document.getElementById("progressBox");
const progress = document.getElementById("progress");
const fileName = document.getElementById("fileName");

let ws = null;
let pc = null;
let dataChannel = null;
let isCreator = false;

let incoming = null;
let roomId = null;
let secretKey = null;

let remoteDescriptionSet = false;
const pendingCandidates = [];

function randomBytes(size) {
const bytes = new Uint8Array(size);
crypto.getRandomValues(bytes);
return bytes;
}

function bytesToBase64(bytes) {
let binary = "";

```
for (const byte of bytes) {
    binary += String.fromCharCode(byte);
}

return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
```

}

function base64ToBytes(str) {
str = str
.replace(/-/g, "+")
.replace(/_/g, "/");

```
while (str.length % 4) {
    str += "=";
}

const binary = atob(str);
const bytes = new Uint8Array(binary.length);

for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
}

return bytes;
```

}

async function importKey(raw) {
return crypto.subtle.importKey(
"raw",
raw,
{
name: "AES-GCM"
},
false,
["encrypt", "decrypt"]
);
}

async function createSecret() {
const raw = randomBytes(32);

```
secretKey = await importKey(raw);

return bytesToBase64(raw);
```

}

async function loadSecret(encoded) {
const raw = base64ToBytes(encoded);

```
if (raw.length !== 32) {
    throw new Error("Invalid room secret");
}

secretKey = await importKey(raw);
```

}

function connect() {
const protocol =
location.protocol === "https:"
? "wss:"
: "ws:";

```
const wsUrl =
    `${protocol}//${location.host}/api/ws`;

console.log(
    "Connecting to signaling server:",
    wsUrl
);

ws = new WebSocket(wsUrl);

ws.onopen = () => {
    console.log(
        "Connected to signaling server"
    );
};

ws.onmessage = async event => {
    let msg;

    try {
        msg = JSON.parse(event.data);
    } catch (error) {
        console.error(
            "Invalid signaling message:",
            error
        );
        return;
    }

    if (msg.type === "created") {
        showRoom(msg.room);
        return;
    }

    if (msg.type === "joined") {
        showRoom(roomId);
        return;
    }

    if (msg.type === "peer-joined") {
        status.textContent =
            "Peer found. Establishing secure connection...";

        if (isCreator) {
            await createPeer(true);
        }

        return;
    }

    if (msg.type === "offer") {
        try {
            await createPeer(false);

            await pc.setRemoteDescription(
                msg.offer
            );

            remoteDescriptionSet = true;

            await flushPendingCandidates();

            const answer =
                await pc.createAnswer();

            await pc.setLocalDescription(
                answer
            );

            await waitForIceGathering();

            sendSignal({
                type: "answer",
                answer: pc.localDescription
            });

        } catch (error) {
            console.error(
                "Offer handling error:",
                error
            );

            status.textContent =
                "ERROR: Unable to establish peer connection.";
        }

        return;
    }

    if (msg.type === "answer") {
        try {
            if (!pc) return;

            await pc.setRemoteDescription(
                msg.answer
            );

            remoteDescriptionSet = true;

            await flushPendingCandidates();

        } catch (error) {
            console.error(
                "Answer handling error:",
                error
            );
        }

        return;
    }

    if (msg.type === "candidate") {
        if (!pc) return;

        try {
            if (
                remoteDescriptionSet &&
                pc.remoteDescription
            ) {
                await pc.addIceCandidate(
                    msg.candidate
                );
            } else {
                pendingCandidates.push(
                    msg.candidate
                );
            }
        } catch (error) {
            console.warn(
                "ICE candidate error:",
                error
            );
        }

        return;
    }

    if (msg.type === "peer-left") {
        status.textContent =
            "Peer disconnected.";
    }

    if (msg.type === "error") {
        console.error(
            "Server error:",
            msg.message
        );

        alert(msg.message);
    }
};

ws.onerror = error => {
    console.error(
        "WebSocket error:",
        error
    );

    status.textContent =
        "Connection to server failed.";
};

ws.onclose = event => {
    console.log(
        "Signaling connection closed:",
        event.code,
        event.reason
    );
};
```

}

function sendSignal(message) {
if (
ws &&
ws.readyState === WebSocket.OPEN
) {
ws.send(
JSON.stringify(message)
);
}
}

async function waitForIceGathering() {
if (!pc) return;

```
if (
    pc.iceGatheringState ===
    "complete"
) {
    return;
}

await new Promise(resolve => {
    const timeout = setTimeout(
        resolve,
        5000
    );

    const check = () => {
        if (
            pc.iceGatheringState ===
            "complete"
        ) {
            clearTimeout(timeout);

            pc.removeEventListener(
                "icegatheringstatechange",
                check
            );

            resolve();
        }
    };

    pc.addEventListener(
        "icegatheringstatechange",
        check
    );
});
```

}

async function flushPendingCandidates() {
if (!pc) return;

```
while (
    pendingCandidates.length > 0
) {
    const candidate =
        pendingCandidates.shift();

    try {
        await pc.addIceCandidate(
            candidate
        );
    } catch (error) {
        console.warn(
            "Queued ICE candidate error:",
            error
        );
    }
}
```

}

function showRoom(id) {
home.classList.add("hidden");
room.classList.remove("hidden");

```
roomId = id;

const secret =
    location.hash.substring(1);

link.value =
    `${location.origin}/?room=${encodeURIComponent(id)}#${secret}`;
```

}

async function createPeer(initiator) {
if (pc) {
try {
pc.close();
} catch {}
}

```
pc = new RTCPeerConnection({
    iceServers: [
        {
            urls: [
                "stun:stun.relay.metered.ca:80"
            ]
        },
        {
            urls: [
                "turn:global.relay.metered.ca:80",
                "turn:global.relay.metered.ca:80?transport=tcp",
                "turn:global.relay.metered.ca:443"
            ],
            username:
                "48f77a018104b53c1fad5990",
            credential:
                "z244JKcipGf/rINB"
        },
        {
            urls:
                "turns:global.relay.metered.ca:443?transport=tcp",
            username:
                "48f77a018104b53c1fad5990",
            credential:
                "z244JKcipGf/rINB"
        }
    ]
});

remoteDescriptionSet = false;
pendingCandidates.length = 0;

pc.onicecandidate = event => {
    if (
        event.candidate &&
        ws &&
        ws.readyState === WebSocket.OPEN
    ) {
        sendSignal({
            type: "candidate",
            candidate:
                event.candidate
        });
    }
};

pc.onicegatheringstatechange = () => {
    console.log(
        "ICE gathering:",
        pc.iceGatheringState
    );
};

pc.onconnectionstatechange = () => {
    console.log(
        "Peer connection state:",
        pc.connectionState
    );

    if (
        pc.connectionState ===
        "connected"
    ) {
        status.textContent =
            "✓ Encrypted peer-to-peer connection established.";
    }

    if (
        pc.connectionState ===
        "failed"
    ) {
        status.textContent =
            "Peer connection failed.";
    }

    if (
        pc.connectionState ===
        "disconnected"
    ) {
        status.textContent =
            "Peer connection lost.";
    }
};

pc.oniceconnectionstatechange = () => {
    console.log(
        "ICE state:",
        pc.iceConnectionState
    );
};

if (initiator) {
    dataChannel =
        pc.createDataChannel(
            "files"
        );

    setupDataChannel(
        dataChannel
    );

    const offer =
        await pc.createOffer();

    await pc.setLocalDescription(
        offer
    );

    await waitForIceGathering();

    sendSignal({
        type: "offer",
        offer: pc.localDescription
    });

} else {
    pc.ondatachannel =
        event => {
            dataChannel =
                event.channel;

            setupDataChannel(
                dataChannel
            );
        };
}
```

}

function setupDataChannel(channel) {
channel.binaryType =
"arraybuffer";

```
channel.onopen = () => {
    status.textContent =
        "✓ Encrypted peer-to-peer connection established.";
};

channel.onerror = error => {
    console.error(
        "Data channel error:",
        error
    );
};

channel.onclose = () => {
    console.log(
        "Data channel closed"
    );
};

channel.onmessage =
    event => {
        receiveData(
            event.data
        );
    };
```

}

createBtn.onclick =
async () => {
isCreator = true;

```
    try {
        const secret =
            await createSecret();

        location.hash =
            secret;

        connect();

        const wait =
            setInterval(() => {
                if (
                    ws &&
                    ws.readyState ===
                    WebSocket.OPEN
                ) {
                    clearInterval(wait);

                    sendSignal({
                        type: "create"
                    });
                }
            }, 50);

    } catch (error) {
        console.error(
            "Create room error:",
            error
        );

        status.textContent =
            "ERROR: Unable to create room.";
    }
};
```

joinBtn.onclick =
async () => {
const input =
roomInput.value.trim();

```
    if (!input) {
        alert(
            "Enter room ID"
        );

        return;
    }

    const secret =
        location.hash.substring(1);

    if (!secret) {
        alert(
            "This room link has no secret key."
        );

        return;
    }

    try {
        await loadSecret(
            secret
        );
    } catch {
        alert(
            "Invalid room secret."
        );

        return;
    }

    roomId = input;

    history.replaceState(
        {},
        "",
        `/?room=${encodeURIComponent(roomId)}#${secret}`
    );

    connect();

    const wait =
        setInterval(() => {
            if (
                ws &&
                ws.readyState ===
                WebSocket.OPEN
            ) {
                clearInterval(wait);

                sendSignal({
                    type: "join",
                    room: roomId
                });
            }
        }, 50);
};
```

const existingRoom =
new URLSearchParams(
location.search
).get("room");

const existingSecret =
location.hash.substring(1);

if (
existingRoom &&
existingSecret
) {
roomInput.value =
existingRoom;

```
loadSecret(
    existingSecret
).catch(() => {
    console.warn(
        "Invalid room secret"
    );
});
```

}

copyBtn.onclick =
async () => {
try {
await navigator.clipboard.writeText(
link.value
);

```
        copyBtn.textContent =
            "Copied";

        setTimeout(() => {
            copyBtn.textContent =
                "Copy";
        }, 1500);

    } catch (error) {
        console.error(
            "Copy error:",
            error
        );
    }
};
```

fileInput.onchange =
async () => {
const file =
fileInput.files[0];

```
    if (!file) return;

    if (
        !dataChannel ||
        dataChannel.readyState !== "open"
    ) {
        alert(
            "Secure connection is not ready."
        );

        return;
    }

    if (!secretKey) {
        alert(
            "Encryption key is missing."
        );

        return;
    }

    progressBox.classList.remove(
        "hidden"
    );

    fileName.textContent =
        file.name;

    progress.value = 0;

    status.textContent =
        "Encrypting file locally...";

    try {
        const original =
            await file.arrayBuffer();

        const iv =
            randomBytes(12);

        const encrypted =
            await crypto.subtle.encrypt(
                {
                    name: "AES-GCM",
                    iv
                },
                secretKey,
                original
            );

        const encryptedBytes =
            new Uint8Array(
                encrypted
            );

        const packet =
            new Uint8Array(
                12 +
                encryptedBytes.length
            );

        packet.set(iv, 0);

        packet.set(
            encryptedBytes,
            12
        );

        const encoded =
            bytesToBase64(packet);

        const total =
            Math.ceil(
                encoded.length /
                48000
            );

        dataChannel.send(
            JSON.stringify({
                type: "file",
                name: file.name,
                size: file.size,
                mime: file.type,
                total
            })
        );

        status.textContent =
            "Sending encrypted file...";

        for (
            let i = 0;
            i < total;
            i++
        ) {
            while (
                dataChannel.bufferedAmount >
                2 * 1024 * 1024
            ) {
                await new Promise(
                    resolve =>
                        setTimeout(
                            resolve,
                            20
                        )
                );
            }

            const part =
                encoded.slice(
                    i * 48000,
                    (i + 1) * 48000
                );

            dataChannel.send(
                JSON.stringify({
                    type: "chunk",
                    index: i,
                    data: part
                })
            );

            progress.value =
                Math.floor(
                    ((i + 1) / total) *
                    100
                );
        }

        dataChannel.send(
            JSON.stringify({
                type: "file-end"
            })
        );

        status.textContent =
            "✓ File encrypted and sent.";

    } catch (error) {
        console.error(
            "FILE ENCRYPTION ERROR:",
            error
        );

        status.textContent =
            "ERROR: Unable to encrypt file.";
    }
};
```

async function receiveData(data) {
if (typeof data !== "string") {
console.error(
"Unexpected binary message"
);

```
    return;
}

let msg;

try {
    msg = JSON.parse(data);
} catch {
    console.error(
        "Invalid message"
    );

    return;
}

if (msg.type === "file") {
    incoming = {
        name: msg.name,
        size: msg.size,
        mime: msg.mime,
        total: msg.total,
        parts: new Array(msg.total),
        receivedParts: 0
    };

    progressBox.classList.remove(
        "hidden"
    );

    progressBox
        .querySelectorAll("a")
        .forEach(a => a.remove());

    fileName.textContent =
        msg.name;

    progress.value = 0;

    status.textContent =
        "Receiving encrypted file...";

    return;
}

if (msg.type === "chunk") {
    if (!incoming) {
        return;
    }

    if (
        msg.index < 0 ||
        msg.index >= incoming.total
    ) {
        console.error(
            "Invalid chunk index"
        );

        return;
    }

    if (
        incoming.parts[msg.index] ===
        undefined
    ) {
        incoming.parts[msg.index] =
            msg.data;

        incoming.receivedParts++;

        progress.value =
            Math.floor(
                (
                    incoming.receivedParts /
                    incoming.total
                ) * 100
            );
    }

    return;
}

if (msg.type === "file-end") {
    if (!incoming) {
        return;
    }

    if (
        incoming.receivedParts !==
        incoming.total
    ) {
        console.error(
            "Missing chunks",
            {
                expected:
                    incoming.total,

                received:
                    incoming.receivedParts
            }
        );

        status.textContent =
            "ERROR: File transfer incomplete.";

        incoming = null;

        return;
    }

    try {
        status.textContent =
            "Decrypting file locally...";

        const encoded =
            incoming.parts.join("");

        const packet =
            base64ToBytes(
                encoded
            );

        const iv =
            packet.slice(
                0,
                12
            );

        const encrypted =
            packet.slice(
                12
            );

        const decrypted =
            await crypto.subtle.decrypt(
                {
                    name: "AES-GCM",
                    iv
                },
                secretKey,
                encrypted
            );

        if (
            decrypted.byteLength !==
            incoming.size
        ) {
            throw new Error(
                `Size mismatch: ${decrypted.byteLength} != ${incoming.size}`
            );
        }

        const blob =
            new Blob(
                [decrypted],
                {
                    type:
                        incoming.mime ||
                        "application/octet-stream"
                }
            );

        const url =
            URL.createObjectURL(
                blob
            );

        const a =
            document.createElement(
                "a"
            );

        a.href = url;

        a.download =
            incoming.name;

        a.textContent =
            `Download ${incoming.name}`;

        a.style.display =
            "block";

        a.style.marginTop =
            "20px";

        a.style.color =
            "white";

        progressBox.appendChild(
            a
        );

        progress.value = 100;

        status.textContent =
            "✓ File decrypted successfully.";

        incoming = null;

    } catch (error) {
        console.error(
            "FINAL DECRYPTION ERROR:",
            error
        );

        status.textContent =
            "ERROR: Unable to decrypt file.";

        incoming = null;
    }
}
```

}
