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

const CHUNK_SIZE = 48 * 1024;

/*
 * WebRTC ICE servers.
 *
 * STUN помогает определить внешний адрес.
 * TURN нужен, когда прямое P2P-соединение невозможно.
 *
 * Для собственного TURN-сервера можно передать:
 *
 * TURN_URL
 * TURN_USERNAME
 * TURN_CREDENTIAL
 *
 * через переменные окружения/конфигурацию сайта.
 *
 * Ниже также есть публичный TURN для тестирования.
 */

const ICE_SERVERS = [
    {
        urls: [
            "stun:stun.l.google.com:19302",
            "stun:stun1.l.google.com:19302",
            "stun:stun2.l.google.com:19302"
        ]
    },

    {
        urls: [
            "turn:openrelay.metered.ca:80",
            "turn:openrelay.metered.ca:443",
            "turn:openrelay.metered.ca:80?transport=tcp",
            "turn:openrelay.metered.ca:443?transport=tcp"
        ],
        username: "openrelayproject",
        credential: "openrelayproject"
    }
];

function randomBytes(size) {
    const bytes = new Uint8Array(size);
    crypto.getRandomValues(bytes);
    return bytes;
}

function bytesToBase64(bytes) {
    let binary = "";

    for (const byte of bytes) {
        binary += String.fromCharCode(byte);
    }

    return btoa(binary)
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/, "");
}

function base64ToBytes(str) {
    str = str
        .replace(/-/g, "+")
        .replace(/_/g, "/");

    while (str.length % 4) {
        str += "=";
    }

    const binary = atob(str);

    const bytes = new Uint8Array(binary.length);

    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }

    return bytes;
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

    secretKey = await importKey(raw);

    return bytesToBase64(raw);
}

async function loadSecret(encoded) {
    const raw = base64ToBytes(encoded);

    if (raw.length !== 32) {
        throw new Error("Invalid room secret");
    }

    secretKey = await importKey(raw);
}

function connect() {
    const protocol =
        location.protocol === "https:"
            ? "wss:"
            : "ws:";

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
                try {
                    await createPeer(true);
                } catch (error) {
                    console.error(
                        "Peer creation error:",
                        error
                    );

                    status.textContent =
                        "ERROR: Unable to start WebRTC.";
                }
            }

            return;
        }

        if (msg.type === "offer") {
            try {
                await createPeer(false);

                await pc.setRemoteDescription(
                    new RTCSessionDescription(msg.offer)
                );

                remoteDescriptionSet = true;

                await flushPendingCandidates();

                const answer =
                    await pc.createAnswer();

                await pc.setLocalDescription(answer);

                ws.send(
                    JSON.stringify({
                        type: "answer",
                        answer: pc.localDescription
                    })
                );
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
                    new RTCSessionDescription(msg.answer)
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
            if (!pc || !msg.candidate) {
                return;
            }

            try {
                if (
                    remoteDescriptionSet &&
                    pc.remoteDescription
                ) {
                    await pc.addIceCandidate(
                        new RTCIceCandidate(
                            msg.candidate
                        )
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

            if (pc) {
                try {
                    pc.close();
                } catch {}
            }

            pc = null;
            dataChannel = null;
            remoteDescriptionSet = false;
            pendingCandidates.length = 0;

            return;
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
            "Connection to signaling server failed.";
    };

    ws.onclose = event => {
        console.log(
            "Signaling connection closed:",
            event.code,
            event.reason
        );
    };
}

async function flushPendingCandidates() {
    if (!pc || !pc.remoteDescription) {
        return;
    }

    while (pendingCandidates.length > 0) {
        const candidate =
            pendingCandidates.shift();

        try {
            await pc.addIceCandidate(
                new RTCIceCandidate(candidate)
            );
        } catch (error) {
            console.warn(
                "Queued ICE candidate error:",
                error
            );
        }
    }
}

function showRoom(id) {
    home.classList.add("hidden");
    room.classList.remove("hidden");

    roomId = id;

    const secret =
        location.hash.substring(1);

    link.value =
        `${location.origin}/?room=${encodeURIComponent(id)}#${secret}`;
}

async function createPeer(initiator) {
    if (pc) {
        try {
            pc.close();
        } catch {}
    }

    dataChannel = null;
    remoteDescriptionSet = false;
    pendingCandidates.length = 0;

    console.log(
        "Creating RTCPeerConnection..."
    );

    pc = new RTCPeerConnection({
        iceServers: ICE_SERVERS,

        /*
         * Allow both direct P2P and TURN relay.
         */
        iceTransportPolicy: "all",

        /*
         * More reliable connection establishment.
         */
        bundlePolicy: "max-bundle",
        rtcpMuxPolicy: "require"
    });

    pc.onicecandidate = event => {
        if (
            event.candidate &&
            ws &&
            ws.readyState === WebSocket.OPEN
        ) {
            console.log(
                "Sending ICE candidate:",
                event.candidate.candidate
            );

            ws.send(
                JSON.stringify({
                    type: "candidate",
                    candidate: event.candidate
                })
            );
        }
    };

    pc.onicecandidateerror = event => {
        console.warn(
            "ICE candidate error:",
            event.errorCode,
            event.errorText,
            event.url
        );
    };

    pc.onconnectionstatechange = () => {
        if (!pc) return;

        console.log(
            "Peer connection state:",
            pc.connectionState
        );

        if (pc.connectionState === "connected") {
            status.textContent =
                "✓ Encrypted peer-to-peer connection established.";
        }

        if (pc.connectionState === "connecting") {
            status.textContent =
                "Connecting to peer...";
        }

        if (
            pc.connectionState === "failed"
        ) {
            status.textContent =
                "Peer connection failed. TURN server may be unavailable.";
        }

        if (
            pc.connectionState === "disconnected"
        ) {
            status.textContent =
                "Peer connection temporarily disconnected...";
        }

        if (
            pc.connectionState === "closed"
        ) {
            status.textContent =
                "Peer connection closed.";
        }
    };

    pc.oniceconnectionstatechange = () => {
        if (!pc) return;

        console.log(
            "ICE state:",
            pc.iceConnectionState
        );

        if (
            pc.iceConnectionState === "checking"
        ) {
            status.textContent =
                "Checking network connection...";
        }

        if (
            pc.iceConnectionState === "connected" ||
            pc.iceConnectionState === "completed"
        ) {
            status.textContent =
                "✓ Secure connection established.";
        }

        if (
            pc.iceConnectionState === "failed"
        ) {
            console.error(
                "ICE FAILED — TURN connection was not established."
            );

            status.textContent =
                "ICE failed. Trying to use TURN...";
        }
    };

    pc.onicegatheringstatechange = () => {
        console.log(
            "ICE gathering state:",
            pc.iceGatheringState
        );
    };

    if (initiator) {
        dataChannel =
            pc.createDataChannel("files", {
                ordered: true
            });

        setupDataChannel(dataChannel);

        const offer =
            await pc.createOffer();

        await pc.setLocalDescription(offer);

        /*
         * Wait a little for ICE candidates to be
         * gathered before sending the offer.
         *
         * Trickle ICE still works, but this makes
         * the initial connection more reliable.
         */
        await waitForIceGathering();

        ws.send(
            JSON.stringify({
                type: "offer",
                offer: pc.localDescription
            })
        );
    } else {
        pc.ondatachannel = event => {
            dataChannel =
                event.channel;

            setupDataChannel(dataChannel);
        };
    }
}

function waitForIceGathering() {
    return new Promise(resolve => {
        if (!pc) {
            resolve();
            return;
        }

        if (
            pc.iceGatheringState ===
            "complete"
        ) {
            resolve();
            return;
        }

        const timeout =
            setTimeout(resolve, 5000);

        const check = () => {
            if (!pc) {
                clearTimeout(timeout);
                resolve();
                return;
            }

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
}

function setupDataChannel(channel) {
    channel.binaryType =
        "arraybuffer";

    channel.onopen = () => {
        console.log(
            "Data channel opened"
        );

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

    channel.onmessage = event => {
        receiveData(event.data);
    };
}

createBtn.onclick = async () => {
    isCreator = true;

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

                ws.send(
                    JSON.stringify({
                        type: "create"
                    })
                );
            }
        }, 50);
};

joinBtn.onclick = async () => {
    const input =
        roomInput.value.trim();

    if (!input) {
        alert("Enter room ID");
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
        await loadSecret(secret);
    } catch {
        alert(
            "Invalid room secret."
        );
        return;
    }

    isCreator = false;
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

                ws.send(
                    JSON.stringify({
                        type: "join",
                        room: roomId
                    })
                );
            }
        }, 50);
};

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

    loadSecret(existingSecret)
        .catch(() => {
            console.warn(
                "Invalid room secret"
            );
        });
}

copyBtn.onclick = async () => {
    await navigator.clipboard.writeText(
        link.value
    );

    copyBtn.textContent =
        "Copied";

    setTimeout(() => {
        copyBtn.textContent =
            "Copy";
    }, 1500);
};

fileInput.onchange = async () => {
    const file =
        fileInput.files[0];

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
                encoded.length / 48000
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
                    ((i + 1) / total) * 100
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

async function receiveData(data) {
    if (typeof data !== "string") {
        console.error(
            "Unexpected binary message"
        );
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
                base64ToBytes(encoded);

            const iv =
                packet.slice(0, 12);

            const encrypted =
                packet.slice(12);

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
                URL.createObjectURL(blob);

            const a =
                document.createElement("a");

            a.href = url;
            a.download = incoming.name;
            a.textContent =
                `Download ${incoming.name}`;

            a.style.display = "block";
            a.style.marginTop = "20px";
            a.style.color = "white";

            progressBox.appendChild(a);

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
}
