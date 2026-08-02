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

// Размер чанка данных (16 KB)
const CHUNK_SIZE = 16384;

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
        { name: "AES-GCM" },
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
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${location.host}/api/ws`;

    console.log("Connecting to:", wsUrl);

    if (ws) {
        try { ws.close(); } catch {}
    }

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log("WebSocket connected");
    };

    ws.onmessage = async event => {
        let msg;
        try {
            msg = JSON.parse(event.data);
        } catch (error) {
            console.error("Invalid signaling message:", error);
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
            status.textContent = "Peer found. Establishing secure connection...";
            if (isCreator) {
                try {
                    await createPeer(true);
                } catch (error) {
                    console.error("Peer creation error:", error);
                    status.textContent = "ERROR: Unable to establish peer connection.";
                }
            }
            return;
        }

        if (msg.type === "offer") {
            try {
                await createPeer(false);
                await pc.setRemoteDescription(msg.offer);
                remoteDescriptionSet = true;
                await flushPendingCandidates();

                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);

                sendSignal({
                    type: "answer",
                    answer: pc.localDescription
                });
            } catch (error) {
                console.error("Offer handling error:", error);
                status.textContent = "ERROR: Unable to establish peer connection.";
            }
            return;
        }

        if (msg.type === "answer") {
            try {
                if (!pc) return;
                await pc.setRemoteDescription(msg.answer);
                remoteDescriptionSet = true;
                await flushPendingCandidates();
            } catch (error) {
                console.error("Answer handling error:", error);
            }
            return;
        }

        if (msg.type === "candidate") {
            if (!pc) return;
            try {
                if (remoteDescriptionSet && pc.remoteDescription) {
                    await pc.addIceCandidate(msg.candidate);
                } else {
                    pendingCandidates.push(msg.candidate);
                }
            } catch (error) {
                console.warn("ICE candidate error:", error);
            }
            return;
        }

        if (msg.type === "peer-left") {
            status.textContent = "Peer disconnected.";
            return;
        }

        if (msg.type === "error") {
            console.error("Server error:", msg.message);
            alert(msg.message);
        }
    };

    ws.onerror = error => {
        console.error("WebSocket error:", error);
        status.textContent = "Connection to server failed.";
    };
}

function sendSignal(message) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
    }
}

async function flushPendingCandidates() {
    if (!pc) return;
    while (pendingCandidates.length > 0) {
        const candidate = pendingCandidates.shift();
        try {
            await pc.addIceCandidate(candidate);
        } catch (error) {
            console.warn("Queued ICE candidate error:", error);
        }
    }
}

function showRoom(id) {
    home.classList.add("hidden");
    room.classList.remove("hidden");
    roomId = id;

    const secret = location.hash.substring(1);
    link.value = `${location.origin}/?room=${encodeURIComponent(id)}#${secret}`;
    status.textContent = "Room created. Send the link to your peer.";
}

async function createPeer(initiator) {
    if (pc) {
        try { pc.close(); } catch {}
    }

    pc = new RTCPeerConnection({
        iceServers: [
            { urls: "stun:stun.relay.metered.ca:80" },
            {
                urls: [
                    "turn:global.relay.metered.ca:80",
                    "turn:global.relay.metered.ca:80?transport=tcp",
                    "turn:global.relay.metered.ca:443"
                ],
                username: "48f77a018104b53c1fad5990",
                credential: "z244JKcipGf/rINB"
            },
            {
                urls: "turns:global.relay.metered.ca:443?transport=tcp",
                username: "48f77a018104b53c1fad5990",
                credential: "z244JKcipGf/rINB"
            }
        ]
    });

    remoteDescriptionSet = false;
    pendingCandidates.length = 0;

    pc.onicecandidate = event => {
        if (event.candidate && ws && ws.readyState === WebSocket.OPEN) {
            sendSignal({
                type: "candidate",
                candidate: event.candidate
            });
        }
    };

    pc.onconnectionstatechange = () => {
        console.log("Peer connection state:", pc.connectionState);

        if (pc.connectionState === "connected") {
            status.textContent = "✓ Encrypted peer-to-peer connection established.";
        }
        if (pc.connectionState === "failed") {
            status.textContent = "Peer connection failed.";
        }
        if (pc.connectionState === "disconnected") {
            status.textContent = "Peer connection lost.";
        }
    };

    if (initiator) {
        dataChannel = pc.createDataChannel("files");
        setupDataChannel(dataChannel);

        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);

        sendSignal({
            type: "offer",
            offer: pc.localDescription
        });
    } else {
        pc.ondatachannel = event => {
            dataChannel = event.channel;
            setupDataChannel(dataChannel);
        };
    }
}

function setupDataChannel(channel) {
    channel.binaryType = "arraybuffer";

    channel.onopen = () => {
        status.textContent = "✓ Encrypted peer-to-peer connection established.";
    };

    channel.onerror = error => {
        console.error("Data channel error:", error);
    };

    channel.onclose = () => {
        console.log("Data channel closed");
    };

    channel.onmessage = event => {
        receiveData(event.data);
    };
}

createBtn.onclick = async () => {
    isCreator = true;
    try {
        const secret = await createSecret();
        location.hash = secret;
        status.textContent = "Connecting to server...";
        connect();

        const wait = setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                clearInterval(wait);
                sendSignal({ type: "create" });
            }
        }, 50);

        setTimeout(() => {
            clearInterval(wait);
            if (!ws || ws.readyState !== WebSocket.OPEN) {
                status.textContent = "ERROR: WebSocket server is unavailable.";
            }
        }, 10000);
    } catch (error) {
        console.error("Create room error:", error);
        status.textContent = "ERROR: Unable to create room.";
    }
};

joinBtn.onclick = async () => {
    const input = roomInput.value.trim();
    if (!input) {
        alert("Enter room ID");
        return;
    }

    const secret = location.hash.substring(1);
    if (!secret) {
        alert("This room link has no secret key.");
        return;
    }

    try {
        await loadSecret(secret);
    } catch {
        alert("Invalid room secret.");
        return;
    }

    roomId = input;
    history.replaceState({}, "", `/?room=${encodeURIComponent(roomId)}#${secret}`);

    status.textContent = "Connecting to server...";
    connect();

    const wait = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            clearInterval(wait);
            sendSignal({ type: "join", room: roomId });
        }
    }, 50);

    setTimeout(() => {
        clearInterval(wait);
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            status.textContent = "ERROR: WebSocket server is unavailable.";
        }
    }, 10000);
};

const existingRoom = new URLSearchParams(location.search).get("room");
const existingSecret = location.hash.substring(1);

if (existingRoom && existingSecret) {
    roomInput.value = existingRoom;
    loadSecret(existingSecret).catch(() => {});
}

copyBtn.onclick = async () => {
    try {
        await navigator.clipboard.writeText(link.value);
        copyBtn.textContent = "Copied";
        setTimeout(() => { copyBtn.textContent = "Copy"; }, 1500);
    } catch (error) {
        console.error("Copy error:", error);
    }
};

// --- ОТПРАВКА ФАЙЛА (Потоково) ---
fileInput.onchange = async () => {
    const file = fileInput.files[0];
    if (!file) return;

    if (!dataChannel || dataChannel.readyState !== "open") {
        alert("Secure connection is not ready.");
        return;
    }

    if (!secretKey) {
        alert("Encryption key is missing.");
        return;
    }

    progressBox.classList.remove("hidden");
    fileName.textContent = file.name;
    progress.value = 0;
    status.textContent = "Sending file...";

    try {
        // 1. Отправляем метаданные о файле
        dataChannel.send(JSON.stringify({
            type: "file-meta",
            name: file.name,
            size: file.size,
            mime: file.type
        }));

        let offset = 0;
        let chunkIndex = 0;
        const totalChunks = Math.ceil(file.size / CHUNCH_SIZE_CALC = CHUNK_SIZE); // или просто Math.ceil(file.size / CHUNK_SIZE)

        while (offset < file.size) {
            // Контроль буфера, чтобы не перегрузить канал
            while (dataChannel.bufferedAmount > 2 * 1024 * 1024) {
                await new Promise(resolve => setTimeout(resolve, 20));
            }

            const chunkSlice = file.slice(offset, offset + CHUNK_SIZE);
            const chunkBuffer = await chunkSlice.arrayBuffer();

            // Шифруем каждый чанк отдельно
            const iv = randomBytes(12);
            const encryptedChunk = await crypto.subtle.encrypt(
                { name: "AES-GCM", iv },
                secretKey,
                chunkBuffer
            );

            // Собираем бинарный пакет: [IV (12 байт)] + [Зашифрованные данные]
            const packet = new Uint8Array(12 + encryptedChunk.byteLength);
            packet.set(iv, 0);
            packet.set(new Uint8Array(encryptedChunk), 12);

            // Также добавляем префикс с индексом чанка (первые 4 байта) чтобы получатель знал порядок
            const finalPacket = new Uint8Array(4 + packet.byteLength);
            const view = new DataView(finalPacket.buffer);
            view.setUint32(0, chunkIndex, false);
            finalPacket.set(packet, 4);

            dataChannel.send(finalPacket);

            offset += CHUNK_SIZE;
            chunkIndex++;

            progress.value = Math.floor((offset / file.size) * 100);
        }

        // Сигнал окончания передачи
        dataChannel.send(JSON.stringify({ type: "file-end" }));
        status.textContent = "✓ File sent successfully.";

    } catch (error) {
        console.error("FILE SEND ERROR:", error);
        status.textContent = "ERROR: Unable to send file.";
    }
};

// --- ПОЛУЧЕНИЕ ФАЙЛА ---
let receivedBuffers = [];
let receivedSize = 0;
let fileMeta = null;

function receiveData(data) {
    // Если пришли метаданные или конец файла (строка JSON)
    if (typeof data === "string") {
        let msg;
        try {
            msg = JSON.parse(data);
        } catch {
            return;
        }

        if (msg.type === "file-meta") {
            fileMeta = msg;
            receivedBuffers = [];
            receivedSize = 0;

            progressBox.classList.remove("hidden");
            progressBox.querySelectorAll("a").forEach(a => a.remove());
            fileName.textContent = msg.name;
            progress.value = 0;
            status.textContent = "Receiving file...";
            return;
        }

        if (msg.type === "file-end" && fileMeta) {
            assembleAndDecryptFile();
            return;
        }
        return;
    }

    // Если пришел бинарный чанк
    if (data instanceof ArrayBuffer) {
        if (!fileMeta) return;

        const view = new DataView(data);
        const chunkIndex = view.getUint32(0, false);
        const packet = data.slice(4); // Отрезаем индекс

        receivedBuffers.push({ index: chunkIndex, data: packet });
        receivedSize += packet.byteLength;

        progress.value = Math.floor((receivedSize / (fileMeta.size + (receivedBuffers.length * 16))) * 100);
    }
}

async function assembleAndDecryptFile() {
    try {
        status.textContent = "Decrypting file locally...";

        // Сортируем чанки по порядку на случай прихода вразнобой
        receivedBuffers.sort((a, b) => a.index - b.index);

        const decryptedChunks = [];

        for (const item of receivedBuffers) {
            const packet = new Uint8Array(item.data);
            const iv = packet.slice(0, 12);
            const encrypted = packet.slice(12);

            const decrypted = await crypto.subtle.decrypt(
                { name: "AES-GCM", iv },
                secretKey,
                encrypted
            );

            decryptedChunks.push(decrypted);
        }

        const blob = new Blob(decryptedChunks, {
            type: fileMeta.mime || "application/octet-stream"
        });

        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileMeta.name;
        a.textContent = `Download ${fileMeta.name}`;
        a.style.display = "block";
        a.style.marginTop = "20px";
        a.style.color = "white";

        progressBox.appendChild(a);
        progress.value = 100;
        status.textContent = "✓ File decrypted successfully.";

        fileMeta = null;
        receivedBuffers = [];
    } catch (error) {
        console.error("DECRYPTION ERROR:", error);
        status.textContent = "ERROR: Unable to decrypt file.";
        fileMeta = null;
        receivedBuffers = [];
    }
}
