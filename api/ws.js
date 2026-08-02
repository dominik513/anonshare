const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");
const Redis = require("ioredis");

const server = http.createServer();

const wss = new WebSocket.Server({
    noServer: true,
    maxPayload: 1024 * 1024
});

// ИСПРАВЛЕНИЕ: Явно обрабатываем протокол upgrade для WebSocket
server.on("upgrade", (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit("connection", ws, request);
    });
});

const redis = new Redis(
    process.env.REDIS_URL,
    {
        maxRetriesPerRequest: null,
        retryStrategy(times) {
            return Math.min(times * 200, 5000);
        }
    }
);

const subscriber = redis.duplicate();
const localRooms = new Map();

function randomId() {
    return crypto.randomBytes(24).toString("base64url");
}

function memberKey(room) {
    return `anonshare:room:${room}:members`;
}

function roomKey(room) {
    return `anonshare:room:${room}`;
}

function signalKey(room) {
    return `anonshare:signal:${room}`;
}

function send(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
    }
}

function addLocal(room, ws) {
    if (!room) return;
    if (!localRooms.has(room)) {
        localRooms.set(room, new Set());
    }
    localRooms.get(room).add(ws);
}

function removeLocal(room, ws) {
    if (!room) return;
    const set = localRooms.get(room);
    if (!set) return;
    set.delete(ws);
    if (set.size === 0) {
        localRooms.delete(room);
    }
}

function broadcastLocal(room, message, exceptId) {
    const set = localRooms.get(room);
    if (!set) return;

    for (const peer of set) {
        if (peer.connectionId === exceptId) continue;
        if (peer.readyState === WebSocket.OPEN) {
            try {
                peer.send(JSON.stringify(message));
            } catch (error) {
                console.error("Local send error:", error);
            }
        }
    }
}

subscriber.on("error", error => {
    console.error("Redis subscriber error:", error);
});

subscriber.psubscribe("anonshare:signal:*").catch(error => {
    console.error("Redis subscribe error:", error);
});

subscriber.on("pmessage", (pattern, channel, raw) => {
    try {
        const packet = JSON.parse(raw);
        if (!packet.room || !packet.message) return;
        broadcastLocal(packet.room, packet.message, packet.sender);
    } catch (error) {
        console.error("Redis message error:", error);
    }
});

async function publish(room, sender, message) {
    if (!room) return;
    await redis.publish(
        signalKey(room),
        JSON.stringify({ room, sender, message })
    );
}

async function createRoom(connectionId) {
    for (;;) {
        const id = randomId();
        const created = await redis.set(roomKey(id), "1", "EX", 86400, "NX");

        if (created === "OK") {
            await redis.zadd(memberKey(id), Date.now(), connectionId);
            await redis.expire(memberKey(id), 86400);
            return id;
        }
    }
}

async function joinRoom(room, connectionId) {
    const exists = await redis.exists(roomKey(room));
    if (!exists) {
        return { ok: false, reason: "Room does not exist" };
    }

    const members = memberKey(room);
    const now = Date.now();

    await redis.zremrangebyscore(members, 0, now - 60000);
    const count = await redis.zcard(members);

    if (count >= 2) {
        return { ok: false, reason: "Room is full" };
    }

    await redis.zadd(members, now, connectionId);
    await redis.expire(members, 86400);

    return { ok: true };
}

async function leaveRoom(room, connectionId) {
    if (!room) return;
    const members = memberKey(room);
    await redis.zrem(members, connectionId);
    const count = await redis.zcard(members);

    if (count === 0) {
        await redis.del(members, roomKey(room));
    }
}

wss.on("connection", ws => {
    ws.connectionId = randomId();
    ws.roomId = null;
    ws.isAlive = true;

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    ws.on("message", async raw => {
        let msg;
        try {
            msg = JSON.parse(raw.toString());
        } catch {
            return;
        }

        try {
            if (msg.type === "create") {
                if (ws.roomId) return;
                const room = await createRoom(ws.connectionId);
                ws.roomId = room;
                addLocal(room, ws);
                send(ws, { type: "created", room });
                return;
            }

            if (msg.type === "join") {
                if (ws.roomId) return;
                const room = typeof msg.room === "string" ? msg.room.trim() : "";

                if (!room || room.length > 200) {
                    send(ws, { type: "error", message: "Invalid room" });
                    return;
                }

                const result = await joinRoom(room, ws.connectionId);
                if (!result.ok) {
                    send(ws, { type: "error", message: result.reason });
                    return;
                }

                ws.roomId = room;
                addLocal(room, ws);
                send(ws, { type: "joined" });

                await publish(room, ws.connectionId, { type: "peer-joined" });
                return;
            }

            if (!ws.roomId) return;

            const allowed = new Set(["offer", "answer", "candidate"]);
            if (!allowed.has(msg.type)) return;

            await publish(ws.roomId, ws.connectionId, msg);

        } catch (error) {
            console.error("WebSocket message error:", error);
            send(ws, { type: "error", message: "Server error" });
        }
    });

    ws.on("close", async () => {
        const room = ws.roomId;
        removeLocal(room, ws);

        try {
            await leaveRoom(room, ws.connectionId);
            if (room) {
                await publish(room, ws.connectionId, { type: "peer-left" });
            }
        } catch (error) {
            console.error("Leave room error:", error);
        }
    });

    ws.on("error", error => {
        console.error("WebSocket error:", error);
    });
});

const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
        if (ws.isAlive === false) {
            try { ws.terminate(); } catch {}
            continue;
        }
        ws.isAlive = false;
        try { ws.ping(); } catch {}
    }
}, 30000);

wss.on("close", () => {
    clearInterval(heartbeat);
});

process.on("SIGTERM", async () => {
    clearInterval(heartbeat);
    try { await subscriber.quit(); } catch {}
    try { await redis.quit(); } catch {}
    process.exit(0);
});

module.exports = server;
