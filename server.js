const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const rooms = new Map();

function randomId() {
    let id;

    do {
        id = crypto.randomBytes(24).toString("base64url");
    } while (rooms.has(id));

    return id;
}

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    let pathname = url.pathname;

    if (pathname === "/") {
        pathname = "/index.html";
    }

    if (pathname.includes("..")) {
        res.writeHead(403);
        return res.end("Forbidden");
    }

    const filePath = path.join(
        __dirname,
        "public",
        pathname
    );

    fs.readFile(filePath, (err, data) => {
        if (err) {
            console.log("File not found:", filePath);
            res.writeHead(404, {
                "Content-Type": "text/plain; charset=utf-8"
            });
            return res.end("Not found");
        }

        const types = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8"
        };

        res.writeHead(200, {
            "Content-Type":
                types[path.extname(filePath)] ||
                "application/octet-stream",
            "Cache-Control": "no-store"
        });

        res.end(data);
    });
});

const wss = new WebSocket.Server({ server });

wss.on("connection", ws => {
    let roomId = null;

    ws.on("message", raw => {
        let msg;

        try {
            msg = JSON.parse(raw.toString());
        } catch {
            return;
        }

        if (msg.type === "create") {
            roomId = randomId();

            rooms.set(
                roomId,
                new Set([ws])
            );

            ws.send(JSON.stringify({
                type: "created",
                room: roomId
            }));

            return;
        }

        if (msg.type === "join") {
            roomId = msg.room;

            if (!rooms.has(roomId)) {
                ws.send(JSON.stringify({
                    type: "error",
                    message: "Room does not exist"
                }));

                return;
            }

            const room = rooms.get(roomId);

            if (room.size >= 2) {
                ws.send(JSON.stringify({
                    type: "error",
                    message: "Room is full"
                }));

                return;
            }

            room.add(ws);

            for (const peer of room) {
                if (
                    peer !== ws &&
                    peer.readyState === WebSocket.OPEN
                ) {
                    peer.send(JSON.stringify({
                        type: "peer-joined"
                    }));
                }
            }

            ws.send(JSON.stringify({
                type: "joined"
            }));

            return;
        }

        if (
            !roomId ||
            !rooms.has(roomId)
        ) {
            return;
        }

        for (const peer of rooms.get(roomId)) {
            if (
                peer !== ws &&
                peer.readyState === WebSocket.OPEN
            ) {
                peer.send(
                    JSON.stringify(msg)
                );
            }
        }
    });

    ws.on("close", () => {
        if (
            !roomId ||
            !rooms.has(roomId)
        ) {
            return;
        }

        const room = rooms.get(roomId);

        room.delete(ws);

        for (const peer of room) {
            if (
                peer.readyState === WebSocket.OPEN
            ) {
                peer.send(JSON.stringify({
                    type: "peer-left"
                }));
            }
        }

        if (room.size === 0) {
            rooms.delete(roomId);
        }
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log("");
    console.log("=================================");
    console.log(" ANONSHARE");
    console.log("=================================");
    console.log(
        ` http://localhost:${PORT}`
    );
    console.log("");
});
