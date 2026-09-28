import http from "http";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 10000);
const rooms = new Map();

function getRoom(roomCode) {
  if (!rooms.has(roomCode)) {
    rooms.set(roomCode, new Set());
  }
  return rooms.get(roomCode);
}

function broadcast(roomCode, sender, message) {
  const room = rooms.get(roomCode);
  if (!room) return;

  for (const client of room) {
    if (client !== sender && client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      service: "Together Sync Server",
      websocket: "/ws?room=ROOMCODE"
    }));
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: false, error: "Not found" }));
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  let url;

  try {
    url = new URL(request.url, `http://${request.headers.host}`);
  } catch {
    socket.destroy();
    return;
  }

  if (url.pathname !== "/ws") {
    socket.destroy();
    return;
  }

  const roomCode = (url.searchParams.get("room") || "")
    .trim()
    .toUpperCase();

  if (!/^[A-Z0-9]{4,12}$/.test(roomCode)) {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\nInvalid room");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request, roomCode);
  });
});

wss.on("connection", (ws, request, roomCode) => {
  const room = getRoom(roomCode);
  room.add(ws);

  ws.send(JSON.stringify({
    type: "system",
    event: "connected",
    room: roomCode
  }));

  broadcast(
    roomCode,
    ws,
    JSON.stringify({ type: "system", event: "peer_joined" })
  );

  ws.on("message", (data, isBinary) => {
    const payload = isBinary ? data : data.toString();
    broadcast(roomCode, ws, payload);
  });

  ws.on("close", () => {
    room.delete(ws);

    broadcast(
      roomCode,
      ws,
      JSON.stringify({ type: "system", event: "peer_left" })
    );

    if (room.size === 0) {
      rooms.delete(roomCode);
    }
  });

  ws.on("error", () => {
    // Closing the socket will perform normal room cleanup.
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Together Sync Server listening on port ${PORT}`);
});
