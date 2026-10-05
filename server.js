import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 10000);

const PACKET_AUDIO = 1;
const PACKET_VIDEO = 2;

const rooms = new Map();

function sendText(peer, value) {
  if (peer.ws.readyState !== WebSocket.OPEN) return;
  try { peer.ws.send(JSON.stringify(value)); } catch {}
}

function sendBinary(peer, data) {
  if (peer.ws.readyState !== WebSocket.OPEN) return;
  try { peer.ws.send(data, { binary: true }); } catch {}
}

function broadcastCount(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;

  for (const peer of room.peers) {
    sendText(peer, { type: 'peer-count', peers: room.peers.size });
  }
}

function cleanup(peer) {
  if (peer.closed) return;
  peer.closed = true;

  if (peer.roomId) {
    const room = rooms.get(peer.roomId);
    if (room) {
      room.peers.delete(peer);
      if (room.peers.size === 0) rooms.delete(peer.roomId);
      else broadcastCount(peer.roomId);
    }
  }
}

function closePeer(peer, code, reason) {
  if (peer.ws.readyState === WebSocket.OPEN || peer.ws.readyState === WebSocket.CONNECTING) {
    try { peer.ws.close(code, reason); } catch {}
  }
  cleanup(peer);
}

function handleJson(peer, message) {
  if (message.type === 'ping') {
    sendText(peer, { type: 'pong', at: message.at });
    return;
  }

  if (
    peer.joined &&
    (message.type === 'camera-state' || message.type === 'request-keyframe')
  ) {
    const room = rooms.get(peer.roomId);
    if (!room) return;

    const relayed =
      message.type === 'camera-state'
        ? { type: 'camera-state', enabled: Boolean(message.enabled) }
        : { type: 'request-keyframe' };

    for (const other of room.peers) {
      if (other !== peer) sendText(other, relayed);
    }
    return;
  }

  if (message.type !== 'join' || peer.joined) return;

  const roomId = String(message.room || '').toUpperCase();
  const proof = String(message.proof || '');

  if (!/^[A-Z0-9-]{4,24}$/.test(roomId) || !/^[a-f0-9]{64}$/.test(proof)) {
    sendText(peer, { type: 'error', message: 'Invalid room information.' });
    closePeer(peer, 1008, 'invalid room');
    return;
  }

  let room = rooms.get(roomId);

  if (!room) {
    room = { proof, peers: new Set() };
    rooms.set(roomId, room);
  }

  if (room.proof !== proof) {
    sendText(peer, { type: 'error', message: 'Wrong passphrase for this room.' });
    closePeer(peer, 1008, 'wrong passphrase');
    return;
  }

  if (room.peers.size >= 2) {
    sendText(peer, { type: 'error', message: 'This room already has two callers.' });
    closePeer(peer, 1008, 'room full');
    return;
  }

  room.peers.add(peer);
  peer.roomId = roomId;
  peer.joined = true;

  console.log(`Joined room ${roomId} (${room.peers.size}/2)`);

  sendText(peer, { type: 'joined', peers: room.peers.size });
  broadcastCount(roomId);
}

function handleBinary(peer, data) {
  if (!peer.joined || data.length < 2) return;

  const kind = data[0];
  if (kind === PACKET_AUDIO && data.length > 4097) return;
  if (kind === PACKET_VIDEO && data.length > 300_000) return;
  if (kind !== PACKET_AUDIO && kind !== PACKET_VIDEO) return;

  const room = rooms.get(peer.roomId);
  if (!room) return;

  for (const other of room.peers) {
    if (other !== peer) sendBinary(other, data);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, {
      'Content-Type': 'text/plain; charset=utf-8',
      'Cache-Control': 'no-store'
    });
    res.end('ok');
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end('LinkLine call server is online');
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 300_000,
  perMessageDeflate: false
});

server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/api/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws) => {
  const peer = {
    ws,
    roomId: null,
    joined: false,
    closed: false,
    windowStart: Date.now(),
    frames: 0,
    bytes: 0
  };

  console.log('WebSocket connected');

  ws.on('message', (data, isBinary) => {
    if (peer.closed) return;

    const now = Date.now();
    if (now - peer.windowStart >= 1000) {
      peer.windowStart = now;
      peer.frames = 0;
      peer.bytes = 0;
    }

    const size = Buffer.isBuffer(data) ? data.length : Buffer.byteLength(data);
    peer.frames += 1;
    peer.bytes += size;

    if (peer.frames > 180 || peer.bytes > 3_200_000) return;

    if (isBinary) {
      handleBinary(peer, Buffer.from(data));
      return;
    }

    let message;
    try {
      message = JSON.parse(data.toString());
    } catch {
      return;
    }
    handleJson(peer, message);
  });

  ws.on('close', (code, reason) => {
    console.log(`WebSocket closed code=${code} reason=${reason.toString() || 'none'}`);
    cleanup(peer);
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error.message);
    cleanup(peer);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`LinkLine server running on port ${PORT}`);
});
