const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');

const SIZE = 12;
const WIN_LEN = 5;
const TOTAL = SIZE * SIZE;
const WIN_DIRS = [[0, 1], [1, 0], [1, 1], [1, -1]];
const ROOM_TTL_AFTER_EMPTY_MS = 2 * 60 * 1000;

function idx(r, c) { return r * SIZE + c; }

function checkResult(b) {
  for (const [dr, dc] of WIN_DIRS) {
    for (let r = 0; r < SIZE; r++) {
      for (let c = 0; c < SIZE; c++) {
        const endR = r + dr * (WIN_LEN - 1);
        const endC = c + dc * (WIN_LEN - 1);
        if (endR < 0 || endR >= SIZE || endC < 0 || endC >= SIZE) continue;
        const run = [];
        for (let k = 0; k < WIN_LEN; k++) run.push(idx(r + dr * k, c + dc * k));
        if (b[run[0]] && run.every((cell) => b[cell] === b[run[0]])) {
          return { winner: b[run[0]], cells: run };
        }
      }
    }
  }
  if (b.every((v) => v)) return { winner: null, cells: null, draw: true };
  return null;
}

function genRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let s = '';
  for (let i = 0; i < 6; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

const rooms = new Map();

function makeRoom(password) {
  let id;
  do { id = genRoomCode(); } while (rooms.has(id));
  const room = {
    id,
    password,
    players: [],
    board: new Array(TOTAL).fill(''),
    turn: 'X',
    started: false,
    over: null,
    pendingLeave: null,
    emptySince: null,
  };
  rooms.set(id, room);
  return room;
}

function send(ws, msg) {
  if (!ws || ws.readyState !== 1) return;
  try { ws.send(JSON.stringify(msg)); } catch (e) { /* ignore */ }
}

function roomPublicState(room, forClientId) {
  return {
    roomId: room.id,
    board: room.board,
    turn: room.turn,
    started: room.started,
    over: room.over,
    pendingLeave: room.pendingLeave ? { byYou: room.pendingLeave === forClientId } : null,
    players: room.players.map((p) => ({
      mark: p.mark,
      ready: p.ready,
      connected: !!p.ws && p.ws.readyState === 1,
      isYou: p.clientId === forClientId,
    })),
  };
}

function broadcast(room) {
  for (const p of room.players) {
    send(p.ws, { type: 'room_update', state: roomPublicState(room, p.clientId) });
  }
}

function scheduleCleanup(roomId) {
  setTimeout(() => {
    const room = rooms.get(roomId);
    if (room && room.players.every((p) => !p.ws)) rooms.delete(roomId);
  }, ROOM_TTL_AFTER_EMPTY_MS);
}

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let clientId = null;
  let roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'hello') {
      clientId = typeof msg.clientId === 'string' && msg.clientId ? msg.clientId : crypto.randomUUID();
      send(ws, { type: 'hello_ack', clientId });
      for (const room of rooms.values()) {
        const p = room.players.find((pp) => pp.clientId === clientId);
        if (p) {
          p.ws = ws;
          roomId = room.id;
          send(ws, { type: 'rejoined', roomId: room.id });
          broadcast(room);
          break;
        }
      }
      return;
    }

    if (!clientId) return;

    if (msg.type === 'create_room') {
      const password = String(msg.password || '').slice(0, 64);
      if (!password) return send(ws, { type: 'error', message: 'Mật khẩu không được để trống.' });
      const room = makeRoom(password);
      room.players.push({ clientId, ws, mark: 'X', ready: false });
      roomId = room.id;
      send(ws, { type: 'joined', roomId: room.id, mark: 'X' });
      broadcast(room);
      return;
    }

    if (msg.type === 'join_room') {
      const code = String(msg.roomId || '').trim().toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'Không tìm thấy phòng với mã này.' });
      const already = room.players.find((pp) => pp.clientId === clientId);
      if (!already) {
        if (room.players.length >= 2) return send(ws, { type: 'error', message: 'Phòng đã đủ 2 người.' });
        if (room.password !== String(msg.password || '')) return send(ws, { type: 'error', message: 'Sai mật khẩu.' });
        room.players.push({ clientId, ws, mark: 'O', ready: false });
      } else {
        already.ws = ws;
      }
      roomId = room.id;
      send(ws, { type: 'joined', roomId: room.id, mark: already ? already.mark : 'O' });
      broadcast(room);
      return;
    }

    const room = roomId ? rooms.get(roomId) : null;
    if (!room) return;
    const me = room.players.find((pp) => pp.clientId === clientId);
    if (!me) return;

    if (msg.type === 'ready') {
      if (room.started) return;
      me.ready = !me.ready;
      if (room.players.length === 2 && room.players.every((p) => p.ready)) {
        room.started = true;
        room.board = new Array(TOTAL).fill('');
        room.turn = 'X';
        room.over = null;
      }
      broadcast(room);
      return;
    }

    if (msg.type === 'move') {
      if (!room.started || room.over) return;
      if (me.mark !== room.turn) return;
      const i = msg.index;
      if (typeof i !== 'number' || i < 0 || i >= TOTAL || room.board[i]) return;
      room.board[i] = me.mark;
      const result = checkResult(room.board);
      if (result) {
        room.over = result;
        room.started = false;
        room.players.forEach((p) => { p.ready = false; });
      } else {
        room.turn = room.turn === 'X' ? 'O' : 'X';
      }
      broadcast(room);
      return;
    }

    if (msg.type === 'request_leave') {
      if (room.players.length < 2) {
        send(ws, { type: 'room_closed' });
        rooms.delete(room.id);
        return;
      }
      room.pendingLeave = clientId;
      broadcast(room);
      return;
    }

    if (msg.type === 'cancel_leave') {
      if (room.pendingLeave === clientId) {
        room.pendingLeave = null;
        broadcast(room);
      }
      return;
    }

    if (msg.type === 'respond_leave') {
      if (room.pendingLeave && room.pendingLeave !== clientId) {
        if (msg.approve) {
          room.players.forEach((p) => send(p.ws, { type: 'room_closed' }));
          rooms.delete(room.id);
        } else {
          room.pendingLeave = null;
          broadcast(room);
        }
      }
      return;
    }
  });

  ws.on('close', () => {
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;
    const p = room.players.find((pp) => pp.clientId === clientId);
    if (p) p.ws = null;
    broadcast(room);
    if (room.players.every((pp) => !pp.ws)) scheduleCleanup(roomId);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('Caro online server listening on port ' + PORT);
});
