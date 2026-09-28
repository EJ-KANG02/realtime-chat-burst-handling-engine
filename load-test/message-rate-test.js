import ws from 'k6/ws';
import { Trend, Counter } from 'k6/metrics';

const latency = new Trend('ws_message_latency', true);
const sent = new Counter('ws_messages_sent');
const received = new Counter('ws_messages_received');

export const options = {
  scenarios: {
    message_rate_burst: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 50),
      duration: __ENV.DURATION || '30s',
      gracefulStop: '2s',
    },
  },
};

const url = 'ws://localhost:8080/chat';
const msgIntervalMs = Number(__ENV.MSG_INTERVAL_MS || 500);

export default function () {
  const pending = new Map();
  let counter = 0;

  ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      socket.setInterval(function () {
        const id = `${__VU}-${counter}`;
        counter = counter + 1;
        const sentAt = Date.now();

        pending.set(id, sentAt);
        socket.send(JSON.stringify({ id: id, sentAt: sentAt }));
        sent.add(1);
      }, msgIntervalMs);
    });

    socket.on('message', function (data) {
      const payload = JSON.parse(data);
      const sentAt = pending.get(payload.id);

      if (sentAt !== undefined) {
        latency.add(Date.now() - sentAt);
        pending.delete(payload.id);
      }

      received.add(1);
    });

    socket.on('error', function (e) {
      console.log('websocket error: ' + e.error());
    });
  });
}
