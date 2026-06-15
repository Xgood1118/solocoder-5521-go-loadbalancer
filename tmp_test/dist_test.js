const http = require('http');

const N = 600;
const counts = {};
let done = 0;
let concurrency = 50;
let inFlight = 0;
let started = 0;

function fire() {
  while (inFlight < concurrency && started < N) {
    started++;
    inFlight++;
    http.get('http://127.0.0.1:8128/', (res) => {
      const id = res.headers['x-backend-id'] || 'unknown';
      counts[id] = (counts[id] || 0) + 1;
      res.resume();
      inFlight--;
      done++;
      if (done === N) {
        console.log('Total:', N);
        console.log('Distribution:');
        const entries = Object.entries(counts).sort();
        for (const [k, v] of entries) {
          const pct = ((v / N) * 100).toFixed(1);
          console.log(`  ${k}: ${v} (${pct}%)`);
        }
        const ids = entries.map(e => e[0]).join(',');
        const vals = entries.map(e => e[1]).join(',');
        console.log(`JSON: ${JSON.stringify(counts)}`);
      } else {
        setImmediate(fire);
      }
    }).on('error', (e) => {
      console.error('err', e.message);
      inFlight--;
      done++;
    });
  }
}
fire();
