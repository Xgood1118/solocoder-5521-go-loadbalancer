const http = require('http');

function get(path) {
  return new Promise((resolve, reject) => {
    http.get('http://127.0.0.1:8129' + path, (res) => {
      let body = '';
      res.on('data', (c) => body += c);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          resolve({ raw: body });
        }
      });
    }).on('error', reject);
  });
}

function getN(n) {
  return new Promise((resolve, reject) => {
    let done = 0;
    for (let i = 0; i < n; i++) {
      http.get('http://127.0.0.1:8128/', (res) => {
        res.resume();
        res.on('end', () => {
          done++;
          if (done === n) resolve();
        });
      }).on('error', reject);
    }
  });
}

async function main() {
  // Phase 1: send 100 requests, snapshot with window=10s
  console.log('Phase 1: send 100 reqs, snapshot with 10s window');
  await getN(100);
  const snap10s = await get('/stats?window=10s');
  console.log('  window=10s result:', JSON.stringify(snap10s));

  // Wait 12 seconds (more than 10s)
  console.log('Phase 2: sleep 12s...');
  await new Promise(r => setTimeout(r, 12000));

  // Now snapshot with 5s window (these 100 should be outside)
  const snap5s = await get('/stats?window=5s');
  console.log('  window=5s result (after 12s):', JSON.stringify(snap5s));

  const snapDefault = await get('/stats');
  console.log('  window=default result:', JSON.stringify(snapDefault));

  // Send another 50 in the new window
  console.log('Phase 3: send 50 more reqs after 12s wait');
  await getN(50);
  const snap5sAfter = await get('/stats?window=5s');
  console.log('  window=5s result (after 50 more):', JSON.stringify(snap5sAfter));
}

main().catch(e => { console.error(e); process.exit(1); });
