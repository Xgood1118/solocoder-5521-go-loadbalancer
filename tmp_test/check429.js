const http = require('http');
// Drain the bucket first with a burst
function req(cb) {
  const r = http.request({ host: '127.0.0.1', port: 8121, path: '/api/429', method: 'GET' }, (res) => {
    cb({ status: res.statusCode, headers: res.headers });
    res.resume();
  });
  r.on('error', e => cb({ status: 'ERR', err: e.message }));
  r.end();
}
const N = 8;
let done = 0;
const results = [];
for (let i = 0; i < N; i++) req(r => { results.push(r); if (++done === N) {
  for (const r of results) {
    if (r.status === 429) {
      console.log('429 headers:',
        'X-RateLimit-Limit=' + r.headers['x-ratelimit-limit'],
        'X-RateLimit-Remaining=' + r.headers['x-ratelimit-remaining'],
        'X-RateLimit-Reset=' + r.headers['x-ratelimit-reset']);
    }
  }
  console.log('all codes:', results.map(r => r.status).join(','));
}});
