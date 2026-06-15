const http = require('http');
const N = 30;
let done = 0, codes = {};
for (let i = 0; i < N; i++) {
  const req = http.request({ host: '127.0.0.1', port: 8121, path: '/api/burst', method: 'GET' }, (res) => {
    codes[res.statusCode] = (codes[res.statusCode] || 0) + 1;
    res.resume();
    if (++done === N) console.log('codes:', JSON.stringify(codes));
  });
  req.on('error', e => { codes['ERR'] = (codes['ERR'] || 0) + 1; if (++done === N) console.log('codes:', JSON.stringify(codes)); });
  req.end();
}
