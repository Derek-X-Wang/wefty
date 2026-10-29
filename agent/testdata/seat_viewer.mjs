// Exercises the shipped page controller with a deterministic browser boundary.
// noVNC's renderer and a real desktop are covered by the attended browser lane.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = fs.readFileSync(new URL('../viewer/viewer.js', import.meta.url), 'utf8').replace(/^import RFB from .*;\n/, '');
const flush = () => new Promise(resolve => setImmediate(resolve));
function browser(prefix = '') {
    const elements = new Map();
    const element = id => {
        if (!elements.has(id)) elements.set(id, {textContent: '', value: '', disabled: false, handlers: {},
            addEventListener(name, callback) { this.handlers[name] = callback; }});
        return elements.get(id);
    };
    const requests = [], timeouts = new Map(), intervals = new Map();
    let timerID = 0, now = 0, screen, socket, hold = false, resume;
    let actionStatus = 200, actionPlain = false;
    let nextDelay;
    let state = {driver: 'AI agent', remaining_seconds: 3600}, status = 200;
    class WS {
        static OPEN = 1;
        constructor(url, protocols) { this.url = String(url); this.protocols = protocols; this.readyState = 1; socket = this; }
        close() { this.readyState = 3; }
    }
    class RFB {
        constructor(target, channel) { this.channel = channel; this._sock = {calls: []}; this._fbWidth = 1280; this._fbHeight = 800; this.viewOnly = true; screen = this; }
        addEventListener(name, callback) { if (name === 'connect') callback(); }
        disconnect() { socket.close(); }
    }
    RFB.messages = {pixelFormat(sock, ...args) {sock.calls.push(['pixelFormat', ...args]);}, clientEncodings(sock, encodings) {sock.calls.push(['encodings', ...encodings]);}, fbUpdateRequest(sock, ...args) {sock.calls.push(['update', ...args]);}};
    const context = {RFB, WebSocket: WS, URL, TextEncoder, performance: {now: () => now},
        location: {pathname: prefix + '/v1/seats/seat_test/person/viewer', href: 'http://seat-node' + prefix + '/v1/seats/seat_test/person/viewer', protocol: 'http:'},
        document: {getElementById: element}, window: {addEventListener(name, callback) { if (name === 'connect') callback(); }},
        setTimeout(callback, delay) {nextDelay = delay; timeouts.set(++timerID, callback); return timerID;}, clearTimeout(id) {timeouts.delete(id);},
        setInterval(callback) {intervals.set(++timerID, callback); return timerID;}, clearInterval(id) {intervals.delete(id);},
        async fetch(url, options) {
            requests.push({url, options});
            if (url.endsWith('/viewer-status') && hold) await new Promise(resolve => { resume = resolve; });
            if (url.endsWith('/viewer-status')) return {status, ok: status === 200, async json() { return state; }};
            return {status: actionStatus, ok: actionStatus === 200, async json() {if (actionPlain) throw new SyntaxError('Unexpected private transport text'); return {};}};
        }};
    vm.runInNewContext(source, context);
    return {element, requests, timeouts, intervals, get pollDelay() {return nextDelay;}, actionReply(code, plain = false) {actionStatus = code; actionPlain = plain;}, get screen() {return screen;}, get socket() {return socket;},
        holdNextPoll() {hold = true;}, releasePoll() {hold = false; resume();},
        async connect() {socket.onmessage({data: JSON.stringify({bearer: 'private-control-bearer', remaining_seconds: 3600})}); await flush();},
        async poll(body, code = 200) {state = body; status = code; const [id, callback] = [...timeouts][0]; timeouts.delete(id); await callback(); await flush();},
        tick(milliseconds) {now += milliseconds; for (const callback of [...intervals.values()]) callback();}};
}

{
    const b = browser(); await b.connect();
    assert.equal(b.screen.viewOnly, true);
    assert.equal(b.element('driver').textContent, 'AI agent');
    assert.equal(b.element('take').disabled, false);
    assert.equal(b.pollDelay, 3000);
    assert.ok(!b.socket.url.includes('private-control-bearer'));
    assert.equal(b.requests[0].options.headers['X-Wefty-Control-Token'], 'private-control-bearer');
    await b.element('take').handlers.click(); await flush();
    assert.equal(b.screen.viewOnly, false);
    assert.equal(b.element('driver').textContent, 'you');
    assert.deepEqual(b.screen._sock.calls.map(c => Array.from(c)), [['pixelFormat',24,true],['encodings',0,-223,-224],['update',false,0,0,1280,800]]);
    b.element('note').value = 'The browser is ready.';
    await b.element('release').handlers.click(); await flush();
    assert.equal(b.screen.viewOnly, true);
    const release = b.requests.find(r => r.url.endsWith('/release'));
    assert.equal(JSON.parse(release.options.body).note, 'The browser is ready.');
    assert.ok(b.requests.every(r => !r.url.includes('private-control-bearer')));
    await b.poll({error: {code: 'seat_ended'}}, 409);
    assert.equal(b.element('message').textContent, 'This Seat has ended');
    assert.equal(b.element('take').disabled, true);
    assert.equal(b.socket.readyState, 3);
    assert.equal(b.timeouts.size, 0);
}
{
    const b = browser(); await b.connect();
    await b.poll({}, 404);
    assert.equal(b.element('message').textContent, 'Cannot check the Seat. Reconnecting…');
    assert.equal(b.element('take').disabled, true);
    assert.equal(b.socket.readyState, 1);
    assert.equal(b.timeouts.size, 1);
    b.tick(3000);
    await b.poll({driver: 'AI agent', remaining_seconds: 3597});
    assert.equal(b.element('message').textContent, 'Watching the Seat live.');
    assert.equal(b.element('take').disabled, false);
}
{
    const b = browser(); await b.connect();
    await b.poll({}, 404);
    for (let i = 0; i < 9; i++) {b.tick(3000); await b.poll({}, 404);}
    assert.equal(b.socket.readyState, 1);
    b.holdNextPoll();
    const pending = b.poll({}, 404);
    b.tick(3000); // The deadline also fires while a status fetch is still pending.
    b.releasePoll(); await pending;
    assert.equal(b.element('message').textContent, 'Cannot reconnect to the Seat. Reopen the viewer to try again.');
    assert.notEqual(b.element('message').textContent, 'This Seat has ended');
    assert.equal(b.socket.readyState, 3);
    assert.equal(b.timeouts.size, 0);
}
{
    const b = browser(); await b.connect();
    b.tick(3600000);
    assert.match(b.element('message').textContent, /one-hour.*Reconnect/);
    assert.equal(b.socket.readyState, 3);
}
{
    const b = browser(); await b.connect();
    await b.poll({error: {code: 'takeover_session_ended'}, receipt: {session_end_reason: 'session_cap_expired'}}, 410);
    assert.match(b.element('message').textContent, /one-hour.*Reconnect/);
}
{
    const b = browser(); await b.connect();
    await b.poll({error: {code: 'unavailable'}}, 503);
    assert.equal(b.element('message').textContent, 'Cannot check the Seat. Retrying…');
    assert.equal(b.element('driver').textContent, 'Cannot check');
    assert.equal(b.screen.viewOnly, true);
    assert.equal(b.element('take').disabled, true);
    assert.equal(b.socket.readyState, 1);
}
{
    const b = browser(); await b.connect();
    b.holdNextPoll();
    const pending = b.poll({driver: 'AI agent', remaining_seconds: 1});
    b.tick(3600000);
    b.releasePoll(); await pending;
    assert.match(b.element('message').textContent, /one-hour.*Reconnect/);
    assert.equal(b.timeouts.size, 0);
}
{
    const b = browser(); await b.connect();
    b.holdNextPoll();
    const pending = b.poll({driver: 'AI agent', remaining_seconds: 3599});
    await b.element('take').handlers.click(); await flush();
    b.releasePoll(); await pending;
    assert.equal(b.element('driver').textContent, 'you');
    assert.equal(b.screen.viewOnly, false);
}
{
    const b = browser(); await b.connect();
    b.actionReply(503, true);
    await b.element('take').handlers.click(); await flush();
    assert.equal(b.element('message').textContent, 'Control action was refused.');
    assert.equal(b.element('driver').textContent, 'AI agent');
    assert.equal(b.screen.viewOnly, true);
}
console.log('viewer page: view, take, note release, Seat end, cap and unavailable passed');

{
    const prefix = '/random-relay-prefix-for-test';
    const b = browser(prefix); await b.connect();
    assert.equal(b.socket.url, 'ws://seat-node' + prefix + '/v1/seats/seat_test/person/viewer-session');
    await b.element('take').handlers.click(); await flush();
    await b.element('release').handlers.click(); await flush();
    assert.ok(b.requests.every(r => r.url.startsWith(prefix + '/v1/seats/seat_test/person/')));
    assert.ok(b.requests.some(r => r.url.endsWith('/viewer-status')));
    assert.ok(b.requests.some(r => r.url.endsWith('/take')));
    assert.ok(b.requests.some(r => r.url.endsWith('/release')));
}
