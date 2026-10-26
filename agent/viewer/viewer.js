import RFB from './novnc/core/rfb.js';

// The existing door negotiates replacement backends but does not replay client
// display state. Use stateless Raw rectangles and reassert the exact pixel
// format plus a full update when the wheel changes. These hooks are pinned to
// noVNC 1.6.0; upstream stays unmodified.
class SeatRFB extends RFB {
    _sendEncodings() {
        RFB.messages.clientEncodings(this._sock, [0, -223, -224]); // Raw, DesktopSize, LastRect
    }
    refresh() {
        RFB.messages.pixelFormat(this._sock, 24, true);
        this._sendEncodings();
        RFB.messages.fbUpdateRequest(this._sock, false, 0, 0, this._fbWidth, this._fbHeight);
    }
}

const el = id => document.getElementById(id);
const base = location.pathname.replace(/\/viewer$/, '');
let bearer = '', rfb, stopped = false, pollTimer, clockTimer, deadline = 0, busy = false, screenReady = false, lastDriver = null, actionEpoch = 0, reconnectSince = null;
const wsURL = new URL(base + '/viewer-session', location.href);
wsURL.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
const socket = new WebSocket(wsURL, ['binary']);
socket.binaryType = 'arraybuffer';

function stop(message) {
    if (stopped) return;
    stopped = true;
    clearTimeout(pollTimer); clearInterval(clockTimer);
    el('message').textContent = message;
    el('take').disabled = el('release').disabled = true;
    el('note').disabled = true;
    bearer = '';
    if (rfb) rfb.disconnect();
    socket.close();
}
function driving(driver) {
    const known = ['AI agent', 'you', 'a person'].includes(driver);
    if (screenReady && known && lastDriver !== null && lastDriver !== 'Cannot check' && lastDriver !== driver) rfb.refresh();
    lastDriver = driver;
    el('driver').textContent = driver;
    if (rfb) rfb.viewOnly = driver !== 'you';
    el('take').disabled = stopped || busy || !screenReady || !known || driver === 'you';
    el('release').disabled = stopped || busy || !screenReady || !known || driver !== 'you';
}
function remaining(seconds) {
    deadline = performance.now() + seconds * 1000;
    tick();
}
function tick() {
    const seconds = Math.max(0, Math.ceil((deadline - performance.now()) / 1000));
    el('remaining').textContent = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')} remaining`;
    if (!seconds) stop('The one-hour viewing limit has ended. Reconnect to continue.');
    reconnectTimedOut();
}
function terminal(response, body) {
    if (body.error?.code === 'seat_ended') {
        stop('This Seat has ended'); return true;
    }
    if (body.error?.code === 'takeover_session_ended') {
        stop(body.receipt?.session_end_reason === 'session_cap_expired'
            ? 'The one-hour viewing limit has ended. Reconnect to continue.'
            : 'The viewing session has ended. Reconnect to continue.');
        return true;
    }
    if (response.status === 401 || response.status === 403) {
        stop('Viewing access ended. Reconnect to continue.'); return true;
    }
    return false;
}
function reconnectTimedOut() {
    if (reconnectSince === null || performance.now() - reconnectSince < 30000) return false;
    stop('Cannot reconnect to the Seat. Reopen the viewer to try again.');
    return true;
}
function cannotCheck() {
    driving('Cannot check');
    if (reconnectTimedOut()) return;
    if (reconnectSince !== null) {
        el('message').textContent = 'Cannot check the Seat. Reconnecting…';
    } else {
        el('message').textContent = 'Cannot check the Seat. Retrying…';
    }
}
async function poll() {
    if (stopped || !bearer) return;
    const epoch = actionEpoch;
    try {
        const response = await fetch(base + '/viewer-status', {headers: {'X-Wefty-Control-Token': bearer}, cache: 'no-store'});
        const body = await response.json().catch(() => ({}));
        if (stopped || epoch !== actionEpoch || busy) return;
        if (terminal(response, body)) return;
        if (response.status === 404 && reconnectSince === null) reconnectSince = performance.now();
        if (!response.ok) throw new Error('Cannot check the Seat. Retrying…');
        reconnectSince = null;
        driving(body.driver); remaining(body.remaining_seconds);
        el('message').textContent = socket.readyState === WebSocket.OPEN ? 'Watching the Seat live.' : 'Screen disconnected. Reconnect to continue.';
    } catch (error) {
        if (stopped || epoch !== actionEpoch || busy) return;
        cannotCheck();
    } finally {
        if (!stopped) pollTimer = setTimeout(poll, 3000);
    }
}
async function action(name) {
    if (busy || stopped || !bearer || !screenReady) return;
    const note = el('note').value;
    if (name === 'release' && new TextEncoder().encode(note).length > 8192) {
        el('message').textContent = 'Release note must be at most 8192 bytes.'; return;
    }
    actionEpoch++;
    busy = true; driving(el('driver').textContent);
    try {
        const response = await fetch(base + '/wefty/control/' + name, {
            method: 'POST', headers: {'X-Wefty-Control-Token': bearer, 'Content-Type': 'application/json'},
            body: JSON.stringify(name === 'release' ? {note} : {})
        });
        const body = await response.json().catch(() => ({}));
        if (stopped) return;
        if (terminal(response, body)) return;
        if (!response.ok) throw new Error(body.error?.message || 'Control action was refused.');
        driving(name === 'take' ? 'you' : 'AI agent');
        if (name === 'release') el('note').value = '';
        el('message').textContent = name === 'take' ? 'You drive. The AI agent can still run commands.' : 'Control released to the AI agent.';
    } catch (error) { if (!stopped) el('message').textContent = error.message; }
    finally { actionEpoch++; busy = false; driving(el('driver').textContent); }
}
el('take').addEventListener('click', () => action('take'));
el('release').addEventListener('click', () => action('release'));
socket.onmessage = event => {
    if (stopped) return;
    try {
        const session = JSON.parse(event.data);
        if (typeof session.bearer !== 'string' || !session.bearer || !Number.isFinite(session.remaining_seconds)) throw new Error();
        bearer = session.bearer;
        rfb = new SeatRFB(el('screen'), socket);
        rfb.viewOnly = true; rfb.scaleViewport = true; rfb.resizeSession = false;
        remaining(session.remaining_seconds);
        if (stopped) return;
        clockTimer = setInterval(tick, 250);
        rfb.addEventListener('connect', () => { screenReady = true; driving(lastDriver || 'AI agent'); });
        rfb.addEventListener('disconnect', () => { screenReady = false; el('take').disabled = el('release').disabled = true; });
        poll();
    } catch (error) { stop('Could not open the Seat screen. Reconnect to continue.'); }
};
socket.onerror = () => { if (!bearer) stop('Could not open the Seat screen. Check your administrator access and reconnect.'); };
window.addEventListener('pagehide', () => stop('Viewing session closed.'));
