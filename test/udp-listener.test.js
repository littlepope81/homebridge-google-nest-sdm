const test = require('node:test');
const assert = require('node:assert');
const {createSocket} = require('node:dgram');

const {waitForUdpListener} = require('../dist/NestStreamer');

async function bind(socket, port) {
    await new Promise((resolve, reject) => {
        const onError = error => {
            socket.off('listening', onListening);
            reject(error);
        };
        const onListening = () => {
            socket.off('error', onError);
            resolve();
        };
        socket.once('error', onError);
        socket.once('listening', onListening);
        socket.bind(port, '127.0.0.1');
    });
}

async function close(socket) {
    const closed = new Promise(resolve => socket.once('close', resolve));
    socket.close();
    await closed;
}

async function unusedPort(t) {
    const socket = createSocket('udp4');
    try {
        await bind(socket, 0);
    } catch (error) {
        if (error.code === 'EPERM') {
            t.skip('sandbox does not permit binding a loopback UDP socket');
            return undefined;
        }
        throw error;
    }
    const port = socket.address().port;
    await close(socket);
    return port;
}

test('waits until a UDP listener binds', async (t) => {
    const port = await unusedPort(t);
    if (port === undefined) return;
    const listener = createSocket('udp4');
    const waiting = waitForUdpListener(port, 1000);
    await new Promise(resolve => setTimeout(resolve, 100));
    await bind(listener, port);

    assert.strictEqual(await waiting, true);
    await close(listener);
});

// Only Linux reports the refused probe, so elsewhere the fixed startup window always resolves true.
test('returns false when no UDP listener binds', {skip: process.platform !== 'linux'}, async (t) => {
    const port = await unusedPort(t);
    if (port === undefined) return;

    assert.strictEqual(await waitForUdpListener(port, 150), false);
});
