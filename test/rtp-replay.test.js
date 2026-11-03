const test = require('node:test');
const assert = require('node:assert');

const {RtpReplay} = require('../dist/RtpReplay');

function deferred() {
    let resolve;
    const promise = new Promise(done => resolve = done);
    return {promise, resolve};
}

function packet(value, arrivalMs = value, size = 1) {
    return {kind: 'video', packet: Buffer.alloc(size, value), arrivalMs};
}

function subscriber(received) {
    return {
        onAudio: value => received.push(value),
        onVideo: value => received.push(value),
    };
}

function values(received) {
    return received.map(value => value[0]);
}

test('delivers the snapshot then queued live packets exactly once', async () => {
    const gate = deferred();
    const received = [];
    const replay = new RtpReplay(subscriber(received), [packet(1), packet(2)], gate.promise, {
        batchBytes: 1024,
        tickMs: 1,
    });
    replay.push(packet(3));
    replay.push(packet(4));
    gate.resolve(true);
    await new Promise(resolve => setTimeout(resolve, 5));

    assert.deepStrictEqual(values(received), [1, 2, 3, 4]);
    replay.push(packet(5));
    assert.deepStrictEqual(values(received), [1, 2, 3, 4, 5]);
});

test('packets arriving while the snapshot is draining stay behind it', async () => {
    const gate = deferred();
    const received = [];
    const replay = new RtpReplay(subscriber(received), [packet(1), packet(2), packet(3)], gate.promise, {
        batchBytes: 1,
        tickMs: 2,
    });
    gate.resolve(true);
    await Promise.resolve();
    assert.deepStrictEqual(values(received), [1]);
    replay.push(packet(4));
    await new Promise(resolve => setTimeout(resolve, 10));

    assert.deepStrictEqual(values(received), [1, 2, 3, 4]);
});

test('queue overflow abandons old replay data and attaches live', () => {
    const gate = deferred();
    const received = [];
    const fallbacks = [];
    const replay = new RtpReplay(subscriber(received), [packet(1)], gate.promise, {
        maxQueueBytes: 2,
        onFallback: reason => fallbacks.push(reason),
    });
    replay.push(packet(2, 2, 2));
    replay.push(packet(3, 3, 2));
    replay.push(packet(4, 4, 1));

    assert.deepStrictEqual(values(received), [3, 4]);
    assert.strictEqual(fallbacks.length, 1);
});

test('unsubscribe during replay stops timers and all later delivery', async () => {
    const received = [];
    const replay = new RtpReplay(
        subscriber(received),
        [packet(1), packet(2), packet(3)],
        Promise.resolve(true),
        {batchBytes: 1, tickMs: 20}
    );
    await Promise.resolve();
    assert.deepStrictEqual(values(received), [1]);
    replay.unsubscribe();
    replay.push(packet(4));
    await new Promise(resolve => setTimeout(resolve, 30));

    assert.deepStrictEqual(values(received), [1]);
});
