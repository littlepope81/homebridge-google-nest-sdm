const test = require('node:test');
const assert = require('node:assert');

const {SharedWebRtcSource} = require('../dist/SharedWebRtcSource');

const log = {
    debug() {},
    info() {},
    warn() {},
    error() {},
};

class FakeSource {
    constructor(options, startError) {
        this.options = options;
        this.startError = startError;
        this.lastRtpAt = undefined;
        this.startCalls = 0;
        this.stopCalls = 0;
        this.attachCalls = 0;
        this.detachCalls = 0;
        this.consumerCount = 0;
        this.subscribers = new Set();
        this.replayCalls = [];
        this.parameterSets = undefined;
    }

    async start() {
        this.startCalls++;
        if (this.startError)
            throw this.startError;
    }

    async stop() {
        this.stopCalls++;
    }

    subscribe(subscriber) {
        this.subscribers.add(subscriber);
        let subscribed = true;
        return () => {
            if (!subscribed) return;
            subscribed = false;
            this.subscribers.delete(subscriber);
        };
    }

    subscribeWithReplay(subscriber, targetMs, ready) {
        this.replayCalls.push({subscriber, targetMs, ready});
        return this.subscribe(subscriber);
    }

    getParameterSets() {
        return this.parameterSets;
    }

    attachConsumer() {
        this.attachCalls++;
        this.consumerCount++;
        let attached = true;
        return () => {
            if (!attached) return;
            attached = false;
            this.detachCalls++;
            this.consumerCount--;
        };
    }

    fail(reason = 'fake failure') {
        this.options.onFailure(reason);
    }

    emitVideo(packet = Buffer.from('video')) {
        for (const subscriber of Array.from(this.subscribers))
            subscriber.onVideo(packet);
    }
}

function makeShared(startErrors = []) {
    const sources = [];
    const shared = new SharedWebRtcSource(log, 'Front Door', options => {
        const source = new FakeSource(options, startErrors[sources.length]);
        sources.push(source);
        return source;
    });
    return {shared, sources};
}

async function flush() {
    for (let i = 0; i < 4; i++)
        await Promise.resolve();
    await new Promise(resolve => setImmediate(resolve));
    for (let i = 0; i < 4; i++)
        await Promise.resolve();
}

function subscriber(onVideo = () => {}) {
    return {onAudio() {}, onVideo};
}

test('does not start until wanted', async () => {
    const {shared, sources} = makeShared();

    assert.strictEqual(shared.subscribe(subscriber()), undefined);
    assert.strictEqual(sources.length, 0);

    shared.setWanted(true);
    await flush();

    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].startCalls, 1);
    await shared.shutdown();
});

test('subscribe rejects a source before RTP and when RTP is stale', async (t) => {
    t.mock.timers.enable({apis: ['Date', 'setTimeout'], now: 10_000});
    const {shared, sources} = makeShared();
    shared.setWanted(true);
    await flush();
    const source = sources[0];

    assert.strictEqual(shared.subscribe(subscriber()), undefined);
    source.lastRtpAt = Date.now() - 5_001;
    assert.strictEqual(shared.subscribe(subscriber()), undefined);

    source.lastRtpAt = Date.now();
    const unsubscribe = shared.subscribe(subscriber());
    assert.strictEqual(typeof unsubscribe, 'function');
    unsubscribe();
    await shared.shutdown();
});

test('healthy subscription delivers and unsubscribe uncounts exactly once', async () => {
    const {shared, sources} = makeShared();
    shared.setWanted(true);
    await flush();
    const source = sources[0];
    source.lastRtpAt = Date.now();
    const packets = [];

    const unsubscribe = shared.subscribe(subscriber(packet => packets.push(packet)));
    assert.strictEqual(source.attachCalls, 1);
    assert.strictEqual(source.consumerCount, 1);
    source.emitVideo();
    assert.strictEqual(packets.length, 1);

    unsubscribe();
    unsubscribe();
    assert.strictEqual(source.detachCalls, 1);
    assert.strictEqual(source.consumerCount, 0);
    source.emitVideo();
    assert.strictEqual(packets.length, 1);
    await shared.shutdown();
});

test('replay subscription uses the same health and consumer accounting', async () => {
    const {shared, sources} = makeShared();
    shared.setWanted(true);
    await flush();
    const source = sources[0];
    const ready = Promise.resolve(true);

    assert.strictEqual(shared.subscribeWithReplay(subscriber(), 8000, ready), undefined);
    source.lastRtpAt = Date.now();
    source.parameterSets = {sps: 'sps', pps: 'pps'};
    const unsubscribe = shared.subscribeWithReplay(subscriber(), 8000, ready);

    assert.strictEqual(source.replayCalls.length, 1);
    assert.strictEqual(source.replayCalls[0].targetMs, 8000);
    assert.strictEqual(source.replayCalls[0].ready, ready);
    assert.strictEqual(source.consumerCount, 1);
    assert.deepStrictEqual(shared.getParameterSets(), source.parameterSets);

    unsubscribe();
    assert.strictEqual(source.consumerCount, 0);
    await shared.shutdown();
});

test('failure rebuilds with capped backoff and never moves old subscribers to a new generation', async (t) => {
    t.mock.timers.enable({apis: ['Date', 'setTimeout'], now: 1_000});
    const {shared, sources} = makeShared();
    shared.setWanted(true);
    await flush();

    const first = sources[0];
    first.lastRtpAt = Date.now();
    let oldDeliveries = 0;
    shared.subscribe(subscriber(() => oldDeliveries++));
    first.fail();
    await flush();

    assert.strictEqual(first.stopCalls, 1);
    assert.strictEqual(first.detachCalls, 1);
    t.mock.timers.tick(4_999);
    await flush();
    assert.strictEqual(sources.length, 1);
    t.mock.timers.tick(1);
    await flush();
    assert.strictEqual(sources.length, 2);
    sources[1].lastRtpAt = Date.now();
    sources[1].emitVideo();
    assert.strictEqual(oldDeliveries, 0);

    const delays = [30_000, 120_000, 300_000, 300_000];
    for (const delay of delays) {
        const current = sources[sources.length - 1];
        current.fail();
        await flush();
        t.mock.timers.tick(delay - 1);
        await flush();
        const countBefore = sources.length;
        t.mock.timers.tick(1);
        await flush();
        assert.strictEqual(sources.length, countBefore + 1);
    }

    await shared.shutdown();
});

test('setWanted(false) stops the source and cancels a pending rebuild', async (t) => {
    t.mock.timers.enable({apis: ['Date', 'setTimeout'], now: 1_000});
    const {shared, sources} = makeShared();
    shared.setWanted(true);
    await flush();
    sources[0].lastRtpAt = Date.now();
    shared.subscribe(subscriber());

    shared.setWanted(false);
    await flush();
    assert.strictEqual(sources[0].stopCalls, 1);
    assert.strictEqual(sources[0].detachCalls, 1);

    shared.setWanted(true);
    await flush();
    assert.strictEqual(sources.length, 2);
    sources[1].fail();
    await flush();

    shared.setWanted(false);
    t.mock.timers.tick(60_000);
    await flush();

    assert.strictEqual(sources.length, 2);
    assert.strictEqual(sources[1].stopCalls, 1);
    await shared.shutdown();
});

test('shutdown is permanent and idempotent', async () => {
    const {shared, sources} = makeShared();
    shared.setWanted(true);
    await flush();

    const firstShutdown = shared.shutdown();
    const secondShutdown = shared.shutdown();
    assert.strictEqual(firstShutdown, secondShutdown);
    await firstShutdown;
    assert.strictEqual(sources[0].stopCalls, 1);

    shared.setWanted(true);
    await flush();
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(shared.subscribe(subscriber()), undefined);
});

test('start rejection stops the failed generation and rebuilds with backoff', async (t) => {
    t.mock.timers.enable({apis: ['Date', 'setTimeout'], now: 1_000});
    const {shared, sources} = makeShared([new Error('start exploded')]);
    shared.setWanted(true);
    await flush();

    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].startCalls, 1);
    assert.strictEqual(sources[0].stopCalls, 1);
    t.mock.timers.tick(5_000);
    await flush();

    assert.strictEqual(sources.length, 2);
    assert.strictEqual(sources[1].startCalls, 1);
    await shared.shutdown();
});
