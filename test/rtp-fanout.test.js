const test = require('node:test');
const assert = require('node:assert');

// Tests run against dist/ — the compiled output is what actually ships and what
// Homebridge loads, so testing it avoids proving something about source that the
// build could still get wrong. Run `npm run build` first.
const {RtpFanout} = require('../dist/WebRtcSource');

function subscriber(onAudio = () => {}, onVideo = () => {}) {
    return {onAudio, onVideo};
}

test('delivers synchronously to subscribers in subscription order', () => {
    const fanout = new RtpFanout();
    const packet = Buffer.from([1, 2, 3]);
    const deliveries = [];

    fanout.subscribe(subscriber(() => deliveries.push(['first', packet])));
    fanout.subscribe(subscriber(() => deliveries.push(['second', packet])));
    fanout.subscribe(subscriber(() => deliveries.push(['third', packet])));

    fanout.publishAudio(packet);

    assert.deepStrictEqual(deliveries.map(([name]) => name), ['first', 'second', 'third']);
    deliveries.forEach(([, deliveredPacket]) => assert.strictEqual(deliveredPacket, packet));
});

test('unsubscribe is idempotent', () => {
    const fanout = new RtpFanout();
    let deliveries = 0;
    const unsubscribe = fanout.subscribe(subscriber(() => deliveries++));

    fanout.publishAudio(Buffer.alloc(0));
    unsubscribe();
    unsubscribe();
    fanout.publishAudio(Buffer.alloc(0));

    assert.strictEqual(deliveries, 1);
});

test('unsubscribe during delivery is safe for self and other subscribers', () => {
    const fanout = new RtpFanout();
    const deliveries = [];
    let unsubscribeFirst;
    let unsubscribeThird;

    unsubscribeFirst = fanout.subscribe(subscriber(() => {
        deliveries.push('first');
        unsubscribeFirst();
        unsubscribeThird();
    }));
    fanout.subscribe(subscriber(() => deliveries.push('second')));
    unsubscribeThird = fanout.subscribe(subscriber(() => deliveries.push('third')));

    fanout.publishAudio(Buffer.alloc(0));
    fanout.publishAudio(Buffer.alloc(0));

    assert.deepStrictEqual(deliveries, ['first', 'second', 'third', 'second']);
});

test('a throwing subscriber reports the error and does not block others', () => {
    const reported = [];
    const fanout = new RtpFanout(error => reported.push(error));
    const expected = new Error('subscriber failed');
    const deliveries = [];

    fanout.subscribe(subscriber(() => deliveries.push('before')));
    fanout.subscribe(subscriber(() => { throw expected; }));
    fanout.subscribe(subscriber(() => deliveries.push('after')));

    assert.doesNotThrow(() => fanout.publishAudio(Buffer.alloc(0)));
    assert.deepStrictEqual(deliveries, ['before', 'after']);
    assert.deepStrictEqual(reported, [expected]);
});

test('routes audio and video to the matching callback', () => {
    const fanout = new RtpFanout();
    const audio = Buffer.from('audio');
    const video = Buffer.from('video');
    const receivedAudio = [];
    const receivedVideo = [];

    fanout.subscribe(subscriber(
        packet => receivedAudio.push(packet),
        packet => receivedVideo.push(packet)
    ));

    fanout.publishAudio(audio);
    fanout.publishVideo(video);

    assert.deepStrictEqual(receivedAudio, [audio]);
    assert.deepStrictEqual(receivedVideo, [video]);
});
