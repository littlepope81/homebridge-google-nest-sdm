const test = require('node:test');
const assert = require('node:assert');

const {RtpRing} = require('../dist/RtpRing');

function entry(kind, value, arrivalMs, size = 1) {
    return {kind, packet: Buffer.alloc(size, value), arrivalMs};
}

test('evicts oldest entries by age', () => {
    const ring = new RtpRing(100, 1024);
    ring.push(entry('video', 1, 0), {keyframeStart: true});
    ring.push(entry('audio', 2, 50));
    ring.push(entry('video', 3, 101));

    assert.strictEqual(ring.snapshot(0, 101), undefined);
});

test('evicts oldest entries by total bytes', () => {
    const ring = new RtpRing(1000, 5);
    ring.push(entry('video', 1, 0, 3), {keyframeStart: true});
    ring.push(entry('video', 2, 1, 3));

    assert.strictEqual(ring.snapshot(0, 1), undefined);
});

test('selects the newest keyframe old enough, otherwise the oldest keyframe', () => {
    const ring = new RtpRing();
    const first = entry('video', 1, 0);
    const middle = entry('video', 2, 5000);
    const newest = entry('video', 3, 9000);
    ring.push(first, {keyframeStart: true});
    ring.push(middle, {keyframeStart: true});
    ring.push(newest, {keyframeStart: true});

    assert.strictEqual(ring.snapshot(4000, 10000)[0], middle);
    assert.strictEqual(ring.chosenAgeMs, 5000);
    assert.strictEqual(ring.snapshot(12000, 10000)[0], first);
    assert.strictEqual(ring.chosenAgeMs, 10000);
});

test('starts audio with the chosen video keyframe', () => {
    const ring = new RtpRing();
    const earlyAudio = entry('audio', 1, 10);
    const keyframe = entry('video', 2, 20);
    const audio = entry('audio', 3, 21);
    const video = entry('video', 4, 22);
    ring.push(earlyAudio);
    ring.push(keyframe, {keyframeStart: true});
    ring.push(audio);
    ring.push(video);

    assert.deepStrictEqual(ring.snapshot(0, 30), [keyframe, audio, video]);
});

test('returns undefined when no keyframe is buffered', () => {
    const ring = new RtpRing();
    ring.push(entry('audio', 1, 0));
    ring.push(entry('video', 2, 1));

    assert.strictEqual(ring.snapshot(100, 100), undefined);
    assert.strictEqual(ring.chosenAgeMs, undefined);
});

test('keeps keyframe bookkeeping consistent after eviction and late marking', () => {
    const ring = new RtpRing(1000, 4);
    const evicted = entry('video', 1, 0, 3);
    const candidate = entry('video', 2, 1, 2);
    ring.push(evicted, {keyframeStart: true});
    ring.push(candidate);
    assert.strictEqual(ring.markKeyframeStart(candidate.packet), true);

    assert.deepStrictEqual(ring.snapshot(0, 2), [candidate]);
    assert.strictEqual(ring.markKeyframeStart(evicted.packet), false);
});
