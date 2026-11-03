const test = require('node:test');
const assert = require('node:assert');

const {
    containsKeyframe,
    containsParameterSet,
    nalUnitTypes,
    parameterSets,
} = require('../dist/H264');

function stap(...units) {
    const parts = [Buffer.from([0x78])];
    for (const unit of units) {
        const length = Buffer.alloc(2);
        length.writeUInt16BE(unit.length);
        parts.push(length, unit);
    }
    return Buffer.concat(parts);
}

test('parses single NAL payloads and returns raw parameter sets', () => {
    const idr = Buffer.from([0x65, 1, 2]);
    const sps = Buffer.from([0x67, 3, 4]);

    assert.deepStrictEqual(nalUnitTypes(idr), [5]);
    assert.strictEqual(containsKeyframe(idr), true);
    assert.strictEqual(containsParameterSet(sps), true);
    assert.strictEqual(parameterSets(sps).sps, sps);
});

test('parses every complete NAL in a STAP-A payload', () => {
    const sps = Buffer.from([0x67, 1]);
    const pps = Buffer.from([0x68, 2]);
    const idr = Buffer.from([0x65, 3]);
    const payload = stap(sps, pps, idr);

    assert.deepStrictEqual(nalUnitTypes(payload), [7, 8, 5]);
    assert.strictEqual(containsKeyframe(payload), true);
    assert.strictEqual(containsParameterSet(payload), true);
    assert.deepStrictEqual(parameterSets(payload), {sps, pps});
});

test('recognizes only the start fragment of a FU-A NAL', () => {
    const idrStart = Buffer.from([0x7c, 0x85, 1]);
    const idrMiddle = Buffer.from([0x7c, 0x05, 2]);
    const spsStart = Buffer.from([0x7c, 0x87, 3]);

    assert.deepStrictEqual(nalUnitTypes(idrStart), [5]);
    assert.strictEqual(containsKeyframe(idrStart), true);
    assert.deepStrictEqual(nalUnitTypes(idrMiddle), []);
    assert.strictEqual(containsKeyframe(idrMiddle), false);
    assert.strictEqual(containsParameterSet(spsStart), true);
    assert.deepStrictEqual(parameterSets(spsStart), {});
});
