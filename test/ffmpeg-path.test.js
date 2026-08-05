const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Tests run against dist/ — the compiled output is what actually ships and what
// Homebridge loads, so testing it avoids proving something about source that the
// build could still get wrong. Run `npm run build` first.
const {
    resolveFfmpegPath,
    probeFfmpeg,
    describeProbeFailure,
    PROBE_TIMEOUT_MS,
} = require('../dist/FfmpegPath');

// ---------------------------------------------------------------------------
// resolveFfmpegPath — the branch every existing user takes is the bundled one,
// so a regression here breaks everybody, not just people who set the option.
// ---------------------------------------------------------------------------

test('resolveFfmpegPath prefers an explicitly configured videoProcessor', () => {
    const result = resolveFfmpegPath({videoProcessor: '/opt/custom/ffmpeg'}, () => '/bundled/ffmpeg');
    assert.strictEqual(result, '/opt/custom/ffmpeg');
});

test('resolveFfmpegPath falls back to the bundled binary when the option is unset', () => {
    // REGRESSION GUARD: this is the default path for every existing installation.
    assert.strictEqual(resolveFfmpegPath({}, () => '/bundled/ffmpeg'), '/bundled/ffmpeg');
    assert.strictEqual(resolveFfmpegPath(undefined, () => '/bundled/ffmpeg'), '/bundled/ffmpeg');
});

test('resolveFfmpegPath falls back to PATH when neither is available', () => {
    assert.strictEqual(resolveFfmpegPath({}, () => undefined), 'ffmpeg');
    assert.strictEqual(resolveFfmpegPath({}, () => null), 'ffmpeg');
    assert.strictEqual(resolveFfmpegPath({}, () => false), 'ffmpeg');
});

test('resolveFfmpegPath treats a blank or whitespace-only option as unset', () => {
    // A cleared field in the Homebridge config UI leaves an empty string behind;
    // spawning '' would be a confusing failure.
    assert.strictEqual(resolveFfmpegPath({videoProcessor: ''}, () => '/bundled/ffmpeg'), '/bundled/ffmpeg');
    assert.strictEqual(resolveFfmpegPath({videoProcessor: '   '}, () => '/bundled/ffmpeg'), '/bundled/ffmpeg');
});

test('resolveFfmpegPath trims surrounding whitespace from the configured path', () => {
    assert.strictEqual(resolveFfmpegPath({videoProcessor: '  /opt/ffmpeg  '}, () => '/b'), '/opt/ffmpeg');
});

test('resolveFfmpegPath preserves a path containing spaces', () => {
    // Safe because the path is passed to spawn as the command with an argv array,
    // never embedded in the whitespace-split live-stream argument string.
    assert.strictEqual(resolveFfmpegPath({videoProcessor: '/Applications/My Tools/ffmpeg'}, () => '/b'),
        '/Applications/My Tools/ffmpeg');
});

// ---------------------------------------------------------------------------
// probeFfmpeg — each failure kind needs its own message to be actionable.
// ---------------------------------------------------------------------------

test('probeFfmpeg reports not_found for a nonexistent binary', async () => {
    const result = await probeFfmpeg(path.join(os.tmpdir(), 'definitely-not-ffmpeg-' + Date.now()));
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.kind, 'not_found');
});

test('probeFfmpeg reports not_found when arguments are baked into the path', async () => {
    // spawn treats the whole string as one filename, which is exactly why the schema
    // and README say "path to the binary only".
    const result = await probeFfmpeg('/usr/bin/ffmpeg -hide_banner');
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.kind, 'not_found');
});

test('probeFfmpeg reports not_executable for a file without the execute bit', async () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'ffprobe-test-')), 'ffmpeg');
    fs.writeFileSync(f, '#!/bin/sh\necho hi\n', {mode: 0o644});
    const result = await probeFfmpeg(f);
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.kind, 'not_executable');
});

test('probeFfmpeg reports exit_error when the binary runs but fails', async () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'ffprobe-test-')), 'ffmpeg');
    fs.writeFileSync(f, '#!/bin/sh\nexit 3\n', {mode: 0o755});
    const result = await probeFfmpeg(f);
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.kind, 'exit_error');
});

test('probeFfmpeg reports timeout for a binary that hangs', async () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'ffprobe-test-')), 'ffmpeg');
    fs.writeFileSync(f, '#!/bin/sh\nsleep 30\n', {mode: 0o755});
    const result = await probeFfmpeg(f, 300);
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.kind, 'timeout');
});

test('probeFfmpeg reports missing_libfdk_aac when the encoder is absent', async () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'ffprobe-test-')), 'ffmpeg');
    fs.writeFileSync(f, '#!/bin/sh\necho " V..... libx264   H.264"\n', {mode: 0o755});
    const result = await probeFfmpeg(f);
    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.kind, 'missing_libfdk_aac');
});

test('probeFfmpeg succeeds when libfdk_aac is present', async () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'ffprobe-test-')), 'ffmpeg');
    fs.writeFileSync(f, '#!/bin/sh\necho " A..... libfdk_aac  Fraunhofer FDK AAC"\n', {mode: 0o755});
    assert.deepStrictEqual(await probeFfmpeg(f), {ok: true});
});

test('probeFfmpeg detects libfdk_aac reported on stderr', async () => {
    const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'ffprobe-test-')), 'ffmpeg');
    fs.writeFileSync(f, '#!/bin/sh\necho " A..... libfdk_aac" 1>&2\n', {mode: 0o755});
    assert.deepStrictEqual(await probeFfmpeg(f), {ok: true});
});

test('every failure kind has a distinct, non-empty message', () => {
    const kinds = ['not_found', 'not_executable', 'timeout', 'exit_error', 'missing_libfdk_aac'];
    const messages = kinds.map(kind => describeProbeFailure({ok: false, kind, detail: 'X'}));
    messages.forEach(m => assert.ok(m && m.length > 20, 'message should be substantive'));
    assert.strictEqual(new Set(messages).size, kinds.length, 'messages must be distinct');
    // The one that costs the user audio must name the encoder so it is searchable.
    assert.match(describeProbeFailure({ok: false, kind: 'missing_libfdk_aac', detail: 'X'}), /libfdk_aac/);
});

test('PROBE_TIMEOUT_MS is bounded so startup cannot hang', () => {
    assert.ok(PROBE_TIMEOUT_MS > 0 && PROBE_TIMEOUT_MS <= 30000);
});

// ---------------------------------------------------------------------------
// Integration: the resolved path must actually REACH both spawn sites.
// Without this, the resolver could be perfect while a leftover require() kept
// running in the real code path and every unit test above would still pass.
// ---------------------------------------------------------------------------

test('no consumer resolves ffmpeg for itself — FfmpegPath is the only place that does', () => {
    const dist = path.join(__dirname, '..', 'dist');
    for (const file of ['FfMpegProcess.js', 'HksvStreamer.js', 'StreamingDelegate.js', 'Platform.js']) {
        const src = fs.readFileSync(path.join(dist, file), 'utf8');
        assert.ok(!src.includes('ffmpeg-for-homebridge'),
            `${file} must not resolve ffmpeg itself; Platform passes the path down`);
    }
    assert.ok(fs.readFileSync(path.join(dist, 'FfmpegPath.js'), 'utf8').includes('ffmpeg-for-homebridge'),
        'FfmpegPath.js is the one place the bundled binary is looked up');
});

test('both consumers accept an injected ffmpeg path and use it verbatim', () => {
    const HksvStreamer = require('../dist/HksvStreamer').default;
    const log = {debug() {}, info() {}, warn() {}, error() {}};
    const nestStream = {args: '-i pipe:0', stdin: null};

    const streamer = new HksvStreamer(log, nestStream, [], [], false, '/injected/ffmpeg');
    assert.strictEqual(streamer.ffmpegPath, '/injected/ffmpeg',
        'HksvStreamer must use the path it was given');
    streamer.server.close();

    // FfmpegProcess spawns on construction, so assert on its signature instead of
    // building one: the path must be a declared parameter, not resolved internally.
    const FfmpegProcess = require('../dist/FfMpegProcess').FfmpegProcess;
    assert.ok(FfmpegProcess.length >= 8,
        'FfmpegProcess must take the ffmpeg path as a constructor parameter');
});
