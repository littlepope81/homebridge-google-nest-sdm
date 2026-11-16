"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const buffer = require("buffer");
// Node 25 removed buffer.SlowBuffer. buffer-equal-constant-time (pulled in by
// google-auth-library -> jws -> jwa, still current upstream) reads
// SlowBuffer.prototype at load time, so on Node 25+ requiring Pub/Sub throws and
// Homebridge refuses to load the plugin at all. Nothing in that chain calls
// SlowBuffer itself; it only needs an object with a prototype to read. This must
// be imported before anything that loads google-auth-library.
const exported = buffer;
if (exported.SlowBuffer === undefined) {
    try {
        Object.defineProperty(buffer, 'SlowBuffer', {
            value: function SlowBuffer(size) {
                return Buffer.allocUnsafeSlow(size);
            },
            configurable: true,
            writable: true,
        });
    }
    catch (_a) {
        // If the module namespace is not extensible, loading will fail exactly as before.
    }
}
//# sourceMappingURL=NodeCompat.js.map