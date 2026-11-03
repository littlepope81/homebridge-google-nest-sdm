"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RtpRing = void 0;
class RtpRing {
    constructor(maxAgeMs = 15000, maxBytes = 8 * 1024 * 1024) {
        this.maxAgeMs = maxAgeMs;
        this.maxBytes = maxBytes;
        this.entries = [];
        this.totalBytes = 0;
        if (maxAgeMs < 0 || maxBytes < 1)
            throw new Error('RTP ring limits must be positive.');
    }
    push(entry, flags = {}) {
        this.entries.push({ entry, keyframeStart: Boolean(flags.keyframeStart) });
        this.totalBytes += entry.packet.length;
        this.evict(entry.arrivalMs);
    }
    markKeyframeStart(packet) {
        for (let index = this.entries.length - 1; index >= 0; index--) {
            if (this.entries[index].entry.packet === packet) {
                this.entries[index].keyframeStart = true;
                return true;
            }
        }
        return false;
    }
    snapshot(targetMs, nowMs) {
        this.evict(nowMs);
        this.snapshotAgeMs = undefined;
        let oldestKeyframeIndex;
        let chosenIndex;
        for (let index = 0; index < this.entries.length; index++) {
            const stored = this.entries[index];
            if (!stored.keyframeStart)
                continue;
            if (oldestKeyframeIndex === undefined)
                oldestKeyframeIndex = index;
            if (nowMs - stored.entry.arrivalMs >= targetMs)
                chosenIndex = index;
        }
        chosenIndex !== null && chosenIndex !== void 0 ? chosenIndex : (chosenIndex = oldestKeyframeIndex);
        if (chosenIndex === undefined)
            return undefined;
        const keyframeArrivalMs = this.entries[chosenIndex].entry.arrivalMs;
        this.snapshotAgeMs = Math.max(0, nowMs - keyframeArrivalMs);
        return this.entries.slice(chosenIndex)
            .map(stored => stored.entry)
            .filter(entry => entry.kind === 'video' || entry.arrivalMs >= keyframeArrivalMs);
    }
    get chosenAgeMs() {
        return this.snapshotAgeMs;
    }
    clear() {
        this.entries.length = 0;
        this.totalBytes = 0;
        this.snapshotAgeMs = undefined;
    }
    evict(nowMs) {
        const cutoff = nowMs - this.maxAgeMs;
        while (this.entries.length > 0
            && (this.entries[0].entry.arrivalMs < cutoff || this.totalBytes > this.maxBytes)) {
            const evicted = this.entries.shift();
            this.totalBytes -= evicted.entry.packet.length;
        }
    }
}
exports.RtpRing = RtpRing;
//# sourceMappingURL=RtpRing.js.map