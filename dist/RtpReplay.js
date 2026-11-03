"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RtpReplay = void 0;
class RtpReplay {
    constructor(subscriber, snapshot, ready, options = {}) {
        var _a, _b, _c, _d;
        this.subscriber = subscriber;
        this.snapshot = snapshot;
        this.options = options;
        this.queue = [];
        this.queueIndex = 0;
        this.queueBytes = 0;
        this.snapshotIndex = 0;
        this.active = true;
        this.direct = false;
        this.ready = false;
        this.maxQueueBytes = (_a = options.maxQueueBytes) !== null && _a !== void 0 ? _a : 8 * 1024 * 1024;
        this.maxQueueAgeMs = (_b = options.maxQueueAgeMs) !== null && _b !== void 0 ? _b : 15000;
        // Measured against FFmpeg 8 with the recording args and 8s of 1600x1200-class video:
        // 32 and 16 KiB per tick overflow its UDP receive buffer and corrupt replayed frames,
        // 8 and 4 KiB are clean. Slower only delays footage HomeKit receives in order anyway.
        this.batchBytes = (_c = options.batchBytes) !== null && _c !== void 0 ? _c : 4 * 1024;
        this.tickMs = (_d = options.tickMs) !== null && _d !== void 0 ? _d : 5;
        void ready.then(listening => {
            if (!this.active)
                return;
            if (!listening) {
                this.fallback('FFmpeg did not open its RTP listener in time.');
                return;
            }
            this.ready = true;
            this.drain();
        }).catch((error) => {
            var _a, _b;
            if (!this.active)
                return;
            (_b = (_a = this.options).onError) === null || _b === void 0 ? void 0 : _b.call(_a, error);
            this.fallback('FFmpeg RTP listener check failed.');
        });
    }
    push(entry) {
        if (!this.active)
            return;
        if (this.direct) {
            this.deliver(entry);
            return;
        }
        const oldest = this.queue[this.queueIndex];
        if (this.queueBytes + entry.packet.length > this.maxQueueBytes
            || (oldest && entry.arrivalMs - oldest.arrivalMs > this.maxQueueAgeMs)) {
            this.fallback('live RTP queue exceeded its replay limit.');
            if (this.active)
                this.deliver(entry);
            return;
        }
        this.queue.push(entry);
        this.queueBytes += entry.packet.length;
    }
    unsubscribe() {
        if (!this.active)
            return;
        this.active = false;
        this.direct = false;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }
        this.snapshot.length = 0;
        this.queue.length = 0;
        this.queueBytes = 0;
    }
    drain() {
        if (!this.active || !this.ready || this.direct)
            return;
        let deliveredBytes = 0;
        let deliveredPackets = 0;
        while (this.active) {
            const entry = this.nextEntry();
            if (!entry)
                break;
            if (deliveredPackets > 0 && deliveredBytes + entry.packet.length > this.batchBytes)
                break;
            this.consumeEntry(entry);
            this.deliver(entry);
            deliveredPackets++;
            deliveredBytes += entry.packet.length;
        }
        if (!this.active)
            return;
        if (!this.nextEntry()) {
            this.direct = true;
            this.snapshot.length = 0;
            this.queue.length = 0;
            this.queueBytes = 0;
            return;
        }
        this.timer = setTimeout(() => {
            this.timer = undefined;
            this.drain();
        }, this.tickMs);
    }
    nextEntry() {
        var _a;
        return (_a = this.snapshot[this.snapshotIndex]) !== null && _a !== void 0 ? _a : this.queue[this.queueIndex];
    }
    consumeEntry(entry) {
        if (this.snapshotIndex < this.snapshot.length) {
            this.snapshotIndex++;
            return;
        }
        this.queueIndex++;
        this.queueBytes -= entry.packet.length;
        if (this.queueIndex >= 1024 && this.queueIndex * 2 >= this.queue.length) {
            this.queue.splice(0, this.queueIndex);
            this.queueIndex = 0;
        }
    }
    fallback(reason) {
        var _a, _b;
        if (!this.active || this.direct)
            return;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }
        this.snapshot.length = 0;
        this.queue.length = 0;
        this.queueBytes = 0;
        this.direct = true;
        (_b = (_a = this.options).onFallback) === null || _b === void 0 ? void 0 : _b.call(_a, reason);
    }
    deliver(entry) {
        var _a, _b;
        try {
            if (entry.kind === 'audio')
                this.subscriber.onAudio(entry.packet);
            else
                this.subscriber.onVideo(entry.packet);
        }
        catch (error) {
            (_b = (_a = this.options).onError) === null || _b === void 0 ? void 0 : _b.call(_a, error);
        }
    }
}
exports.RtpReplay = RtpReplay;
//# sourceMappingURL=RtpReplay.js.map