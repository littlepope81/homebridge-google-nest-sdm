"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SharedWebRtcSource = void 0;
class SharedWebRtcSource {
    constructor(log, displayName, factory, idleBitrate = 600000) {
        this.log = log;
        this.displayName = displayName;
        this.factory = factory;
        this.idleBitrate = idleBitrate;
        this.running = false;
        this.wanted = false;
        this.shutDown = false;
        this.rebuildIndex = 0;
        this.consumers = new Set();
        this.stopping = new Set();
    }
    setWanted(wanted) {
        if (this.shutDown)
            return;
        this.wanted = wanted;
        if (wanted) {
            this.startSource();
            return;
        }
        this.clearRebuild();
        this.clearHealthyTimer();
        this.detachConsumers();
        const source = this.source;
        this.source = undefined;
        this.running = false;
        if (source) {
            void this.stopSource(source).then(() => {
                if (this.wanted && !this.shutDown)
                    this.startSource();
            });
        }
    }
    subscribe(subscriber) {
        const source = this.source;
        const lastRtpAt = source === null || source === void 0 ? void 0 : source.lastRtpAt;
        if (!source || !this.running || lastRtpAt === undefined || Date.now() - lastRtpAt > 5000)
            return undefined;
        let unsubscribeRtp;
        let detachConsumer;
        try {
            unsubscribeRtp = source.subscribe(subscriber);
            detachConsumer = source.attachConsumer();
        }
        catch (error) {
            unsubscribeRtp === null || unsubscribeRtp === void 0 ? void 0 : unsubscribeRtp();
            detachConsumer === null || detachConsumer === void 0 ? void 0 : detachConsumer();
            throw error;
        }
        let subscribed = true;
        const unsubscribe = () => {
            if (!subscribed)
                return;
            subscribed = false;
            this.consumers.delete(unsubscribe);
            unsubscribeRtp === null || unsubscribeRtp === void 0 ? void 0 : unsubscribeRtp();
            detachConsumer === null || detachConsumer === void 0 ? void 0 : detachConsumer();
        };
        this.consumers.add(unsubscribe);
        return unsubscribe;
    }
    shutdown() {
        if (this.shutdownPromise)
            return this.shutdownPromise;
        this.shutDown = true;
        this.wanted = false;
        this.clearRebuild();
        this.clearHealthyTimer();
        this.detachConsumers();
        const source = this.source;
        this.source = undefined;
        this.running = false;
        this.log.debug('Shutting down always-on WebRTC stream.', this.displayName);
        const stop = source ? this.stopSource(source) : Promise.resolve();
        this.shutdownPromise = (async () => {
            await stop;
            await Promise.allSettled(Array.from(this.stopping));
        })();
        return this.shutdownPromise;
    }
    startSource() {
        var _a;
        if (!this.wanted || this.shutDown || this.source || this.rebuildTimeout || this.stopping.size > 0)
            return;
        let created;
        try {
            created = this.factory({
                persistent: true,
                idleBitrate: this.idleBitrate,
                onFailure: reason => {
                    if (created)
                        this.failSource(created, reason);
                }
            });
        }
        catch (error) {
            this.log.debug(`Always-on WebRTC source creation failed: ${(_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error}`, this.displayName);
            this.scheduleRebuild();
            return;
        }
        const source = created;
        this.source = source;
        this.running = false;
        this.log.debug('Starting always-on WebRTC stream.', this.displayName);
        Promise.resolve().then(() => source.start()).then(() => {
            if (this.source !== source || !this.wanted || this.shutDown)
                return;
            this.running = true;
            this.clearHealthyTimer();
            this.healthyTimeout = setTimeout(() => {
                this.healthyTimeout = undefined;
                if (this.source === source && this.running)
                    this.rebuildIndex = 0;
            }, 5 * 60000);
        }).catch((error) => {
            var _a;
            this.failSource(source, `start failed: ${(_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error}`);
        });
    }
    failSource(source, reason) {
        if (this.source !== source)
            return;
        this.source = undefined;
        this.running = false;
        this.clearHealthyTimer();
        this.detachConsumers();
        this.log.debug(`Always-on WebRTC stream failed: ${reason}`, this.displayName);
        void this.stopSource(source).then(() => {
            if (this.wanted && !this.shutDown)
                this.scheduleRebuild();
        });
    }
    scheduleRebuild() {
        if (!this.wanted || this.shutDown || this.rebuildTimeout)
            return;
        const index = Math.min(this.rebuildIndex, SharedWebRtcSource.REBUILD_DELAYS.length - 1);
        const delay = SharedWebRtcSource.REBUILD_DELAYS[index];
        this.rebuildIndex++;
        this.log.debug(`Rebuilding always-on WebRTC stream in ${delay / 1000}s.`, this.displayName);
        this.rebuildTimeout = setTimeout(() => {
            this.rebuildTimeout = undefined;
            if (!this.wanted || this.shutDown)
                return;
            this.log.info('Rebuilding always-on WebRTC stream.', this.displayName);
            this.startSource();
        }, delay);
    }
    stopSource(source) {
        const stopping = Promise.resolve().then(() => source.stop()).catch((error) => {
            var _a;
            this.log.debug(`Always-on WebRTC stream stop failed: ${(_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error}`, this.displayName);
        });
        this.stopping.add(stopping);
        void stopping.finally(() => this.stopping.delete(stopping));
        return stopping;
    }
    detachConsumers() {
        for (const unsubscribe of Array.from(this.consumers))
            unsubscribe();
    }
    clearRebuild() {
        if (!this.rebuildTimeout)
            return;
        clearTimeout(this.rebuildTimeout);
        this.rebuildTimeout = undefined;
    }
    clearHealthyTimer() {
        if (!this.healthyTimeout)
            return;
        clearTimeout(this.healthyTimeout);
        this.healthyTimeout = undefined;
    }
}
exports.SharedWebRtcSource = SharedWebRtcSource;
SharedWebRtcSource.REBUILD_DELAYS = [5000, 30000, 120000, 300000];
//# sourceMappingURL=SharedWebRtcSource.js.map