import {Logger} from "homebridge";
import {RtpSubscriber, WebRtcSourceOptions} from "./WebRtcSource";

export interface SourceLike {
    readonly lastRtpAt: number | undefined;
    start(): Promise<void>;
    stop(): Promise<void>;
    subscribe(subscriber: RtpSubscriber): () => void;
    attachConsumer(): () => void;
}

export type WebRtcSourceFactory = (options: WebRtcSourceOptions) => SourceLike;

export class SharedWebRtcSource {
    private static readonly REBUILD_DELAYS = [5_000, 30_000, 120_000, 300_000];
    private source: SourceLike | undefined;
    private running = false;
    private wanted = false;
    private shutDown = false;
    private rebuildIndex = 0;
    private rebuildTimeout: ReturnType<typeof setTimeout> | undefined;
    private healthyTimeout: ReturnType<typeof setTimeout> | undefined;
    private readonly consumers = new Set<() => void>();
    private readonly stopping = new Set<Promise<void>>();
    private shutdownPromise: Promise<void> | undefined;

    constructor(
        private readonly log: Logger,
        private readonly displayName: string,
        private readonly factory: WebRtcSourceFactory,
        private readonly idleBitrate = 600_000
    ) {}

    setWanted(wanted: boolean): void {
        if (this.shutDown) return;
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

    subscribe(subscriber: RtpSubscriber): (() => void) | undefined {
        const source = this.source;
        const lastRtpAt = source?.lastRtpAt;
        if (!source || !this.running || lastRtpAt === undefined || Date.now() - lastRtpAt > 5_000)
            return undefined;

        let unsubscribeRtp: (() => void) | undefined;
        let detachConsumer: (() => void) | undefined;
        try {
            unsubscribeRtp = source.subscribe(subscriber);
            detachConsumer = source.attachConsumer();
        } catch (error) {
            unsubscribeRtp?.();
            detachConsumer?.();
            throw error;
        }

        let subscribed = true;
        const unsubscribe = () => {
            if (!subscribed) return;
            subscribed = false;
            this.consumers.delete(unsubscribe);
            unsubscribeRtp?.();
            detachConsumer?.();
        };
        this.consumers.add(unsubscribe);
        return unsubscribe;
    }

    shutdown(): Promise<void> {
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

    private startSource(): void {
        if (!this.wanted || this.shutDown || this.source || this.rebuildTimeout || this.stopping.size > 0)
            return;

        let created: SourceLike | undefined;
        try {
            created = this.factory({
                persistent: true,
                idleBitrate: this.idleBitrate,
                onFailure: reason => {
                    if (created)
                        this.failSource(created, reason);
                }
            });
        } catch (error: any) {
            this.log.debug(`Always-on WebRTC source creation failed: ${error?.message ?? error}`, this.displayName);
            this.scheduleRebuild();
            return;
        }

        const source = created;
        this.source = source;
        this.running = false;
        this.log.debug('Starting always-on WebRTC stream.', this.displayName);
        Promise.resolve().then(() => source.start()).then(() => {
            if (this.source !== source || !this.wanted || this.shutDown) return;
            this.running = true;
            this.clearHealthyTimer();
            this.healthyTimeout = setTimeout(() => {
                this.healthyTimeout = undefined;
                if (this.source === source && this.running)
                    this.rebuildIndex = 0;
            }, 5 * 60_000);
        }).catch((error: any) => {
            this.failSource(source, `start failed: ${error?.message ?? error}`);
        });
    }

    private failSource(source: SourceLike, reason: string): void {
        if (this.source !== source) return;
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

    private scheduleRebuild(): void {
        if (!this.wanted || this.shutDown || this.rebuildTimeout) return;
        const index = Math.min(this.rebuildIndex, SharedWebRtcSource.REBUILD_DELAYS.length - 1);
        const delay = SharedWebRtcSource.REBUILD_DELAYS[index];
        this.rebuildIndex++;
        this.log.debug(`Rebuilding always-on WebRTC stream in ${delay / 1000}s.`, this.displayName);
        this.rebuildTimeout = setTimeout(() => {
            this.rebuildTimeout = undefined;
            if (!this.wanted || this.shutDown) return;
            this.log.info('Rebuilding always-on WebRTC stream.', this.displayName);
            this.startSource();
        }, delay);
    }

    private stopSource(source: SourceLike): Promise<void> {
        const stopping = Promise.resolve().then(() => source.stop()).catch((error: any) => {
            this.log.debug(`Always-on WebRTC stream stop failed: ${error?.message ?? error}`, this.displayName);
        });
        this.stopping.add(stopping);
        void stopping.finally(() => this.stopping.delete(stopping));
        return stopping;
    }

    private detachConsumers(): void {
        for (const unsubscribe of Array.from(this.consumers))
            unsubscribe();
    }

    private clearRebuild(): void {
        if (!this.rebuildTimeout) return;
        clearTimeout(this.rebuildTimeout);
        this.rebuildTimeout = undefined;
    }

    private clearHealthyTimer(): void {
        if (!this.healthyTimeout) return;
        clearTimeout(this.healthyTimeout);
        this.healthyTimeout = undefined;
    }
}
