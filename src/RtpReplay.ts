import {RtpRingEntry} from "./RtpRing";
import {RtpSubscriber} from "./WebRtcSource";

export interface RtpReplayOptions {
    maxQueueBytes?: number;
    maxQueueAgeMs?: number;
    batchBytes?: number;
    tickMs?: number;
    onFallback?: (reason: string) => void;
    onError?: (error: unknown) => void;
}

export class RtpReplay {
    private readonly maxQueueBytes: number;
    private readonly maxQueueAgeMs: number;
    private readonly batchBytes: number;
    private readonly tickMs: number;
    private readonly queue: RtpRingEntry[] = [];
    private queueIndex = 0;
    private queueBytes = 0;
    private snapshotIndex = 0;
    private timer: ReturnType<typeof setTimeout> | undefined;
    private active = true;
    private direct = false;
    private ready = false;

    constructor(
        private readonly subscriber: RtpSubscriber,
        private readonly snapshot: RtpRingEntry[],
        ready: Promise<boolean>,
        private readonly options: RtpReplayOptions = {}
    ) {
        this.maxQueueBytes = options.maxQueueBytes ?? 8 * 1024 * 1024;
        this.maxQueueAgeMs = options.maxQueueAgeMs ?? 15_000;
        // Measured against FFmpeg 8 with the recording args and 8s of 1600x1200-class video:
        // 32 and 16 KiB per tick overflow its UDP receive buffer and corrupt replayed frames,
        // 8 and 4 KiB are clean. Slower only delays footage HomeKit receives in order anyway.
        this.batchBytes = options.batchBytes ?? 4 * 1024;
        this.tickMs = options.tickMs ?? 5;
        void ready.then(listening => {
            if (!this.active)
                return;
            if (!listening) {
                this.fallback('FFmpeg did not open its RTP listener in time.');
                return;
            }
            this.ready = true;
            this.drain();
        }).catch((error: unknown) => {
            if (!this.active)
                return;
            this.options.onError?.(error);
            this.fallback('FFmpeg RTP listener check failed.');
        });
    }

    push(entry: RtpRingEntry): void {
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

    unsubscribe(): void {
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

    private drain(): void {
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

    private nextEntry(): RtpRingEntry | undefined {
        return this.snapshot[this.snapshotIndex] ?? this.queue[this.queueIndex];
    }

    private consumeEntry(entry: RtpRingEntry): void {
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

    private fallback(reason: string): void {
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
        this.options.onFallback?.(reason);
    }

    private deliver(entry: RtpRingEntry): void {
        try {
            if (entry.kind === 'audio')
                this.subscriber.onAudio(entry.packet);
            else
                this.subscriber.onVideo(entry.packet);
        } catch (error: unknown) {
            this.options.onError?.(error);
        }
    }
}
