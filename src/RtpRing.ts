export type RtpKind = 'audio' | 'video';

export interface RtpRingEntry {
    kind: RtpKind;
    packet: Buffer;
    arrivalMs: number;
}

export interface RtpRingPushFlags {
    keyframeStart?: boolean;
}

interface StoredEntry {
    entry: RtpRingEntry;
    keyframeStart: boolean;
}

export class RtpRing {
    private readonly entries: StoredEntry[] = [];
    private totalBytes = 0;
    private snapshotAgeMs: number | undefined;

    constructor(
        private readonly maxAgeMs = 15_000,
        private readonly maxBytes = 8 * 1024 * 1024
    ) {
        if (maxAgeMs < 0 || maxBytes < 1)
            throw new Error('RTP ring limits must be positive.');
    }

    push(entry: RtpRingEntry, flags: RtpRingPushFlags = {}): void {
        this.entries.push({entry, keyframeStart: Boolean(flags.keyframeStart)});
        this.totalBytes += entry.packet.length;
        this.evict(entry.arrivalMs);
    }

    markKeyframeStart(packet: Buffer): boolean {
        for (let index = this.entries.length - 1; index >= 0; index--) {
            if (this.entries[index].entry.packet === packet) {
                this.entries[index].keyframeStart = true;
                return true;
            }
        }
        return false;
    }

    snapshot(targetMs: number, nowMs: number): RtpRingEntry[] | undefined {
        this.evict(nowMs);
        this.snapshotAgeMs = undefined;
        let oldestKeyframeIndex: number | undefined;
        let chosenIndex: number | undefined;

        for (let index = 0; index < this.entries.length; index++) {
            const stored = this.entries[index];
            if (!stored.keyframeStart)
                continue;
            if (oldestKeyframeIndex === undefined)
                oldestKeyframeIndex = index;
            if (nowMs - stored.entry.arrivalMs >= targetMs)
                chosenIndex = index;
        }

        chosenIndex ??= oldestKeyframeIndex;
        if (chosenIndex === undefined)
            return undefined;

        const keyframeArrivalMs = this.entries[chosenIndex].entry.arrivalMs;
        this.snapshotAgeMs = Math.max(0, nowMs - keyframeArrivalMs);
        return this.entries.slice(chosenIndex)
            .map(stored => stored.entry)
            .filter(entry => entry.kind === 'video' || entry.arrivalMs >= keyframeArrivalMs);
    }

    get chosenAgeMs(): number | undefined {
        return this.snapshotAgeMs;
    }

    clear(): void {
        this.entries.length = 0;
        this.totalBytes = 0;
        this.snapshotAgeMs = undefined;
    }

    private evict(nowMs: number): void {
        const cutoff = nowMs - this.maxAgeMs;
        while (this.entries.length > 0
            && (this.entries[0].entry.arrivalMs < cutoff || this.totalBytes > this.maxBytes)) {
            const evicted = this.entries.shift()!;
            this.totalBytes -= evicted.entry.packet.length;
        }
    }
}
