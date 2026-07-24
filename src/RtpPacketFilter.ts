const RTP_SEQUENCE_MODULUS = 0x10000;
const RTP_SEQUENCE_HALF_RANGE = 0x8000;
const DEFAULT_RECENT_SEQUENCE_WINDOW = 4096;

interface RtpSequenceState {
    highestSequenceNumber: number | undefined;
    recentSequenceNumbers: Set<number>;
    recentSequenceOrder: number[];
    recentSequenceCursor: number;
}

export interface RtpPacketFilterStats {
    received: number;
    forwarded: number;
    duplicateDrops: number;
    emptyPayloadPackets: number;
    gapEvents: number;
    missingAtDetection: number;
    lateArrivals: number;
}

export interface RtpPacketFilterDecision {
    forward: boolean;
    reason?: 'duplicate';
    gapSize: number;
    lateArrival: boolean;
    emptyPayload: boolean;
}

/**
 * Filters RTP packets before they reach FFmpeg.
 *
 * WebRTC retransmission can deliver the same RTP sequence number more than
 * once. FFmpeg's RTP reorder queue does not reliably discard duplicates that
 * were queued behind an earlier gap, so a repeated H.264 fragment can reach
 * its depacketizer and corrupt the assembled frame. Keep a bounded recent
 * sequence window per SSRC and drop exact duplicates without delaying packets.
 *
 * The bounded window is deliberately much smaller than the 16-bit RTP
 * sequence space. Old values are evicted before a legitimate sequence-number
 * wrap can revisit them.
 */
export class RtpPacketFilter {
    readonly stats: RtpPacketFilterStats = {
        received: 0,
        forwarded: 0,
        duplicateDrops: 0,
        emptyPayloadPackets: 0,
        gapEvents: 0,
        missingAtDetection: 0,
        lateArrivals: 0,
    };

    private readonly states = new Map<number, RtpSequenceState>();

    constructor(private readonly recentSequenceWindow = DEFAULT_RECENT_SEQUENCE_WINDOW) {
        if (recentSequenceWindow < 1 || recentSequenceWindow >= RTP_SEQUENCE_MODULUS) {
            throw new Error('RTP recent-sequence window must be between 1 and 65535 packets.');
        }
    }

    inspect(ssrc: number, sequenceNumber: number, payloadLength: number): RtpPacketFilterDecision {
        this.stats.received++;

        const state = this.stateFor(ssrc);
        if (state.recentSequenceNumbers.has(sequenceNumber)
            || state.highestSequenceNumber === sequenceNumber) {
            this.stats.duplicateDrops++;
            return {
                forward: false,
                reason: 'duplicate',
                gapSize: 0,
                lateArrival: false,
                emptyPayload: false,
            };
        }

        let gapSize = 0;
        let lateArrival = false;
        if (state.highestSequenceNumber === undefined) {
            state.highestSequenceNumber = sequenceNumber;
        } else {
            const distance = (sequenceNumber - state.highestSequenceNumber + RTP_SEQUENCE_MODULUS)
                % RTP_SEQUENCE_MODULUS;
            if (distance < RTP_SEQUENCE_HALF_RANGE) {
                if (distance > 1) {
                    gapSize = distance - 1;
                    this.stats.gapEvents++;
                    this.stats.missingAtDetection += gapSize;
                }
                state.highestSequenceNumber = sequenceNumber;
            } else {
                // A sequence number behind the highest one is a recovered or
                // naturally reordered packet. It is new, so preserve it for
                // FFmpeg's reorder queue instead of treating it as a duplicate.
                lateArrival = true;
                this.stats.lateArrivals++;
            }
        }

        this.remember(state, sequenceNumber);

        const emptyPayload = payloadLength === 0;
        if (emptyPayload)
            this.stats.emptyPayloadPackets++;

        this.stats.forwarded++;
        return {forward: true, gapSize, lateArrival, emptyPayload};
    }

    private stateFor(ssrc: number): RtpSequenceState {
        let state = this.states.get(ssrc);
        if (!state) {
            state = {
                highestSequenceNumber: undefined,
                recentSequenceNumbers: new Set<number>(),
                recentSequenceOrder: [],
                recentSequenceCursor: 0,
            };
            this.states.set(ssrc, state);
        }
        return state;
    }

    private remember(state: RtpSequenceState, sequenceNumber: number): void {
        if (state.recentSequenceOrder.length < this.recentSequenceWindow) {
            state.recentSequenceOrder.push(sequenceNumber);
        } else {
            const evicted = state.recentSequenceOrder[state.recentSequenceCursor];
            state.recentSequenceNumbers.delete(evicted);
            state.recentSequenceOrder[state.recentSequenceCursor] = sequenceNumber;
            state.recentSequenceCursor = (state.recentSequenceCursor + 1) % this.recentSequenceWindow;
        }
        state.recentSequenceNumbers.add(sequenceNumber);
    }
}
