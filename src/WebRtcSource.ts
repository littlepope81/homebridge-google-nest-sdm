import {Logger} from "homebridge";
import {
    MediaStreamTrack,
    RTCPeerConnection,
    RTCRtpCodecParameters,
    RTCRtpTransceiver,
    RtcpPayloadSpecificFeedback,
    ReceiverEstimatedMaxBitrate
} from "werift";
import {Config} from "./Config";
import {containsKeyframe, buildFirFeedback, containsParameterSet, parameterSets} from "./H264";
import {RtpPacketFilter} from "./RtpPacketFilter";
import {RtpReplay} from "./RtpReplay";
import {RtpRing} from "./RtpRing";
import {Camera} from "./sdm/Camera";
import {GenerateWebRtcStream} from "./sdm/Responses";

export interface RtpSubscriber {
    // Fanout shares packet buffers, so subscribers must treat them as read-only.
    onAudio(packet: Buffer): void;
    onVideo(packet: Buffer): void;
}

export interface WebRtcSourceOptions {
    persistent?: boolean;
    idleBitrate?: number;
    onFailure?: (reason: string) => void;
}

export interface RtpParameterSets {
    sps: string;
    pps: string;
}

export class RtpFanout {
    private readonly subscribers = new Set<{subscriber: RtpSubscriber}>();

    constructor(private readonly onError?: (error: unknown) => void) {}

    subscribe(subscriber: RtpSubscriber): () => void {
        const subscription = {subscriber};
        this.subscribers.add(subscription);
        let subscribed = true;
        return () => {
            if (!subscribed) return;
            subscribed = false;
            this.subscribers.delete(subscription);
        };
    }

    publishAudio(packet: Buffer): void {
        this.publish(subscriber => subscriber.onAudio(packet));
    }

    publishVideo(packet: Buffer): void {
        this.publish(subscriber => subscriber.onVideo(packet));
    }

    private publish(deliver: (subscriber: RtpSubscriber) => void): void {
        for (const subscription of Array.from(this.subscribers)) {
            try {
                deliver(subscription.subscriber);
            } catch (error: unknown) {
                try {
                    this.onError?.(error);
                } catch {
                    // Error reporting must not break delivery to other subscribers.
                }
            }
        }
    }
}

export class WebRtcSource {
    // Start high so the first IDR is not throttled by the sender's conservative
    // initial estimate. HomeKit's RECONFIGURE requests are not passed on: on a
    // Mac they arrive ~10s into every view asking for ~132kbps, which would cap
    // the camera at a fraction of what the network can carry.
    private static readonly ACTIVE_BITRATE = 4_000_000;
    private static readonly FIRST_RTP_TIMEOUT_MS = 30_000;
    private rembBitrate = WebRtcSource.ACTIVE_BITRATE;
    private pc: RTCPeerConnection | undefined;
    private keyframeRequestInterval: ReturnType<typeof setInterval> | undefined;
    private keyframeRequestTimeout: ReturnType<typeof setTimeout> | undefined;
    private rembRefreshInterval: ReturnType<typeof setInterval> | undefined;
    private initialBitrateTimeout: ReturnType<typeof setTimeout> | undefined;
    private rtpInactivityTimeout: ReturnType<typeof setTimeout> | undefined;
    private extensionTimeout: ReturnType<typeof setTimeout> | undefined;
    private parameterSetRetryTimeout: ReturnType<typeof setTimeout> | undefined;
    private readonly startTime: number;
    private readonly displayName: string;
    private videoTransceiver: RTCRtpTransceiver | undefined;
    private videoTrack: MediaStreamTrack | undefined;
    private transportConnected = false;
    private firstRembSent = false;
    private firSeq = 0;
    private readonly videoRtpFilter = new RtpPacketFilter();
    private readonly fanout = new RtpFanout(
        (error: any) => this.log.debug('RTP subscriber failed.', error?.message ?? error, this.displayName)
    );
    private token: string | undefined;
    private teardownRequested = false;
    private stopPromise: Promise<void> | undefined;
    private consumerCount = 0;
    private failureReported = false;
    private readonly ring?: RtpRing;
    private sps: string | undefined;
    private pps: string | undefined;
    private parameterSetFirAttempts: number | undefined;
    private parameterSetCandidateTimestamp: number | undefined;
    private parameterSetCandidatePacket: Buffer | undefined;
    private lastBufferedKeyframeTimestamp: number | undefined;
    public lastRtpAt: number | undefined;

    constructor(
        private readonly log: Logger,
        private readonly camera: Camera,
        _config: Config,
        private readonly options: WebRtcSourceOptions = {}
    ) {
        // Diagnostics: log a timeline of WebRTC startup milestones relative to this
        // point, so we can see where the time-to-first-frame actually goes
        // (connection setup vs. first RTP vs. first keyframe). Debug level only.
        this.startTime = Date.now();
        this.displayName = this.camera.getDisplayName();
        if (options.persistent) {
            this.ring = new RtpRing();
            this.parameterSetFirAttempts = 0;
        }
    }

    subscribe(subscriber: RtpSubscriber): () => void {
        return this.fanout.subscribe(subscriber);
    }

    subscribeWithReplay(subscriber: RtpSubscriber, targetMs: number, ready: Promise<boolean>): () => void {
        const snapshot = this.ring?.snapshot(targetMs, Date.now());
        if (!snapshot) {
            this.log.debug('No buffered keyframe; attaching live.', this.displayName);
            return this.fanout.subscribe(subscriber);
        }

        const ageSeconds = ((this.ring?.chosenAgeMs ?? 0) / 1000).toFixed(1);
        this.log.debug(
            `Replaying ${ageSeconds}s of buffered video (${snapshot.length} packets).`,
            this.displayName
        );
        const replay = new RtpReplay(subscriber, snapshot, ready, {
            onFallback: reason => this.log.debug(`Buffered RTP replay abandoned: ${reason}`, this.displayName),
            onError: (error: any) => this.log.debug(
                'RTP replay subscriber failed.', error?.message ?? error, this.displayName)
        });
        const unsubscribeLive = this.fanout.subscribe({
            onAudio: packet => replay.push({kind: 'audio', packet, arrivalMs: Date.now()}),
            onVideo: packet => replay.push({kind: 'video', packet, arrivalMs: Date.now()})
        });
        let subscribed = true;
        return () => {
            if (!subscribed) return;
            subscribed = false;
            replay.unsubscribe();
            unsubscribeLive();
        };
    }

    getParameterSets(): RtpParameterSets | undefined {
        return this.sps && this.pps ? {sps: this.sps, pps: this.pps} : undefined;
    }

    attachConsumer(): () => void {
        this.consumerCount++;
        if (this.options.persistent && this.consumerCount === 1 && !this.teardownRequested) {
            this.rembBitrate = WebRtcSource.ACTIVE_BITRATE;
            if (this.videoTransceiver && this.videoTrack)
                this.sendRemb();
            this.startConsumerKeyframes();
        }

        let attached = true;
        return () => {
            if (!attached) return;
            attached = false;
            this.consumerCount--;
            if (this.options.persistent && this.consumerCount === 0) {
                this.stopKeyframeRequests();
                if (this.initialBitrateTimeout) {
                    clearTimeout(this.initialBitrateTimeout);
                    this.initialBitrateTimeout = undefined;
                }
                this.rembBitrate = this.options.idleBitrate ?? 600_000;
                if (this.videoTransceiver && this.videoTrack && !this.teardownRequested)
                    this.sendRemb();
            }
        };
    }

    private startConsumerKeyframes(): void {
        this.stopKeyframeRequests();
        this.keyframeRequestTimeout = setTimeout(() => {
            this.keyframeRequestTimeout = undefined;
            if (this.teardownRequested || this.consumerCount === 0) return;
            this.requestKeyframe();
            this.keyframeRequestInterval = setInterval(() => {
                if (!this.teardownRequested && this.consumerCount > 0)
                    this.requestKeyframe();
            }, 2000);
        }, 500);
    }

    private stopKeyframeRequests(): void {
        if (this.keyframeRequestTimeout) {
            clearTimeout(this.keyframeRequestTimeout);
            this.keyframeRequestTimeout = undefined;
        }
        if (this.keyframeRequestInterval) {
            clearInterval(this.keyframeRequestInterval);
            this.keyframeRequestInterval = undefined;
        }
    }

    private parameterSetsUpdated(): void {
        if (!this.sps || !this.pps || !this.parameterSetRetryTimeout)
            return;
        clearTimeout(this.parameterSetRetryTimeout);
        this.parameterSetRetryTimeout = undefined;
    }

    private scheduleParameterSetRetry(): void {
        if (!this.options.persistent || this.sps && this.pps
            || (this.parameterSetFirAttempts ?? 0) >= 3 || this.teardownRequested)
            return;
        this.parameterSetRetryTimeout = setTimeout(() => {
            this.parameterSetRetryTimeout = undefined;
            if (this.teardownRequested || this.sps && this.pps || (this.parameterSetFirAttempts ?? 0) >= 3)
                return;
            this.parameterSetFirAttempts = (this.parameterSetFirAttempts ?? 0) + 1;
            this.requestKeyframe();
            this.scheduleParameterSetRetry();
        }, 10_000);
    }

    private noteRtp(): void {
        this.lastRtpAt = Date.now();
        if (!this.options.persistent || this.teardownRequested) return;
        if (!this.rtpInactivityTimeout)
            this.rtpInactivityTimeout = setTimeout(() => this.checkRtpHealth(), 20_000);
    }

    private checkRtpHealth(): void {
        this.rtpInactivityTimeout = undefined;
        if (this.teardownRequested) return;
        if (this.lastRtpAt === undefined) {
            this.reportFailure(`No RTP received within ${WebRtcSource.FIRST_RTP_TIMEOUT_MS / 1000} seconds of connecting.`);
            return;
        }
        const remaining = 20_000 - (Date.now() - this.lastRtpAt);
        if (remaining <= 0) {
            this.reportFailure('No RTP received for 20 seconds.');
        } else {
            this.rtpInactivityTimeout = setTimeout(() => this.checkRtpHealth(), remaining);
        }
    }

    private reportFailure(reason: string): void {
        if (this.failureReported || this.teardownRequested) return;
        this.failureReported = true;
        try {
            this.options.onFailure?.(reason);
        } catch (error: any) {
            this.log.debug('WebRTC failure callback failed.', error?.message ?? error, this.displayName);
        }
    }

    private mark(label: string) {
        this.log.debug(`[startup +${Date.now() - this.startTime}ms] ${label}`, this.displayName);
    }

    private logVideoRtpSummary() {
        const stats = this.videoRtpFilter.stats;
        if (stats.received === 0) return;
        this.log.debug(
            `Video RTP summary: received=${stats.received}, forwarded=${stats.forwarded}, `
            + `duplicateDrops=${stats.duplicateDrops}, emptyPayloadPackets=${stats.emptyPayloadPackets}, `
            + `gapEvents=${stats.gapEvents}, missingAtDetection=${stats.missingAtDetection}, `
            + `lateArrivals=${stats.lateArrivals}.`,
            this.displayName
        );
    }

    // The sender paces its output at its estimated available bandwidth, and
    // the initial estimate is conservative: the first IDR (~50-90 RTP packets)
    // was measured trickling in over 1.3-2.3s at ~375kbps, dominating startup.
    // REMB (negotiated in the answer SDP) is the receiver's way to raise that
    // estimate, so advertise generous bandwidth immediately and keep repeating
    // it alongside the keyframe requests. Per RFC draft, the REMB media-source
    // SSRC field is 0 and the target SSRCs ride in the feedback list.
    private sendRemb() {
        let rembExp = 0, rembMantissa = this.rembBitrate;
        while (rembMantissa > 0x3ffff) { rembMantissa = Math.floor(rembMantissa / 2); rembExp++; }
        try {
            const remb = new RtcpPayloadSpecificFeedback({
                feedback: new ReceiverEstimatedMaxBitrate({
                    senderSsrc: this.videoTransceiver!.receiver.rtcpSsrc,
                    mediaSsrc: 0,
                    ssrcNum: 1,
                    brExp: rembExp,
                    brMantissa: rembMantissa,
                    ssrcFeedbacks: [this.videoTrack!.ssrc!]
                })
            });
            this.videoTransceiver!.receiver.dtlsTransport.sendRtcp([remb]).then(() => {
                if (!this.firstRembSent) {
                    this.firstRembSent = true;
                    this.mark(`sent first REMB (${this.rembBitrate / 1000000}Mbps)`);
                }
            }).catch((e: any) => this.log.debug('REMB send failed.', e?.message ?? e));
        } catch (e: any) {
            this.log.debug('REMB build failed.', e?.message ?? e);
        }
    }

    // Request a keyframe immediately, via both PLI and FIR. PLI ("picture loss")
    // asks for a recovery picture, which these cameras answer with an IDR but
    // *without* SPS/PPS — leaving FFmpeg to wait many seconds for the camera's
    // next periodic parameter sets. FIR ("full intra request", RFC 5104) asks
    // for a full intra frame, which these cameras answer with the parameter
    // sets attached (verified: every FIR-elicited keyframe carries SPS/PPS in
    // the same access unit), so FFmpeg has the dimensions at the first
    // keyframe. FIR needs a per-SSRC sequence number that increments each
    // request, or the camera ignores repeats.
    private requestKeyframe() {
        if (!this.options.persistent)
            this.sendRemb();
        const receiver = this.videoTransceiver!.receiver;
        const track = this.videoTrack!;
        receiver.sendRtcpPLI(track.ssrc!).catch((e: any) => this.log.debug('PLI send failed.', e?.message ?? e));
        try {
            const fir = new RtcpPayloadSpecificFeedback({
                feedback: buildFirFeedback(receiver.rtcpSsrc, track.ssrc!, this.firSeq++)
            });
            receiver.dtlsTransport.sendRtcp([fir]).catch((e: any) => this.log.debug('FIR send failed.', e?.message ?? e));
        } catch (e: any) {
            this.log.debug('FIR build failed.', e?.message ?? e);
        }
    }

    async start(): Promise<void> {
        if (this.teardownRequested) {
            throw new Error('Nest stream initialization completed after teardown was requested.');
        }

        if (this.options.persistent) {
            this.rembBitrate = WebRtcSource.ACTIVE_BITRATE;
            this.initialBitrateTimeout = setTimeout(() => {
                this.initialBitrateTimeout = undefined;
                if (this.consumerCount === 0 && !this.teardownRequested) {
                    this.rembBitrate = this.options.idleBitrate ?? 600_000;
                    if (this.videoTransceiver && this.videoTrack)
                        this.sendRemb();
                }
            }, 15_000);
        }

        this.pc = new RTCPeerConnection({
            bundlePolicy: "max-bundle",
            codecs: {
                audio: [
                    new RTCRtpCodecParameters({
                        mimeType: "audio/opus",
                        clockRate: 48000,
                        channels: 2,
                    })
                ],
                video: [
                    new RTCRtpCodecParameters({
                        mimeType: "video/H264",
                        clockRate: 90000,
                        rtcpFeedback: [
                            { type: "transport-cc" },
                            { type: "ccm", parameter: "fir" },
                            { type: "nack" },
                            { type: "nack", parameter: "pli" },
                            { type: "goog-remb" },
                        ],
                        parameters: 'level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f'
                    })
                ],
            }
        });

        this.pc.iceConnectionStateChange.subscribe((state) => this.mark(`iceConnectionState: ${state}`));
        this.pc.connectionStateChange.subscribe((state) => {
            this.mark(`connectionState: ${state}`);
            this.transportConnected = state === 'connected';
            // onTrack fires while DTLS is still connecting, so sendRtcp() has no
            // encryption context yet and the first REMB always fails. Advertise
            // bandwidth as soon as DTLS is usable instead of waiting for the
            // first RTP packet to trigger requestKeyframe().
            if (this.transportConnected && this.videoTransceiver && this.videoTrack)
                this.sendRemb();
            if ((state === 'failed' || state === 'closed') && !this.teardownRequested)
                this.reportFailure(`WebRTC connection state became ${state}.`);
        });

        const audioTransceiver = this.pc.addTransceiver("audio", {direction: "recvonly"});
        audioTransceiver.onTrack.subscribe((track) => {
            audioTransceiver.sender.replaceTrack(track);
            track.onReceiveRtp.subscribe((rtp) => {
                this.noteRtp();
                const out = rtp.serialize();
                this.ring?.push({kind: 'audio', packet: out, arrivalMs: Date.now()});
                this.fanout.publishAudio(out);
            });
        });

        let sawFirstVideoRtp = false;
        let sawFirstKeyframe = false;
        // Diagnostics for the keyframe→FFmpeg-output gap: the IDR spans many RTP
        // packets (its last one carries the marker bit), and FFmpeg cannot finish
        // probing until it has also seen the *next* frame's timestamp. These track
        // when the first keyframe finishes arriving and when the following frames
        // start, so the timeline shows whether the gap is network pacing or FFmpeg.
        let keyframeTimestamp: number | undefined;
        let keyframePacketCount = 0;
        let keyframeComplete = false;
        let framesAfterKeyframe = 0;
        let lastVideoTimestamp: number | undefined;

        const videoTransceiver = this.pc.addTransceiver("video", {direction: "recvonly"});
        videoTransceiver.onTrack.subscribe((track) => {
            this.videoTransceiver = videoTransceiver;
            this.videoTrack = track;
            this.firstRembSent = false;
            this.firSeq = 0;
            this.mark('video track received');
            videoTransceiver.sender.replaceTrack(track);
            // Usually the track arrives before DTLS connects, but handle the
            // opposite callback order without falling back to the first packet.
            if (this.transportConnected)
                this.sendRemb();
            if (this.options.persistent && !this.teardownRequested) {
                this.rembRefreshInterval = setInterval(() => {
                    if (!this.teardownRequested)
                        this.sendRemb();
                }, 2000);
            }

            track.onReceiveRtp.subscribe((rtp) => {
                this.noteRtp();
                const decision = this.videoRtpFilter.inspect(
                    rtp.header.ssrc,
                    rtp.header.sequenceNumber,
                    rtp.payload.length
                );
                const stats = this.videoRtpFilter.stats;
                if (!decision.forward) {
                    // Log the first occurrence immediately, then periodically
                    // enough to expose a sustained problem without flooding.
                    if (stats.duplicateDrops === 1 || stats.duplicateDrops % 25 === 0) {
                        this.log.debug(
                            `Dropped ${decision.reason} video RTP packet `
                            + `(seq=${rtp.header.sequenceNumber}, total=${stats.duplicateDrops}).`,
                            this.displayName
                        );
                    }
                    return;
                }
                // Silent on an always-on session: its steady padding would hit this every
                // second or so, around the clock, and push real history out of the log.
                if (decision.emptyPayload && !this.options.persistent
                    && (stats.emptyPayloadPackets === 1 || stats.emptyPayloadPackets % 100 === 0)) {
                    this.log.debug(
                        `Forwarded empty-payload video RTP packet to preserve sequence continuity `
                        + `(seq=${rtp.header.sequenceNumber}, total=${stats.emptyPayloadPackets}).`,
                        this.displayName
                    );
                }
                if (decision.gapSize > 0
                    && (stats.gapEvents === 1 || stats.gapEvents % 25 === 0)) {
                    this.log.debug(
                        `Video RTP sequence gap detected `
                        + `(seq=${rtp.header.sequenceNumber}, missing=${decision.gapSize}, `
                        + `gapEvents=${stats.gapEvents}).`,
                        this.displayName
                    );
                }

                if (!sawFirstVideoRtp) {
                    sawFirstVideoRtp = true;
                    this.mark('first video RTP packet');
                }
                const isKeyframe = containsKeyframe(rtp.payload);
                if (isKeyframe && !sawFirstKeyframe) {
                    sawFirstKeyframe = true;
                    keyframeTimestamp = rtp.header.timestamp;
                    this.mark('first video keyframe (IDR)');
                }

                if (sawFirstKeyframe && !keyframeComplete && rtp.header.timestamp === keyframeTimestamp) {
                    keyframePacketCount++;
                    if (rtp.header.marker) {
                        keyframeComplete = true;
                        this.mark(`first keyframe fully received (${keyframePacketCount} packets)`);
                    }
                }
                if (sawFirstKeyframe && rtp.header.timestamp !== keyframeTimestamp
                    && rtp.header.timestamp !== lastVideoTimestamp && framesAfterKeyframe < 3) {
                    framesAfterKeyframe++;
                    this.mark(`video frame ${framesAfterKeyframe} after keyframe started`);
                }
                lastVideoTimestamp = rtp.header.timestamp;

                const out = rtp.serialize();
                if (this.ring) {
                    const found = parameterSets(rtp.payload);
                    if (found.sps)
                        this.sps = found.sps.toString('base64');
                    if (found.pps)
                        this.pps = found.pps.toString('base64');
                    this.parameterSetsUpdated();

                    if (containsParameterSet(rtp.payload)
                        && this.parameterSetCandidateTimestamp !== rtp.header.timestamp) {
                        this.parameterSetCandidateTimestamp = rtp.header.timestamp;
                        this.parameterSetCandidatePacket = out;
                    }

                    let keyframeStart = false;
                    if (isKeyframe && rtp.header.timestamp !== this.lastBufferedKeyframeTimestamp) {
                        this.lastBufferedKeyframeTimestamp = rtp.header.timestamp;
                        const candidate = this.parameterSetCandidateTimestamp === rtp.header.timestamp
                            ? this.parameterSetCandidatePacket
                            : undefined;
                        if (!candidate || candidate === out || !this.ring.markKeyframeStart(candidate))
                            keyframeStart = true;
                        this.parameterSetCandidateTimestamp = undefined;
                        this.parameterSetCandidatePacket = undefined;
                    }
                    this.ring.push(
                        {kind: 'video', packet: out, arrivalMs: Date.now()},
                        {keyframeStart}
                    );
                }
                this.fanout.publishVideo(out);
            });
            track.onReceiveRtp.once(() => {
                if (this.teardownRequested) return;
                if (this.options.persistent) {
                    this.sendRemb();
                    this.parameterSetFirAttempts = 1;
                }
                this.requestKeyframe();
                if (this.options.persistent)
                    this.scheduleParameterSetRetry();
                else
                    this.keyframeRequestInterval = setInterval(this.requestKeyframe.bind(this), 2000);
            });
        });

        this.pc.createDataChannel('dataSendChannel', {id: 1});

        let offer = await this.pc.createOffer();
        await this.pc.setLocalDescription(offer);

        this.mark('sending offer to Nest (GenerateWebRtcStream)');
        const streamInfo = <GenerateWebRtcStream> await this.camera.generateStream(offer.sdp);
        if (!streamInfo) {
            throw new Error(`Unable to start stream for ${this.camera.getDisplayName()}: no response from the Nest API (this is usually rate limiting — see the error above).`);
        }
        this.mark('received answer from Nest');
        this.token = streamInfo.mediaSessionId;
        await this.rejectIfTeardownRequested();
        await this.pc.setRemoteDescription({
            type: 'answer',
            sdp: streamInfo.answerSdp
        });
        if (this.options.persistent && !this.teardownRequested) {
            this.scheduleExtension(streamInfo.expiresAt);
            // noteRtp only arms the stall check once packets flow, so a session that
            // connects and then never sends one would otherwise stay down forever.
            if (!this.rtpInactivityTimeout)
                this.rtpInactivityTimeout = setTimeout(() => this.checkRtpHealth(), WebRtcSource.FIRST_RTP_TIMEOUT_MS);
        }
        this.mark('remote description set; returning to start FFmpeg');
    }

    private scheduleExtension(expiresAt: string): void {
        if (this.extensionTimeout) {
            clearTimeout(this.extensionTimeout);
            this.extensionTimeout = undefined;
        }
        const expiresAtMs = Date.parse(expiresAt);
        if (!Number.isFinite(expiresAtMs)) {
            this.log.warn('WebRTC stream extension could not be scheduled because expiresAt was invalid.', this.displayName);
            this.reportFailure('WebRTC stream expiresAt was invalid.');
            return;
        }
        const delay = Math.max(0, expiresAtMs - Date.now() - 60_000);
        this.extensionTimeout = setTimeout(() => {
            this.extensionTimeout = undefined;
            void this.extendWebRtcStream(false);
        }, delay);
    }

    private async extendWebRtcStream(retrying: boolean): Promise<void> {
        const mediaSessionId = this.token;
        if (!mediaSessionId || this.teardownRequested) return;
        try {
            const response = await this.camera.extendWebRtcStream(mediaSessionId);
            if (!response)
                throw new Error('no response from the Nest API');
            if (this.teardownRequested) {
                await this.camera.stopStream(response.mediaSessionId);
                return;
            }
            this.token = response.mediaSessionId;
            this.log.debug('Extended always-on WebRTC stream.', this.displayName);
            this.scheduleExtension(response.expiresAt);
        } catch (error: any) {
            if (this.teardownRequested) return;
            this.log.warn(
                `Always-on WebRTC stream extension failed${retrying ? ' after retry' : ''}.`,
                error?.message ?? error,
                this.displayName
            );
            if (retrying) {
                this.reportFailure('WebRTC stream extension failed twice.');
            } else {
                this.extensionTimeout = setTimeout(() => {
                    this.extensionTimeout = undefined;
                    void this.extendWebRtcStream(true);
                }, 15_000);
            }
        }
    }

    stop(): Promise<void> {
        this.teardownRequested = true;
        if (!this.stopPromise) {
            this.stopPromise = this.stopOnce();
        }
        return this.stopPromise;
    }

    private async stopOnce(): Promise<void> {
        this.logVideoRtpSummary();
        this.stopKeyframeRequests();
        if (this.rembRefreshInterval) {
            clearInterval(this.rembRefreshInterval);
            this.rembRefreshInterval = undefined;
        }
        if (this.initialBitrateTimeout) {
            clearTimeout(this.initialBitrateTimeout);
            this.initialBitrateTimeout = undefined;
        }
        if (this.rtpInactivityTimeout) {
            clearTimeout(this.rtpInactivityTimeout);
            this.rtpInactivityTimeout = undefined;
        }
        if (this.extensionTimeout) {
            clearTimeout(this.extensionTimeout);
            this.extensionTimeout = undefined;
        }
        if (this.parameterSetRetryTimeout) {
            clearTimeout(this.parameterSetRetryTimeout);
            this.parameterSetRetryTimeout = undefined;
        }
        this.ring?.clear();
        this.parameterSetCandidatePacket = undefined;
        this.sps = undefined;
        this.pps = undefined;

        // initialize() creates local WebRTC resources before SDM assigns a media
        // session id. Always close the local resources below, but skip the doomed
        // stopStream request when initialization failed before assigning a token.
        if (this.token) {
            try {
                await this.stopAssignedStream();
            } catch (error: any) {
                this.log.error('Error stopping camera stream.', error);
            }
        }

        try {
            await this.pc?.close();
        } catch (error: any) {
            this.log.error('Error closing peer connection.', error);
        }
    }

    private async stopAssignedStream(): Promise<void> {
        const token = this.token;
        if (token) {
            await this.camera.stopStream(token);
            if (this.token === token) {
                this.token = undefined;
            }
        }
    }

    private async rejectIfTeardownRequested(): Promise<void> {
        if (!this.teardownRequested) return;
        // initialize() can finish assigning a remote token after cleanup already
        // requested teardown. Retire that late token even though its successor
        // may already be acquiring.
        await this.stopAssignedStream();
        throw new Error('Nest stream initialization completed after teardown was requested.');
    }
}
