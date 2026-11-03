"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebRtcSource = exports.RtpFanout = void 0;
const werift_1 = require("werift");
const H264_1 = require("./H264");
const RtpPacketFilter_1 = require("./RtpPacketFilter");
class RtpFanout {
    constructor(onError) {
        this.onError = onError;
        this.subscribers = new Set();
    }
    subscribe(subscriber) {
        const subscription = { subscriber };
        this.subscribers.add(subscription);
        let subscribed = true;
        return () => {
            if (!subscribed)
                return;
            subscribed = false;
            this.subscribers.delete(subscription);
        };
    }
    publishAudio(packet) {
        this.publish(subscriber => subscriber.onAudio(packet));
    }
    publishVideo(packet) {
        this.publish(subscriber => subscriber.onVideo(packet));
    }
    publish(deliver) {
        var _a;
        for (const subscription of Array.from(this.subscribers)) {
            try {
                deliver(subscription.subscriber);
            }
            catch (error) {
                try {
                    (_a = this.onError) === null || _a === void 0 ? void 0 : _a.call(this, error);
                }
                catch (_b) {
                    // Error reporting must not break delivery to other subscribers.
                }
            }
        }
    }
}
exports.RtpFanout = RtpFanout;
class WebRtcSource {
    constructor(log, camera, _config, options = {}) {
        this.log = log;
        this.camera = camera;
        this.options = options;
        this.rembBitrate = WebRtcSource.ACTIVE_BITRATE;
        this.transportConnected = false;
        this.firstRembSent = false;
        this.firSeq = 0;
        this.videoRtpFilter = new RtpPacketFilter_1.RtpPacketFilter();
        this.fanout = new RtpFanout((error) => { var _a; return this.log.debug('RTP subscriber failed.', (_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error, this.displayName); });
        this.teardownRequested = false;
        this.consumerCount = 0;
        this.failureReported = false;
        // Diagnostics: log a timeline of WebRTC startup milestones relative to this
        // point, so we can see where the time-to-first-frame actually goes
        // (connection setup vs. first RTP vs. first keyframe). Debug level only.
        this.startTime = Date.now();
        this.displayName = this.camera.getDisplayName();
    }
    subscribe(subscriber) {
        return this.fanout.subscribe(subscriber);
    }
    attachConsumer() {
        this.consumerCount++;
        if (this.options.persistent && this.consumerCount === 1 && !this.teardownRequested) {
            this.rembBitrate = WebRtcSource.ACTIVE_BITRATE;
            if (this.videoTransceiver && this.videoTrack)
                this.sendRemb();
            this.startConsumerKeyframes();
        }
        let attached = true;
        return () => {
            var _a;
            if (!attached)
                return;
            attached = false;
            this.consumerCount--;
            if (this.options.persistent && this.consumerCount === 0) {
                this.stopKeyframeRequests();
                if (this.initialBitrateTimeout) {
                    clearTimeout(this.initialBitrateTimeout);
                    this.initialBitrateTimeout = undefined;
                }
                this.rembBitrate = (_a = this.options.idleBitrate) !== null && _a !== void 0 ? _a : 600000;
                if (this.videoTransceiver && this.videoTrack && !this.teardownRequested)
                    this.sendRemb();
            }
        };
    }
    startConsumerKeyframes() {
        this.stopKeyframeRequests();
        this.keyframeRequestTimeout = setTimeout(() => {
            this.keyframeRequestTimeout = undefined;
            if (this.teardownRequested || this.consumerCount === 0)
                return;
            this.requestKeyframe();
            this.keyframeRequestInterval = setInterval(() => {
                if (!this.teardownRequested && this.consumerCount > 0)
                    this.requestKeyframe();
            }, 2000);
        }, 500);
    }
    stopKeyframeRequests() {
        if (this.keyframeRequestTimeout) {
            clearTimeout(this.keyframeRequestTimeout);
            this.keyframeRequestTimeout = undefined;
        }
        if (this.keyframeRequestInterval) {
            clearInterval(this.keyframeRequestInterval);
            this.keyframeRequestInterval = undefined;
        }
    }
    noteRtp() {
        this.lastRtpAt = Date.now();
        if (!this.options.persistent || this.teardownRequested)
            return;
        if (!this.rtpInactivityTimeout)
            this.rtpInactivityTimeout = setTimeout(() => this.checkRtpHealth(), 20000);
    }
    checkRtpHealth() {
        this.rtpInactivityTimeout = undefined;
        if (this.teardownRequested)
            return;
        if (this.lastRtpAt === undefined) {
            this.reportFailure(`No RTP received within ${WebRtcSource.FIRST_RTP_TIMEOUT_MS / 1000} seconds of connecting.`);
            return;
        }
        const remaining = 20000 - (Date.now() - this.lastRtpAt);
        if (remaining <= 0) {
            this.reportFailure('No RTP received for 20 seconds.');
        }
        else {
            this.rtpInactivityTimeout = setTimeout(() => this.checkRtpHealth(), remaining);
        }
    }
    reportFailure(reason) {
        var _a, _b, _c;
        if (this.failureReported || this.teardownRequested)
            return;
        this.failureReported = true;
        try {
            (_b = (_a = this.options).onFailure) === null || _b === void 0 ? void 0 : _b.call(_a, reason);
        }
        catch (error) {
            this.log.debug('WebRTC failure callback failed.', (_c = error === null || error === void 0 ? void 0 : error.message) !== null && _c !== void 0 ? _c : error, this.displayName);
        }
    }
    mark(label) {
        this.log.debug(`[startup +${Date.now() - this.startTime}ms] ${label}`, this.displayName);
    }
    logVideoRtpSummary() {
        const stats = this.videoRtpFilter.stats;
        if (stats.received === 0)
            return;
        this.log.debug(`Video RTP summary: received=${stats.received}, forwarded=${stats.forwarded}, `
            + `duplicateDrops=${stats.duplicateDrops}, emptyPayloadPackets=${stats.emptyPayloadPackets}, `
            + `gapEvents=${stats.gapEvents}, missingAtDetection=${stats.missingAtDetection}, `
            + `lateArrivals=${stats.lateArrivals}.`, this.displayName);
    }
    // The sender paces its output at its estimated available bandwidth, and
    // the initial estimate is conservative: the first IDR (~50-90 RTP packets)
    // was measured trickling in over 1.3-2.3s at ~375kbps, dominating startup.
    // REMB (negotiated in the answer SDP) is the receiver's way to raise that
    // estimate, so advertise generous bandwidth immediately and keep repeating
    // it alongside the keyframe requests. Per RFC draft, the REMB media-source
    // SSRC field is 0 and the target SSRCs ride in the feedback list.
    sendRemb() {
        var _a;
        let rembExp = 0, rembMantissa = this.rembBitrate;
        while (rembMantissa > 0x3ffff) {
            rembMantissa = Math.floor(rembMantissa / 2);
            rembExp++;
        }
        try {
            const remb = new werift_1.RtcpPayloadSpecificFeedback({
                feedback: new werift_1.ReceiverEstimatedMaxBitrate({
                    senderSsrc: this.videoTransceiver.receiver.rtcpSsrc,
                    mediaSsrc: 0,
                    ssrcNum: 1,
                    brExp: rembExp,
                    brMantissa: rembMantissa,
                    ssrcFeedbacks: [this.videoTrack.ssrc]
                })
            });
            this.videoTransceiver.receiver.dtlsTransport.sendRtcp([remb]).then(() => {
                if (!this.firstRembSent) {
                    this.firstRembSent = true;
                    this.mark(`sent first REMB (${this.rembBitrate / 1000000}Mbps)`);
                }
            }).catch((e) => { var _a; return this.log.debug('REMB send failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e); });
        }
        catch (e) {
            this.log.debug('REMB build failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e);
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
    requestKeyframe() {
        var _a;
        if (!this.options.persistent)
            this.sendRemb();
        const receiver = this.videoTransceiver.receiver;
        const track = this.videoTrack;
        receiver.sendRtcpPLI(track.ssrc).catch((e) => { var _a; return this.log.debug('PLI send failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e); });
        try {
            const fir = new werift_1.RtcpPayloadSpecificFeedback({
                feedback: (0, H264_1.buildFirFeedback)(receiver.rtcpSsrc, track.ssrc, this.firSeq++)
            });
            receiver.dtlsTransport.sendRtcp([fir]).catch((e) => { var _a; return this.log.debug('FIR send failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e); });
        }
        catch (e) {
            this.log.debug('FIR build failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e);
        }
    }
    async start() {
        if (this.teardownRequested) {
            throw new Error('Nest stream initialization completed after teardown was requested.');
        }
        if (this.options.persistent) {
            this.rembBitrate = WebRtcSource.ACTIVE_BITRATE;
            this.initialBitrateTimeout = setTimeout(() => {
                var _a;
                this.initialBitrateTimeout = undefined;
                if (this.consumerCount === 0 && !this.teardownRequested) {
                    this.rembBitrate = (_a = this.options.idleBitrate) !== null && _a !== void 0 ? _a : 600000;
                    if (this.videoTransceiver && this.videoTrack)
                        this.sendRemb();
                }
            }, 15000);
        }
        this.pc = new werift_1.RTCPeerConnection({
            bundlePolicy: "max-bundle",
            codecs: {
                audio: [
                    new werift_1.RTCRtpCodecParameters({
                        mimeType: "audio/opus",
                        clockRate: 48000,
                        channels: 2,
                    })
                ],
                video: [
                    new werift_1.RTCRtpCodecParameters({
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
        const audioTransceiver = this.pc.addTransceiver("audio", { direction: "recvonly" });
        audioTransceiver.onTrack.subscribe((track) => {
            audioTransceiver.sender.replaceTrack(track);
            track.onReceiveRtp.subscribe((rtp) => {
                this.noteRtp();
                this.fanout.publishAudio(rtp.serialize());
            });
        });
        let sawFirstVideoRtp = false;
        let sawFirstKeyframe = false;
        // Diagnostics for the keyframe→FFmpeg-output gap: the IDR spans many RTP
        // packets (its last one carries the marker bit), and FFmpeg cannot finish
        // probing until it has also seen the *next* frame's timestamp. These track
        // when the first keyframe finishes arriving and when the following frames
        // start, so the timeline shows whether the gap is network pacing or FFmpeg.
        let keyframeTimestamp;
        let keyframePacketCount = 0;
        let keyframeComplete = false;
        let framesAfterKeyframe = 0;
        let lastVideoTimestamp;
        const videoTransceiver = this.pc.addTransceiver("video", { direction: "recvonly" });
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
                const decision = this.videoRtpFilter.inspect(rtp.header.ssrc, rtp.header.sequenceNumber, rtp.payload.length);
                const stats = this.videoRtpFilter.stats;
                if (!decision.forward) {
                    // Log the first occurrence immediately, then periodically
                    // enough to expose a sustained problem without flooding.
                    if (stats.duplicateDrops === 1 || stats.duplicateDrops % 25 === 0) {
                        this.log.debug(`Dropped ${decision.reason} video RTP packet `
                            + `(seq=${rtp.header.sequenceNumber}, total=${stats.duplicateDrops}).`, this.displayName);
                    }
                    return;
                }
                // Silent on an always-on session: its steady padding would hit this every
                // second or so, around the clock, and push real history out of the log.
                if (decision.emptyPayload && !this.options.persistent
                    && (stats.emptyPayloadPackets === 1 || stats.emptyPayloadPackets % 100 === 0)) {
                    this.log.debug(`Forwarded empty-payload video RTP packet to preserve sequence continuity `
                        + `(seq=${rtp.header.sequenceNumber}, total=${stats.emptyPayloadPackets}).`, this.displayName);
                }
                if (decision.gapSize > 0
                    && (stats.gapEvents === 1 || stats.gapEvents % 25 === 0)) {
                    this.log.debug(`Video RTP sequence gap detected `
                        + `(seq=${rtp.header.sequenceNumber}, missing=${decision.gapSize}, `
                        + `gapEvents=${stats.gapEvents}).`, this.displayName);
                }
                if (!sawFirstVideoRtp) {
                    sawFirstVideoRtp = true;
                    this.mark('first video RTP packet');
                }
                const isKeyframe = (0, H264_1.containsKeyframe)(rtp.payload);
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
                this.fanout.publishVideo(out);
            });
            track.onReceiveRtp.once(() => {
                if (this.teardownRequested)
                    return;
                if (this.options.persistent)
                    this.sendRemb();
                this.requestKeyframe();
                if (!this.options.persistent)
                    this.keyframeRequestInterval = setInterval(this.requestKeyframe.bind(this), 2000);
            });
        });
        this.pc.createDataChannel('dataSendChannel', { id: 1 });
        let offer = await this.pc.createOffer();
        await this.pc.setLocalDescription(offer);
        this.mark('sending offer to Nest (GenerateWebRtcStream)');
        const streamInfo = await this.camera.generateStream(offer.sdp);
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
    scheduleExtension(expiresAt) {
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
        const delay = Math.max(0, expiresAtMs - Date.now() - 60000);
        this.extensionTimeout = setTimeout(() => {
            this.extensionTimeout = undefined;
            void this.extendWebRtcStream(false);
        }, delay);
    }
    async extendWebRtcStream(retrying) {
        var _a;
        const mediaSessionId = this.token;
        if (!mediaSessionId || this.teardownRequested)
            return;
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
        }
        catch (error) {
            if (this.teardownRequested)
                return;
            this.log.warn(`Always-on WebRTC stream extension failed${retrying ? ' after retry' : ''}.`, (_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error, this.displayName);
            if (retrying) {
                this.reportFailure('WebRTC stream extension failed twice.');
            }
            else {
                this.extensionTimeout = setTimeout(() => {
                    this.extensionTimeout = undefined;
                    void this.extendWebRtcStream(true);
                }, 15000);
            }
        }
    }
    stop() {
        this.teardownRequested = true;
        if (!this.stopPromise) {
            this.stopPromise = this.stopOnce();
        }
        return this.stopPromise;
    }
    async stopOnce() {
        var _a;
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
        // initialize() creates local WebRTC resources before SDM assigns a media
        // session id. Always close the local resources below, but skip the doomed
        // stopStream request when initialization failed before assigning a token.
        if (this.token) {
            try {
                await this.stopAssignedStream();
            }
            catch (error) {
                this.log.error('Error stopping camera stream.', error);
            }
        }
        try {
            await ((_a = this.pc) === null || _a === void 0 ? void 0 : _a.close());
        }
        catch (error) {
            this.log.error('Error closing peer connection.', error);
        }
    }
    async stopAssignedStream() {
        const token = this.token;
        if (token) {
            await this.camera.stopStream(token);
            if (this.token === token) {
                this.token = undefined;
            }
        }
    }
    async rejectIfTeardownRequested() {
        if (!this.teardownRequested)
            return;
        // initialize() can finish assigning a remote token after cleanup already
        // requested teardown. Retire that late token even though its successor
        // may already be acquiring.
        await this.stopAssignedStream();
        throw new Error('Nest stream initialization completed after teardown was requested.');
    }
}
exports.WebRtcSource = WebRtcSource;
// Start high so the first IDR is not throttled by the sender's conservative
// initial estimate. HomeKit's RECONFIGURE requests are not passed on: on a
// Mac they arrive ~10s into every view asking for ~132kbps, which would cap
// the camera at a fraction of what the network can carry.
WebRtcSource.ACTIVE_BITRATE = 4000000;
WebRtcSource.FIRST_RTP_TIMEOUT_MS = 30000;
//# sourceMappingURL=WebRtcSource.js.map