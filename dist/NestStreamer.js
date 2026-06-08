"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getStreamer = exports.WebRtcNestStreamer = exports.RtspNestStreamer = exports.NestStreamer = void 0;
const dgram_1 = require("dgram");
const werift_1 = require("werift");
const Traits = __importStar(require("./sdm/Traits"));
const pick_port_1 = __importDefault(require("pick-port"));
const H264_1 = require("./H264");
class NestStreamer {
    constructor(log, camera, streamParamCache) {
        this.log = log;
        this.camera = camera;
        this.streamParamCache = streamParamCache;
    }
}
exports.NestStreamer = NestStreamer;
class RtspNestStreamer extends NestStreamer {
    async initialize() {
        const streamInfo = await this.camera.generateStream();
        this.token = streamInfo.streamExtensionToken;
        return {
            args: '-analyzeduration 15000000 -probesize 100000000 -i ' + streamInfo.streamUrls.rtspUrl
        };
    }
    async teardown() {
        await this.camera.stopStream(this.token);
    }
}
exports.RtspNestStreamer = RtspNestStreamer;
class WebRtcNestStreamer extends NestStreamer {
    async initialize() {
        // Diagnostics: log a timeline of WebRTC startup milestones relative to this
        // point, so we can see where the time-to-first-frame actually goes
        // (connection setup vs. first RTP vs. first keyframe). Debug level only.
        const t0 = Date.now();
        const name = this.camera.getDisplayName();
        const mark = (label) => this.log.debug(`[startup +${Date.now() - t0}ms] ${label}`, name);
        this.udp = (0, dgram_1.createSocket)("udp4");
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
        this.pc.iceConnectionStateChange.subscribe((state) => mark(`iceConnectionState: ${state}`));
        this.pc.connectionStateChange.subscribe((state) => mark(`connectionState: ${state}`));
        const options = {
            type: 'udp',
            ip: '0.0.0.0',
            reserveTimeout: 15
        };
        const audioPort = await (0, pick_port_1.default)(options);
        const audioTransceiver = this.pc.addTransceiver("audio", { direction: "recvonly" });
        audioTransceiver.onTrack.subscribe((track) => {
            audioTransceiver.sender.replaceTrack(track);
            track.onReceiveRtp.subscribe((rtp) => {
                this.udp.send(rtp.serialize(), audioPort, "127.0.0.1");
            });
        });
        const deviceId = this.camera.getName();
        const cached = this.streamParamCache.get(deviceId);
        let capturedSps;
        let capturedPps;
        let sawFirstVideoRtp = false;
        let sawFirstKeyframe = false;
        let injectedParams = false;
        // Original sequence number of the keyframe packet we splice our SPS/PPS in
        // front of. Once set, every packet at or after this point is renumbered +1 to
        // make room for the one synthetic packet. Serial-number arithmetic (RFC 1982)
        // leaves out-of-order stragglers from *before* the splice untouched, so they
        // can't collide with the injected packet's slot.
        let injectAtSeq;
        const videoPort = await (0, pick_port_1.default)(options);
        const videoTransceiver = this.pc.addTransceiver("video", { direction: "recvonly" });
        videoTransceiver.onTrack.subscribe((track) => {
            mark('video track received');
            videoTransceiver.sender.replaceTrack(track);
            track.onReceiveRtp.subscribe((rtp) => {
                if (!sawFirstVideoRtp) {
                    sawFirstVideoRtp = true;
                    mark('first video RTP packet');
                }
                const isKeyframe = (0, H264_1.containsKeyframe)(rtp.payload);
                if (isKeyframe && !sawFirstKeyframe) {
                    sawFirstKeyframe = true;
                    mark('first video keyframe (IDR)');
                }
                // Splice our cached SPS/PPS in as a proper access unit immediately before
                // the first keyframe: same timestamp and SSRC as the IDR, sequenced right
                // in front of it, with every following packet renumbered +1. This mimics
                // exactly how the camera delivers its own parameter sets (SPS→PPS→IDR in
                // one access unit) — the only form FFmpeg actually honors — so it gets the
                // dimensions at the first keyframe (~1s) instead of waiting for the camera's
                // next periodic SPS (up to ~15s).
                if (cached && !injectedParams && isKeyframe) {
                    injectedParams = true;
                    injectAtSeq = rtp.header.sequenceNumber;
                    const psPacket = (0, H264_1.buildParameterSetRtpPacket)({
                        sps: Buffer.from(cached.sps, 'base64'),
                        pps: Buffer.from(cached.pps, 'base64'),
                        payloadType: rtp.header.payloadType,
                        sequenceNumber: injectAtSeq,
                        timestamp: rtp.header.timestamp,
                        ssrc: rtp.header.ssrc
                    });
                    this.udp.send(psPacket, videoPort, "127.0.0.1");
                    mark('injected cached SPS/PPS in-band before keyframe');
                }
                // Learn this camera's H.264 parameter sets from the live stream so future
                // streams can be primed (reads the original payload, before any renumbering).
                if (!capturedSps || !capturedPps) {
                    const { sps, pps } = (0, H264_1.extractParameterSets)(rtp.payload);
                    if (sps)
                        capturedSps = sps;
                    if (pps)
                        capturedPps = pps;
                    if (capturedSps && capturedPps) {
                        this.streamParamCache.set(deviceId, {
                            sps: capturedSps.toString('base64'),
                            pps: capturedPps.toString('base64')
                        });
                    }
                }
                if (injectAtSeq !== undefined && ((rtp.header.sequenceNumber - injectAtSeq) & 0xffff) < 0x8000)
                    rtp.header.sequenceNumber = (rtp.header.sequenceNumber + 1) & 0xffff;
                this.udp.send(rtp.serialize(), videoPort, "127.0.0.1");
            });
            track.onReceiveRtp.once(() => {
                const receiver = videoTransceiver.receiver;
                let firSeq = 0;
                // Request a keyframe immediately, via both PLI and FIR. PLI ("picture loss")
                // asks for a recovery picture, which these cameras answer with an IDR but
                // *without* SPS/PPS — leaving FFmpeg to wait many seconds for the camera's
                // next periodic parameter sets. FIR ("full intra request", RFC 5104) asks
                // for a full intra frame, which encoders typically resend *with* the
                // parameter sets. The hope: get the camera's own SPS to FFmpeg up front so
                // stream detection finishes fast. FIR needs a per-SSRC sequence number that
                // increments each request, or the camera ignores repeats.
                const requestKeyframe = () => {
                    var _a;
                    receiver.sendRtcpPLI(track.ssrc).catch((e) => { var _a; return this.log.debug('PLI send failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e); });
                    try {
                        const fir = new werift_1.RtcpPayloadSpecificFeedback({
                            feedback: (0, H264_1.buildFirFeedback)(receiver.rtcpSsrc, track.ssrc, firSeq++)
                        });
                        receiver.dtlsTransport.sendRtcp([fir]).catch((e) => { var _a; return this.log.debug('FIR send failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e); });
                    }
                    catch (e) {
                        this.log.debug('FIR build failed.', (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e);
                    }
                };
                requestKeyframe();
                this.keyframeRequestInterval = setInterval(requestKeyframe, 2000);
            });
        });
        this.pc.createDataChannel('dataSendChannel', { id: 1 });
        let offer = await this.pc.createOffer();
        await this.pc.setLocalDescription(offer);
        mark('sending offer to Nest (GenerateWebRtcStream)');
        const streamInfo = await this.camera.generateStream(offer.sdp);
        mark('received answer from Nest');
        this.token = streamInfo.mediaSessionId;
        await this.pc.setRemoteDescription({
            type: 'answer',
            sdp: streamInfo.answerSdp
        });
        mark('remote description set; returning to start FFmpeg');
        // If we've learned this camera's parameter sets on a previous stream, hand
        // them to FFmpeg up front via sprop-parameter-sets so it knows the video
        // dimensions immediately instead of probing them out of the live stream.
        //
        // Crucially, advertise the profile-level-id that actually matches the cached
        // SPS, and (when primed) drop analyzeduration/probesize low. We inject the
        // parameter sets in-band as the first packet (above), so FFmpeg has the video
        // dimensions immediately — which is what made low analyzeduration fail before.
        // The remaining delay was find_stream_info reading up to analyzeduration worth
        // of *stream-PTS time*; on cameras that emit frames slowly at startup that maps
        // almost 1:1 to wall-clock seconds. With dimensions already in hand there's
        // nothing left to probe, so a low cap lets it return right away. On the first
        // (learning) stream nothing is cached yet, so we keep the safe 15s defaults.
        let videoFmtp = 'a=fmtp:97 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f';
        const analyzeDuration = 15000000;
        const probeSize = 100000000;
        if (cached) {
            const spsBytes = Buffer.from(cached.sps, 'base64');
            const profileLevelId = spsBytes.length >= 4 ? spsBytes.subarray(1, 4).toString('hex') : '42e01f';
            videoFmtp = `a=fmtp:97 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=${profileLevelId};sprop-parameter-sets=${cached.sps},${cached.pps}`;
            this.log.debug(`Priming FFmpeg with cached H.264 parameter sets (profile-level-id=${profileLevelId}).`, this.camera.getDisplayName());
        }
        return {
            args: `-protocol_whitelist pipe,crypto,udp,rtp,fd -analyzeduration ${analyzeDuration} -probesize ${probeSize} -i -`,
            stdin: `v=0
o=- 0 0 IN IP4 127.0.0.1
s=-
c=IN IP4 127.0.0.1
t=0 0
m=audio ${audioPort} UDP 96
a=rtpmap:96 opus/48000/2
a=fmtp:96 minptime=10;useinbandfec=1
a=rtcp-fb:96 transport-cc
a=sendrecv
m=video ${videoPort} UDP 97
a=rtpmap:97 H264/90000
a=rtcp-fb:97 ccm fir
a=rtcp-fb:97 nack
a=rtcp-fb:97 nack pli
a=rtcp-fb:97 goog-remb
${videoFmtp}
a=sendrecv`
        };
    }
    async teardown() {
        var _a, _b;
        if (this.keyframeRequestInterval) {
            clearInterval(this.keyframeRequestInterval);
            this.keyframeRequestInterval = undefined;
        }
        try {
            await this.camera.stopStream(this.token);
        }
        catch (error) {
            this.log.error('Error stopping camera stream.', error);
        }
        try {
            await ((_a = this.pc) === null || _a === void 0 ? void 0 : _a.close());
        }
        catch (error) {
            this.log.error('Error closing peer connection.', error);
        }
        try {
            await ((_b = this.udp) === null || _b === void 0 ? void 0 : _b.close());
        }
        catch (error) {
            this.log.error('Error closing UDP connection to FFMpeg.', error);
        }
    }
}
exports.WebRtcNestStreamer = WebRtcNestStreamer;
async function getStreamer(log, camera, streamParamCache) {
    if ((await camera.getVideoProtocol()) === Traits.ProtocolType.WEB_RTC) {
        return new WebRtcNestStreamer(log, camera, streamParamCache);
    }
    else {
        return new RtspNestStreamer(log, camera, streamParamCache);
    }
}
exports.getStreamer = getStreamer;
//# sourceMappingURL=NestStreamer.js.map