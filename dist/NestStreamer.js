"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
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
exports.WebRtcNestStreamer = exports.RtspNestStreamer = exports.NestStreamer = void 0;
exports.getStreamer = getStreamer;
const dgram_1 = require("dgram");
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const Traits = __importStar(require("./sdm/Traits"));
const pick_port_1 = __importDefault(require("pick-port"));
const WebRtcSource_1 = require("./WebRtcSource");
class NestStreamer {
    constructor(log, camera, config) {
        this.teardownRequested = false;
        this.log = log;
        this.camera = camera;
        this.config = config;
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
exports.NestStreamer = NestStreamer;
class RtspNestStreamer extends NestStreamer {
    async initialize() {
        var _a, _b;
        const streamInfo = await this.camera.generateStream();
        if (!streamInfo) {
            throw new Error(`Unable to start stream for ${this.camera.getDisplayName()}: no response from the Nest API (this is usually rate limiting — see the error above).`);
        }
        this.token = streamInfo.streamExtensionToken;
        await this.rejectIfTeardownRequested();
        return {
            args: `-analyzeduration ${(_a = this.config.analyzeDuration) !== null && _a !== void 0 ? _a : 15000000} -probesize ${(_b = this.config.probeSize) !== null && _b !== void 0 ? _b : 100000000} -i ` + streamInfo.streamUrls.rtspUrl
        };
    }
    async teardown() {
        this.teardownRequested = true;
        // initialize() can throw before a stream token is assigned. In that case
        // cleanup still runs, but there is no SDM stream to stop.
        await this.stopAssignedStream();
    }
}
exports.RtspNestStreamer = RtspNestStreamer;
class WebRtcNestStreamer extends NestStreamer {
    constructor() {
        super(...arguments);
        this.displayName = '';
    }
    // Write one forwarded packet to the capture file as [2-byte BE length][bytes].
    captureVideo(buf) {
        if (!this.captureStream)
            return;
        const len = Buffer.alloc(2);
        len.writeUInt16BE(buf.length, 0);
        this.captureStream.write(len);
        this.captureStream.write(Buffer.from(buf));
    }
    async initialize() {
        var _a, _b, _c;
        if (this.teardownRequested)
            throw new Error('Nest stream initialization completed after teardown was requested.');
        this.source = new WebRtcSource_1.WebRtcSource(this.log, this.camera, this.config);
        this.displayName = this.camera.getDisplayName();
        const captureDir = process.env.NEST_RTP_CAPTURE_DIR;
        if (captureDir) {
            const file = path.join(captureDir, `nest-rtp-${this.displayName.replace(/[^a-zA-Z0-9]+/g, '_')}-${Date.now()}.rtpdump`);
            try {
                this.captureStream = fs.createWriteStream(file);
                this.log.info(`Capturing video RTP sent to FFmpeg to ${file}`, this.displayName);
            }
            catch (e) {
                this.log.warn(`Could not open RTP capture file ${file}.`, (_a = e === null || e === void 0 ? void 0 : e.message) !== null && _a !== void 0 ? _a : e);
            }
        }
        this.udp = (0, dgram_1.createSocket)("udp4");
        const options = {
            type: 'udp',
            ip: '0.0.0.0',
            reserveTimeout: 15
        };
        const audioPort = await (0, pick_port_1.default)(options);
        const videoPort = await (0, pick_port_1.default)(options);
        this.unsubscribe = this.source.subscribe({
            onAudio: (packet) => {
                this.udp.send(packet, audioPort, "127.0.0.1");
            },
            onVideo: (packet) => {
                this.captureVideo(packet);
                this.udp.send(packet, videoPort, "127.0.0.1");
            }
        });
        await this.source.start();
        // An RTP capture of what FFmpeg actually receives proved the real cause of slow
        // startup: FFmpeg already has the video dimensions at the first keyframe (the
        // camera sends its SPS/PPS in-band with it — FIR makes sure of it), so it is NOT
        // waiting for parameter sets. It was burning the analyzeduration window
        // estimating the frame rate of a slow ~7.5fps stream. Lowering analyzeduration
        // attacked that but broke cameras whose first keyframe arrives after the cap,
        // because it also limits how long find_stream_info will WAIT for codec
        // parameters. -fpsprobesize 0 targets only the fps sampling: find_stream_info
        // returns as soon as it has codec parameters, while the full analyzeduration
        // window remains available for a late keyframe. Video is stream-copied, so fps
        // metadata is never used downstream.
        //
        // -max_ts_probe 0: even with codec parameters in hand, find_stream_info kept
        // reading until the H.264 parser had produced a packet with a dts, which takes
        // ~7 frames (~0.5s at 15fps). Worse, the Nest Hello sends one frame after its
        // keyframe and then only padding for ~4s, so FFmpeg sat on a decodable keyframe
        // the whole time. Replaying that pattern into FFmpeg 6.0 and 8.0, output started
        // at 4.4s without this and 0.1s with it, with identical output packets and
        // timestamps; a keyframe that arrives late is still waited for.
        return {
            args: `-protocol_whitelist pipe,crypto,udp,rtp,fd -analyzeduration ${(_b = this.config.analyzeDuration) !== null && _b !== void 0 ? _b : 15000000} -probesize ${(_c = this.config.probeSize) !== null && _c !== void 0 ? _c : 100000000} -fpsprobesize 0 -max_ts_probe 0 -i -`,
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
a=fmtp:97 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f
a=sendrecv`
        };
    }
    async teardown() {
        var _a, _b;
        this.teardownRequested = true;
        if (this.unsubscribe) {
            this.unsubscribe();
            this.unsubscribe = undefined;
        }
        if (this.captureStream) {
            this.captureStream.end();
            this.captureStream = undefined;
        }
        await ((_a = this.source) === null || _a === void 0 ? void 0 : _a.stop());
        try {
            await ((_b = this.udp) === null || _b === void 0 ? void 0 : _b.close());
        }
        catch (error) {
            this.log.error('Error closing UDP connection to FFMpeg.', error);
        }
    }
}
exports.WebRtcNestStreamer = WebRtcNestStreamer;
async function getStreamer(log, camera, config) {
    if ((await camera.getVideoProtocol()) === Traits.ProtocolType.WEB_RTC) {
        return new WebRtcNestStreamer(log, camera, config);
    }
    else {
        return new RtspNestStreamer(log, camera, config);
    }
}
//# sourceMappingURL=NestStreamer.js.map