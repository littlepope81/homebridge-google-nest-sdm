import {Camera} from "./sdm/Camera";
import {GenerateRtspStream} from "./sdm/Responses";
import {createSocket, Socket} from "dgram";
import * as fs from "fs";
import * as path from "path";
import * as Traits from "./sdm/Traits";
import {Logger} from "homebridge";
import pickPort, { pickPortOptions } from 'pick-port';
import {Config} from "./Config";
import {RtpSubscriber, WebRtcSource} from "./WebRtcSource";
import {SharedWebRtcSource} from "./SharedWebRtcSource";

export function waitForUdpListener(
    port: number,
    timeoutMs: number,
    signal?: AbortSignal
): Promise<boolean> {
    return new Promise(resolve => {
        // The probe relies on Linux reporting a refused datagram back to a connected
        // socket. macOS does not, so it would claim every port is listening; give FFmpeg
        // a fixed startup window there instead.
        if (process.platform !== 'linux') {
            const finish = (listening: boolean) => {
                clearTimeout(timer);
                signal?.removeEventListener('abort', abort);
                resolve(listening);
            };
            const abort = () => finish(false);
            const timer = setTimeout(() => finish(true), Math.min(1000, timeoutMs));
            signal?.addEventListener('abort', abort, {once: true});
            if (signal?.aborted)
                finish(false);
            return;
        }

        const socket = createSocket('udp4');
        const probe = Buffer.alloc(1);
        let settled = false;
        let consecutiveSuccesses = 0;
        let tickErrored = false;
        let tickTimer: ReturnType<typeof setTimeout> | undefined;
        let closePending = false;

        const closeSocket = () => {
            try {
                socket.close();
                closePending = false;
            } catch {
                closePending = true;
            }
        };

        const finish = (listening: boolean) => {
            if (settled) return;
            settled = true;
            clearTimeout(timeoutTimer);
            if (tickTimer)
                clearTimeout(tickTimer);
            signal?.removeEventListener('abort', abort);
            closeSocket();
            resolve(listening);
        };
        const abort = () => finish(false);
        const timeoutTimer = setTimeout(() => finish(false), timeoutMs);

        socket.on('error', () => {
            tickErrored = true;
        });
        socket.on('listening', () => {
            if (settled && closePending)
                closeSocket();
        });
        signal?.addEventListener('abort', abort, {once: true});
        if (signal?.aborted) {
            finish(false);
            return;
        }

        socket.connect(port, '127.0.0.1', () => {
            if (settled) {
                closeSocket();
                return;
            }
            const tick = () => {
                if (settled) return;
                tickErrored = false;
                // This is below RTP's minimum packet length, so FFmpeg discards it.
                socket.send(probe, error => {
                    if (error)
                        tickErrored = true;
                });
                tickTimer = setTimeout(() => {
                    if (tickErrored) {
                        consecutiveSuccesses = 0;
                    } else {
                        consecutiveSuccesses++;
                        if (consecutiveSuccesses >= 2) {
                            finish(true);
                            return;
                        }
                    }
                    tick();
                }, 25);
            };
            tick();
        });
    });
}

export interface NestStream {
    args: string,
    stdin?: string
}

export abstract class NestStreamer {
    protected token: string | undefined;
    protected teardownRequested = false;
    protected camera: Camera;
    protected log: Logger;
    protected config: Config;

    constructor(log: Logger, camera: Camera, config: Config) {
        this.log = log;
        this.camera = camera;
        this.config = config;
    }

    abstract initialize(): Promise<NestStream>;
    abstract teardown(): Promise<void>;

    protected async stopAssignedStream(): Promise<void> {
        const token = this.token;
        if (token) {
            await this.camera.stopStream(token);
            if (this.token === token) {
                this.token = undefined;
            }
        }
    }

    protected async rejectIfTeardownRequested(): Promise<void> {
        if (!this.teardownRequested) return;
        // initialize() can finish assigning a remote token after cleanup already
        // requested teardown. Retire that late token even though its successor
        // may already be acquiring.
        await this.stopAssignedStream();
        throw new Error('Nest stream initialization completed after teardown was requested.');
    }
}

export class RtspNestStreamer extends NestStreamer {
    async initialize(): Promise<NestStream> {
        const streamInfo = <GenerateRtspStream> await this.camera.generateStream();
        if (!streamInfo) {
            throw new Error(`Unable to start stream for ${this.camera.getDisplayName()}: no response from the Nest API (this is usually rate limiting — see the error above).`);
        }
        this.token = streamInfo.streamExtensionToken;
        await this.rejectIfTeardownRequested();
        return {
            args: `-analyzeduration ${this.config.analyzeDuration ?? 15000000} -probesize ${this.config.probeSize ?? 100000000} -i ` + streamInfo.streamUrls.rtspUrl
        };
    }

    async teardown(): Promise<void> {
        this.teardownRequested = true;
        // initialize() can throw before a stream token is assigned. In that case
        // cleanup still runs, but there is no SDM stream to stop.
        await this.stopAssignedStream();
    }
}

export class WebRtcNestStreamer extends NestStreamer {
    private udp: Socket | undefined;
    private source: WebRtcSource | undefined;
    private unsubscribe: (() => void) | undefined;
    // Debug-only: when NEST_RTP_CAPTURE_DIR is set, the exact video RTP bytes we hand
    // FFmpeg (including any synthetic packets we splice in) are written here, each
    // length-prefixed, so the stream FFmpeg actually receives can be inspected offline.
    private captureStream: fs.WriteStream | undefined;
    private displayName = '';
    private listenerAbort: AbortController | undefined;

    constructor(
        log: Logger,
        camera: Camera,
        config: Config,
        private readonly shared?: SharedWebRtcSource,
        private readonly replayMs?: number
    ) {
        super(log, camera, config);
    }

    // Write one forwarded packet to the capture file as [2-byte BE length][bytes].
    private captureVideo(buf: Buffer) {
        if (!this.captureStream) return;
        const len = Buffer.alloc(2);
        len.writeUInt16BE(buf.length, 0);
        this.captureStream.write(len);
        this.captureStream.write(Buffer.from(buf));
    }

    async initialize(): Promise<NestStream> {
        if (this.teardownRequested)
            throw new Error('Nest stream initialization completed after teardown was requested.');
        if (!this.shared)
            this.source = new WebRtcSource(this.log, this.camera, this.config);
        this.displayName = this.camera.getDisplayName();

        const captureDir = process.env.NEST_RTP_CAPTURE_DIR;
        if (captureDir) {
            const file = path.join(captureDir, `nest-rtp-${this.displayName.replace(/[^a-zA-Z0-9]+/g, '_')}-${Date.now()}.rtpdump`);
            try {
                this.captureStream = fs.createWriteStream(file);
                this.log.info(`Capturing video RTP sent to FFmpeg to ${file}`, this.displayName);
            } catch (e: any) {
                this.log.warn(`Could not open RTP capture file ${file}.`, e?.message ?? e);
            }
        }

        this.udp = createSocket("udp4");

        const options: pickPortOptions = {
            type: 'udp',
            ip: '0.0.0.0',
            reserveTimeout: 15
        };
        const audioPort = await pickPort(options);
        const videoPort = await pickPort(options);
        if (this.shared && this.teardownRequested)
            throw new Error('Nest stream initialization completed after teardown was requested.');
        const subscriber: RtpSubscriber = {
            onAudio: (packet) => {
                this.udp!.send(packet, audioPort, "127.0.0.1");
            },
            onVideo: (packet) => {
                this.captureVideo(packet);
                this.udp!.send(packet, videoPort, "127.0.0.1");
            }
        };
        if (this.shared) {
            if (this.replayMs === undefined) {
                this.unsubscribe = this.shared.subscribe(subscriber);
            } else {
                let resolveReady!: (listening: boolean) => void;
                const ready = new Promise<boolean>(resolve => resolveReady = resolve);
                this.unsubscribe = this.shared.subscribeWithReplay(subscriber, this.replayMs, ready);
                if (this.unsubscribe) {
                    const abort = new AbortController();
                    this.listenerAbort = abort;
                    void waitForUdpListener(videoPort, 3000, abort.signal).then(resolveReady).finally(() => {
                        if (this.listenerAbort === abort)
                            this.listenerAbort = undefined;
                    });
                } else {
                    resolveReady(false);
                }
            }
            if (this.unsubscribe)
                this.log.debug('Attached to always-on stream.', this.displayName);
        }
        if (!this.unsubscribe) {
            if (!this.source)
                this.source = new WebRtcSource(this.log, this.camera, this.config);
            this.unsubscribe = this.source.subscribe(subscriber);
            await this.source.start();
        }

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
        const parameterSets = this.shared?.getParameterSets();
        const parameterSetFmtp = parameterSets
            ? `;sprop-parameter-sets=${parameterSets.sps},${parameterSets.pps}`
            : '';
        return {
            args: `-protocol_whitelist pipe,crypto,udp,rtp,fd -analyzeduration ${this.config.analyzeDuration ?? 15000000} -probesize ${this.config.probeSize ?? 100000000} -fpsprobesize 0 -max_ts_probe 0 -i -`,
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
a=fmtp:97 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f${parameterSetFmtp}
a=sendrecv`
        }
    }

    async teardown(): Promise<void> {
        this.teardownRequested = true;
        this.listenerAbort?.abort();
        this.listenerAbort = undefined;
        if (this.unsubscribe) {
            this.unsubscribe();
            this.unsubscribe = undefined;
        }

        if (this.captureStream) {
            this.captureStream.end();
            this.captureStream = undefined;
        }

        await this.source?.stop();

        try {
            await this.udp?.close();
        } catch (error: any) {
            this.log.error('Error closing UDP connection to FFMpeg.', error);
        }
    }
}

export async function getStreamer(
    log: Logger,
    camera: Camera,
    config: Config,
    shared?: SharedWebRtcSource,
    replayMs?: number
): Promise<NestStreamer> {
    if ((await camera.getVideoProtocol()) === Traits.ProtocolType.WEB_RTC) {
        return new WebRtcNestStreamer(log, camera, config, shared, replayMs);
    } else {
        return new RtspNestStreamer(log, camera, config);
    }
}
