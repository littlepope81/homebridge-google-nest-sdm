import {Camera} from "./sdm/Camera";
import {GenerateRtspStream, GenerateWebRtcStream} from "./sdm/Responses";
import {createSocket, Socket} from "dgram";
import {RTCPeerConnection, RTCRtpCodecParameters} from "werift";
import * as Traits from "./sdm/Traits";
import {Logger} from "homebridge";
import pickPort, { pickPortOptions } from 'pick-port';
import {StreamParamCache} from "./StreamParamCache";
import {extractParameterSets} from "./H264";

export interface NestStream {
    args: string,
    stdin?: string
}

export abstract class NestStreamer {
    protected token: string | undefined;
    protected camera: Camera;
    protected log: Logger;
    protected streamParamCache: StreamParamCache;

    constructor(log: Logger, camera: Camera, streamParamCache: StreamParamCache) {
        this.log = log;
        this.camera = camera;
        this.streamParamCache = streamParamCache;
    }

    abstract initialize(): Promise<NestStream>;
    abstract teardown(): void;
}

export class RtspNestStreamer extends NestStreamer {
    async initialize(): Promise<NestStream> {
        const streamInfo = <GenerateRtspStream> await this.camera.generateStream();
        this.token = streamInfo.streamExtensionToken;
        return {
            args: '-analyzeduration 15000000 -probesize 100000000 -i ' + streamInfo.streamUrls.rtspUrl
        };
    }

    async teardown(): Promise<void> {
        await this.camera.stopStream(this.token!);
    }
}

export class WebRtcNestStreamer extends NestStreamer {
    private udp: Socket | undefined;
    private pc: RTCPeerConnection | undefined;

    async initialize(): Promise<NestStream> {

        this.udp = createSocket("udp4");

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

        const options: pickPortOptions = {
          type: 'udp',
          ip: '0.0.0.0',
          reserveTimeout: 15
        };
        const audioPort = await pickPort(options);
        const audioTransceiver = this.pc.addTransceiver("audio", {direction: "recvonly"});
        audioTransceiver.onTrack.subscribe((track) => {
            audioTransceiver.sender.replaceTrack(track);
            track.onReceiveRtp.subscribe((rtp) => {
                this.udp!.send(rtp.serialize(), audioPort, "127.0.0.1");
            });
        });

        const deviceId = this.camera.getName();
        let capturedSps: Buffer | undefined;
        let capturedPps: Buffer | undefined;

        const videoPort = await pickPort(options);
        const videoTransceiver = this.pc.addTransceiver("video", {direction: "recvonly"});
        videoTransceiver.onTrack.subscribe((track) => {
            videoTransceiver.sender.replaceTrack(track);
            track.onReceiveRtp.subscribe((rtp) => {
                // Learn this camera's H.264 parameter sets from the live stream so future
                // streams can prime FFmpeg with them up front (see sprop-parameter-sets below).
                if (!capturedSps || !capturedPps) {
                    const {sps, pps} = extractParameterSets(rtp.payload);
                    if (sps) capturedSps = sps;
                    if (pps) capturedPps = pps;
                    if (capturedSps && capturedPps) {
                        this.streamParamCache.set(deviceId, {
                            sps: capturedSps.toString('base64'),
                            pps: capturedPps.toString('base64')
                        });
                    }
                }
                this.udp!.send(rtp.serialize(), videoPort, "127.0.0.1");
            });
            track.onReceiveRtp.once(() => {
                // Request a keyframe immediately instead of waiting a full interval for the
                // first one. Until an IDR frame arrives FFmpeg can't produce a decodable
                // picture, so firing the initial PLI right away shaves keyframe-wait latency
                // (the dominant cost of stream startup) off the time to first frame.
                videoTransceiver.receiver.sendRtcpPLI(track.ssrc!);
                setInterval(() => videoTransceiver.receiver.sendRtcpPLI(track.ssrc!), 2000);
            });
        });

        this.pc.createDataChannel('dataSendChannel', {id: 1});

        let offer = await this.pc.createOffer();
        await this.pc.setLocalDescription(offer);

        const streamInfo = <GenerateWebRtcStream> await this.camera.generateStream(offer.sdp);
        this.token = streamInfo.mediaSessionId;
        await this.pc.setRemoteDescription({
            type: 'answer',
            sdp: streamInfo.answerSdp
        });

        // If we've learned this camera's parameter sets on a previous stream, hand
        // them to FFmpeg up front via sprop-parameter-sets so it knows the video
        // dimensions immediately instead of probing them out of the live stream.
        const cached = this.streamParamCache.get(deviceId);
        let videoFmtp = 'a=fmtp:97 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f';
        if (cached) {
            videoFmtp += `;sprop-parameter-sets=${cached.sps},${cached.pps}`;
            this.log.debug('Priming FFmpeg with cached H.264 parameter sets.', this.camera.getDisplayName());
        }

        return {
            args: `-protocol_whitelist pipe,crypto,udp,rtp,fd -analyzeduration 15000000 -probesize 100000000 -i -`,
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
        }
    }

    async teardown(): Promise<void> {
        try {
            await this.camera.stopStream(this.token!);
        } catch (error: any) {
            this.log.error('Error stopping camera stream.', error);
        }

        try {
            await this.pc?.close();
        } catch (error: any) {
            this.log.error('Error closing peer connection.', error);
        }

        try {
            await this.udp?.close();
        } catch (error: any) {
            this.log.error('Error closing UDP connection to FFMpeg.', error);
        }
    }
}

export async function getStreamer(log: Logger, camera: Camera, streamParamCache: StreamParamCache): Promise<NestStreamer> {
    if ((await camera.getVideoProtocol()) === Traits.ProtocolType.WEB_RTC) {
        return new WebRtcNestStreamer(log, camera, streamParamCache);
    } else {
        return new RtspNestStreamer(log, camera, streamParamCache);
    }
}