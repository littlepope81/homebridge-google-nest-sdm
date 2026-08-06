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
exports.StreamingDelegate = void 0;
const dgram_1 = require("dgram");
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const os_1 = __importDefault(require("os"));
const systeminformation_1 = require("systeminformation");
const FfMpegProcess_1 = require("./FfMpegProcess");
const NestStreamer_1 = require("./NestStreamer");
const HksvStreamer_1 = __importDefault(require("./HksvStreamer"));
const pick_port_1 = __importDefault(require("pick-port"));
class StreamingDelegate {
    constructor(log, api, platform, camera, accessory) {
        // keep track of sessions
        this.pendingSessions = {};
        this.ongoingSessions = {};
        // Bitrate from a RECONFIGURE that arrived before its START finished setting up
        // the session; applied once the session registers.
        this.pendingMaxBitrate = {};
        this.recordingActive = false;
        this.inFlightAcquisitions = new Set();
        this.nextSessionToken = 1;
        this.shuttingDown = false;
        this.platform = platform;
        this.log = log;
        this.hap = api.hap;
        this.config = platform.platformConfig;
        this.camera = camera;
        this.accessory = accessory;
        api.on("shutdown" /* SHUTDOWN */, async () => {
            var _a, _b;
            this.shuttingDown = true;
            const acquisitions = Array.from(this.inFlightAcquisitions);
            acquisitions.forEach(acquisition => acquisition.cancel = true);
            const recording = (_a = this.recordingSessionInfo) === null || _a === void 0 ? void 0 : _a.session;
            const prewarm = this.prewarm;
            const cleanup = [];
            acquisitions.forEach(acquisition => {
                if (acquisition.session)
                    cleanup.push(this.cleanupSession(acquisition.session));
            });
            if (prewarm)
                cleanup.push(this.cleanupSession(prewarm));
            if (recording && recording.token !== (prewarm === null || prewarm === void 0 ? void 0 : prewarm.token))
                cleanup.push(this.cleanupSession(recording));
            const prewarmSetup = this.prewarmSetup;
            // Backstop: bound the whole shutdown join so a single unsettled promise (a leaked
            // acquisition `done`, a stalled teardown) can never hang process exit forever.
            await Promise.race([
                Promise.allSettled([
                    ...Object.keys(this.ongoingSessions).map(session => this.stopStream(session)),
                    ...acquisitions.map(acquisition => acquisition.done),
                    ...(prewarmSetup ? [prewarmSetup] : []),
                    ...cleanup,
                ]),
                new Promise(resolve => setTimeout(resolve, 5000)),
            ]);
            // A cancelled acquisition can expose its Session only while settling.
            // Recheck after joining it and initiate cleanup for anything it published.
            const lateRecording = (_b = this.recordingSessionInfo) === null || _b === void 0 ? void 0 : _b.session;
            const latePrewarm = this.prewarm;
            const lateCleanup = [];
            if (latePrewarm)
                lateCleanup.push(this.cleanupSession(latePrewarm));
            if (lateRecording && lateRecording.token !== (latePrewarm === null || latePrewarm === void 0 ? void 0 : latePrewarm.token))
                lateCleanup.push(this.cleanupSession(lateRecording));
            await Promise.allSettled(lateCleanup);
        });
        // Hand the accessory's existing MotionSensor (created by MotionAccessory
        // before any delegate is constructed) to the camera controller: HAP then
        // advertises EventTriggerOption.MOTION in the HKSV supported configuration,
        // links the sensor to RecordingManagement, and adds StatusActive. Without
        // this, plain cameras advertise an EMPTY trigger set and motion recordings
        // depend on Apple-hub heuristics. The service stays caller-managed.
        const motionService = accessory.getService(this.hap.Service.MotionSensor);
        this.options = {
            // Number of CONCURRENT streams (one RTPStreamManagement service each), not
            // the resolutions list. This passed resolutions.length (11), creating 11
            // stream services per camera — a wall of duplicate camera tiles in the
            // Homebridge accessories view, and far more than a Nest camera can serve
            // at once. Two is plenty for a live view plus a HomeKit hub recording.
            // HAP prunes the excess cached services on restore when this shrinks.
            cameraStreamCount: 2,
            delegate: this,
            ...(motionService ? { sensors: { motion: motionService } } : {}),
            streamingOptions: {
                supportedCryptoSuites: [0 /* AES_CM_128_HMAC_SHA1_80 */],
                video: {
                    resolutions: camera.getResolutions(),
                    codec: {
                        profiles: [1 /* MAIN */],
                        levels: [0 /* LEVEL3_1 */]
                    }
                },
                audio: {
                    twoWayAudio: false,
                    codecs: [
                        {
                            type: "AAC-eld" /* AAC_ELD */,
                            samplerate: 16 /* KHZ_16 */,
                            audioChannels: 1
                        }
                    ]
                }
            },
            recording: {
                delegate: this,
                options: {
                    // Mandatory in CameraRecordingOptions with a documented floor of 4000ms, so this
                    // cannot be set to "none" -- every plugin advertises at least 4000 whether or not
                    // it has a prebuffer behind it. Nothing backs it here yet: motionPrewarm only
                    // starts buffering once an event has been delivered. See issue #233.
                    prebufferLength: 4000,
                    mediaContainerConfiguration: {
                        type: 0 /* FRAGMENTED_MP4 */,
                        fragmentLength: 4000,
                    },
                    video: {
                        type: 0 /* H264 */,
                        parameters: {
                            profiles: [2 /* HIGH */],
                            levels: [2 /* LEVEL4_0 */],
                        },
                        resolutions: [
                            [320, 180, 30],
                            [320, 240, 15],
                            [320, 240, 30],
                            [480, 270, 30],
                            [480, 360, 30],
                            [640, 360, 30],
                            [640, 480, 30],
                            [1280, 720, 30],
                            [1280, 960, 30],
                            [1920, 1080, 30],
                            [1600, 1200, 30],
                        ],
                    },
                    audio: {
                        codecs: {
                            type: 1 /* AAC_ELD */,
                            audioChannels: 1,
                            samplerate: 5 /* KHZ_48 */,
                            bitrateMode: 0 /* VARIABLE */,
                        },
                    },
                }
            }
        };
    }
    /**
     * Path of the periodically-refreshed JPEG that live and HKSV streams write
     * for this camera (see the snapshot output appended to the FFmpeg commands).
     */
    snapshotFilePath() {
        return path.join(this.platform.snapshotDir, this.accessory.UUID + '.jpg');
    }
    /**
     * FFmpeg output group that decodes the (otherwise stream-copied) video at a
     * low rate and keeps a single JPEG updated on disk, giving HomeKit tiles a
     * real "last seen" frame — SDM offers no snapshot API, so without this the
     * tiles only ever show a static placeholder logo.
     */
    snapshotOutputArgs() {
        // Unavailable directory → no snapshot output at all: a broken extra output
        // would otherwise take the entire FFmpeg command (and the stream) down.
        // -atomic_writing makes each frame a temp-file+rename, so a concurrent
        // reader or second writer (live view + HKSV recording) never sees a torn file.
        if (!this.platform.snapshotDir)
            return [];
        return [
            '-an', '-sn', '-dn',
            '-codec:v', 'mjpeg',
            '-q:v', '4',
            '-vf', 'fps=1/2,scale=640:-2',
            '-f', 'image2',
            '-update', '1',
            '-atomic_writing', '1',
            '-y', this.snapshotFilePath()
        ];
    }
    handleSnapshotRequest(request, callback) {
        this.log.debug(`Snapshot requested (reason: ${request.reason === undefined ? 'unspecified' : request.reason === 0 /* PERIODIC */ ? 'periodic' : 'event'})`, this.camera.getDisplayName());
        if (request.reason === 1 /* EVENT */) {
            const image = this.camera.getCachedEventImage();
            if (image) {
                callback(undefined, image);
                return;
            }
        }
        if (!this.platform.snapshotDir) {
            this.camera.getSnapshot()
                .then(result => callback(undefined, result))
                .catch(error => callback(error));
            return;
        }
        const snapshotFile = this.snapshotFilePath();
        fs.promises.stat(snapshotFile)
            .then(stats => {
            if (Date.now() - stats.mtimeMs > StreamingDelegate.SNAPSHOT_MAX_AGE_MS)
                throw new Error('snapshot file too old');
            return fs.promises.readFile(snapshotFile);
        })
            .then(image => {
            // Serve the file only if it is a structurally complete JPEG (SOI...EOI);
            // a partial file (killed FFmpeg, disk full) must fall back, not break the tile.
            if (image.length >= 4 && image[0] === 0xff && image[1] === 0xd8
                && image[image.length - 2] === 0xff && image[image.length - 1] === 0xd9) {
                callback(undefined, image);
            }
            else {
                throw new Error('incomplete snapshot file');
            }
        })
            .catch(() => this.camera.getSnapshot()
            .then(result => callback(undefined, result))
            .catch(error => callback(error)));
    }
    static determineResolution(request) {
        let width = request.width;
        let height = request.height;
        const filters = [];
        if (width > 0 || height > 0) {
            filters.push('scale=' + (width > 0 ? '\'min(' + width + ',iw)\'' : 'iw') + ':' +
                (height > 0 ? '\'min(' + height + ',ih)\'' : 'ih') +
                ':force_original_aspect_ratio=decrease');
            filters.push('scale=trunc(iw/2)*2:trunc(ih/2)*2'); // Force to fit encoder restrictions
        }
        return {
            width: width,
            height: height,
            videoFilter: filters.join(',')
        };
    }
    async getIpAddress(ipv6) {
        var _a;
        const interfaceName = await (0, systeminformation_1.networkInterfaceDefault)();
        const interfaces = os_1.default.networkInterfaces();
        // @ts-ignore
        const externalInfo = (_a = interfaces[interfaceName]) === null || _a === void 0 ? void 0 : _a.filter((info) => {
            return !info.internal;
        });
        const preferredFamily = ipv6 ? 'IPv6' : 'IPv4';
        const addressInfo = (externalInfo === null || externalInfo === void 0 ? void 0 : externalInfo.find((info) => {
            return info.family === preferredFamily;
        })) || (externalInfo === null || externalInfo === void 0 ? void 0 : externalInfo[0]);
        if (!addressInfo) {
            throw new Error('Unable to get network address for "' + interfaceName + '"!');
        }
        return addressInfo.address;
    }
    /**
     * Some callback methods do not log anything if they are called with an error.
     */
    logThenCallback(callback, message) {
        this.log.error(message);
        callback(new Error(message));
    }
    async prepareStream(request, callback) {
        const camaraInfo = await this.camera.getCameraLiveStream();
        if (!camaraInfo) {
            this.logThenCallback(callback, 'Unable to start stream! Camera info was not received');
            return;
        }
        const ipv6 = request.addressVersion === 'ipv6';
        const options = {
            type: 'udp',
            ip: ipv6 ? '::' : '0.0.0.0',
            reserveTimeout: 15
        };
        const videoReturnPort = await (0, pick_port_1.default)(options);
        const videoSSRC = this.hap.CameraController.generateSynchronisationSource();
        const audioReturnPort = await (0, pick_port_1.default)(options);
        const audioSSRC = this.hap.CameraController.generateSynchronisationSource();
        const currentAddress = await this.getIpAddress(ipv6);
        const sessionInfo = {
            address: request.targetAddress,
            localAddress: currentAddress,
            ipv6: ipv6,
            videoPort: request.video.port,
            videoReturnPort: videoReturnPort,
            videoCryptoSuite: request.video.srtpCryptoSuite,
            videoSRTP: Buffer.concat([request.video.srtp_key, request.video.srtp_salt]),
            videoSSRC: videoSSRC,
            audioPort: request.audio.port,
            audioReturnPort: audioReturnPort,
            audioCryptoSuite: request.audio.srtpCryptoSuite,
            audioSRTP: Buffer.concat([request.audio.srtp_key, request.audio.srtp_salt]),
            audioSSRC: audioSSRC
        };
        const response = {
            address: currentAddress,
            video: {
                port: videoReturnPort,
                ssrc: videoSSRC,
                srtp_key: request.video.srtp_key,
                srtp_salt: request.video.srtp_salt
            },
            audio: {
                port: audioReturnPort,
                ssrc: audioSSRC,
                srtp_key: request.audio.srtp_key,
                srtp_salt: request.audio.srtp_salt
            }
        };
        this.pendingSessions[request.sessionID] = sessionInfo;
        callback(undefined, response);
    }
    async startStream(request, callback) {
        const sessionInfo = this.pendingSessions[request.sessionID];
        const resolution = StreamingDelegate.determineResolution(request.video);
        const bitrate = request.video.max_bit_rate * 4;
        const vEncoder = this.config.vEncoder || 'libx264 -preset ultrafast -tune zerolatency';
        this.log.debug(`Video stream requested: ${request.video.width} x ${request.video.height}, ${request.video.fps} fps, ${request.video.max_bit_rate} kbps`, this.camera.getDisplayName());
        const nestStreamer = await (0, NestStreamer_1.getStreamer)(this.log, this.camera, this.config);
        let ffmpegArgs;
        let nestStream;
        try {
            nestStream = await nestStreamer.initialize(); // '-analyzeduration 15000000 -probesize 100000000 -i ' + streamInfo.streamUrls.rtspUrl;
            ffmpegArgs = nestStream.args;
        }
        catch (error) {
            this.logThenCallback(callback, error);
            return;
        }
        ffmpegArgs += // Video
            ' -an -sn -dn' +
                ` -codec:v ${vEncoder}` +
                ' -f rawvideo' +
                ' -pix_fmt yuv420p' +
                ' -color_range mpeg';
        if (vEncoder !== 'copy') {
            ffmpegArgs +=
                ' -bf 0' +
                    ` -r ${request.video.fps}` +
                    ` -b:v ${bitrate}k` +
                    ` -bufsize ${bitrate}k` +
                    ` -maxrate ${2 * bitrate}k` +
                    ' -filter:v ' + resolution.videoFilter;
        }
        ffmpegArgs += ' -payload_type ' + request.video.pt;
        ffmpegArgs += // Video Stream
            ' -ssrc ' + sessionInfo.videoSSRC +
                ' -f rtp' +
                ' -srtp_out_suite AES_CM_128_HMAC_SHA1_80' +
                ' -srtp_out_params ' + sessionInfo.videoSRTP.toString('base64') +
                ' srtp://' + sessionInfo.address + ':' + sessionInfo.videoPort +
                '?rtcpport=' + sessionInfo.videoPort + '&pkt_size=' + request.video.mtu;
        ffmpegArgs += // Audio
            ' -vn -sn -dn' +
                ' -codec:a libfdk_aac' +
                ' -profile:a aac_eld' +
                ' -flags +global_header' +
                ' -ar ' + request.audio.sample_rate + 'k' +
                ' -b:a ' + request.audio.max_bit_rate + 'k' +
                ' -ac ' + request.audio.channel +
                ' -payload_type ' + request.audio.pt;
        ffmpegArgs += // Audio Stream
            ' -ssrc ' + sessionInfo.audioSSRC +
                ' -f rtp' +
                ' -srtp_out_suite AES_CM_128_HMAC_SHA1_80' +
                ' -srtp_out_params ' + sessionInfo.audioSRTP.toString('base64') +
                ' srtp://' + sessionInfo.address + ':' + sessionInfo.audioPort +
                '?rtcpport=' + sessionInfo.audioPort + '&pkt_size=188';
        // ffmpegArgs is a whitespace-split STRING (see FfmpegProcess), so a snapshot
        // path containing spaces would shatter the whole command and kill the stream.
        // Skip the snapshot output in that case — the HKSV path passes args as an
        // array and keeps working regardless.
        const snapshotArgs = this.snapshotOutputArgs();
        if (snapshotArgs.length > 0 && !/\s/.test(this.snapshotFilePath())) {
            ffmpegArgs += ' ' + snapshotArgs.join(' ');
        }
        else if (snapshotArgs.length > 0) {
            this.log.debug('Snapshot path contains whitespace; skipping snapshot output on the live stream.', this.camera.getDisplayName());
        }
        if (this.platform.debugMode) {
            ffmpegArgs += ' -loglevel level+verbose';
        }
        const activeSession = { streamer: nestStreamer };
        try {
            activeSession.socket = (0, dgram_1.createSocket)(sessionInfo.ipv6 ? 'udp6' : 'udp4');
            activeSession.socket.on('error', (err) => {
                this.log.error('Socket error: ' + err.name, this.camera.getDisplayName());
                this.stopStream(request.sessionID);
            });
            activeSession.socket.on('message', () => {
                if (activeSession.timeout) {
                    clearTimeout(activeSession.timeout);
                }
                activeSession.timeout = setTimeout(() => {
                    this.log.debug('Device appears to be inactive. Stopping stream.', this.camera.getDisplayName());
                    this.controller.forceStopStreamingSession(request.sessionID);
                    this.stopStream(request.sessionID);
                }, request.video.rtcp_interval * 2 * 1000);
            });
            activeSession.socket.bind(sessionInfo.videoReturnPort, sessionInfo.localAddress);
        }
        catch (error) {
            this.logThenCallback(callback, error);
            return;
        }
        activeSession.mainProcess = new FfMpegProcess_1.FfmpegProcess(this.camera.getDisplayName(), request.sessionID, ffmpegArgs, nestStream.stdin, this.log, this.platform.debugMode, this, this.platform.ffmpegPath, callback);
        this.ongoingSessions[request.sessionID] = activeSession;
        delete this.pendingSessions[request.sessionID];
        // A RECONFIGURE that raced this (async) START stashed its bitrate; apply it now.
        const pendingBitrate = this.pendingMaxBitrate[request.sessionID];
        if (pendingBitrate) {
            delete this.pendingMaxBitrate[request.sessionID];
            activeSession.streamer.setMaxBitrate(pendingBitrate);
        }
    }
    async handleStreamRequest(request, callback) {
        switch (request.type) {
            case "start" /* START */:
                this.startStream(request, callback);
                break;
            case "reconfigure" /* RECONFIGURE */:
                // Resolution/fps can't change on a stream-copied feed, but the requested
                // bitrate can be re-advertised to the camera via REMB — which in copy
                // mode adapts the HomeKit-facing rate directly. A RECONFIGURE can arrive
                // while START is still initializing (session not yet registered), so
                // stash it and apply on register rather than dropping it.
                this.log.debug(`Received request to reconfigure: ${request.video.width} x ${request.video.height}, ${request.video.fps} fps, ${request.video.max_bit_rate} kbps`, this.camera.getDisplayName());
                if (this.ongoingSessions[request.sessionID]) {
                    this.ongoingSessions[request.sessionID].streamer.setMaxBitrate(request.video.max_bit_rate * 1000);
                }
                else {
                    this.pendingMaxBitrate[request.sessionID] = request.video.max_bit_rate * 1000;
                }
                callback();
                break;
            case "stop" /* STOP */:
                await this.stopStream(request.sessionID);
                callback();
                break;
        }
    }
    async stopStream(sessionId) {
        var _a, _b, _c;
        const session = this.ongoingSessions[sessionId];
        // Detach first so a successor never waits for this bounded teardown and an
        // old stop cannot later delete a newly-published session with the same id.
        delete this.ongoingSessions[sessionId];
        delete this.pendingMaxBitrate[sessionId];
        if (session) {
            if (session.timeout) {
                clearTimeout(session.timeout);
            }
            try {
                (_a = session.socket) === null || _a === void 0 ? void 0 : _a.close();
            }
            catch (err) {
                this.log.error('Error occurred closing socket: ' + err, this.camera.getDisplayName());
            }
            try {
                (_b = session.mainProcess) === null || _b === void 0 ? void 0 : _b.stop();
            }
            catch (err) {
                this.log.error('Error occurred terminating main FFmpeg process: ' + err, this.camera.getDisplayName());
            }
            try {
                (_c = session.returnProcess) === null || _c === void 0 ? void 0 : _c.stop();
            }
            catch (err) {
                this.log.error('Error occurred terminating two-way FFmpeg process: ' + err, this.camera.getDisplayName());
            }
            try {
                let teardownTimer;
                const teardownComplete = Promise.resolve().then(() => session.streamer.teardown()).then(() => false, err => {
                    this.log.error('Error terminating SDM stream: ' + err, this.camera.getDisplayName());
                    return false;
                });
                const teardownTimedOut = await Promise.race([
                    teardownComplete,
                    new Promise(resolve => teardownTimer = setTimeout(() => resolve(true), StreamingDelegate.TEARDOWN_TIMEOUT_MS))
                ]);
                if (teardownTimer)
                    clearTimeout(teardownTimer);
                if (teardownTimedOut)
                    this.log.error('Timed out terminating SDM stream.', this.camera.getDisplayName());
            }
            catch (err) {
                this.log.error('Error initiating SDM stream teardown: ' + err, this.camera.getDisplayName());
            }
        }
        this.log.debug('Stopped video stream.', this.camera.getDisplayName());
    }
    newSession(token, configuration, nestStreamer) {
        let resolvePromise;
        let initReadyResolved = false;
        const initReady = new Promise(resolve => resolvePromise = resolve);
        return {
            token,
            configuration,
            nestStreamer,
            // Assigned by createRecordingSession after initialize() returns. Keeping the
            // Session identity alive before then lets failed initialization use the same
            // cleanup owner (NestStreamer.teardown is guarded for an unset token).
            hksvStreamer: undefined,
            initFragment: null,
            initReady,
            resolveInitReady: () => {
                if (!initReadyResolved) {
                    initReadyResolved = true;
                    resolvePromise();
                }
            },
            ring: [],
            live: [],
            liveBytes: 0,
            adopted: false,
            ending: false,
            ended: false,
            notify: null,
            producer: Promise.resolve(),
            lastProgress: Date.now(),
            // Assigned when the producer starts; cleanup tolerates setup-phase sessions.
            watchdog: undefined,
            acquisitionSettled: false,
            cleaned: false,
        };
    }
    async cleanupSession(s) {
        if (s.cleanupPromise)
            return s.cleanupPromise;
        s.cleanupPromise = (async () => {
            var _a, _b, _c;
            s.cleaned = true;
            s.ending = true;
            s.ended = true;
            s.ring.length = 0;
            s.live.length = 0;
            s.liveBytes = 0;
            if (s.ttl) {
                clearTimeout(s.ttl);
                s.ttl = undefined;
            }
            if (s.watchdog)
                clearInterval(s.watchdog);
            s.resolveInitReady();
            const notify = s.notify;
            s.notify = null;
            notify === null || notify === void 0 ? void 0 : notify();
            (_a = s.hksvStreamer) === null || _a === void 0 ? void 0 : _a.destroy();
            // Relinquish publication/ownership before retiring the remote stream.
            // New acquisition is allowed to overlap this bounded teardown.
            if (this.prewarm === s)
                this.prewarm = undefined;
            if (((_b = this.recordingSessionInfo) === null || _b === void 0 ? void 0 : _b.token) === s.token)
                this.recordingSessionInfo = undefined;
            if (s.acquisitionSettled && ((_c = this.acquiring) === null || _c === void 0 ? void 0 : _c.token) === s.token)
                this.acquiring = undefined;
            let teardownTimer;
            s.teardownComplete = Promise.resolve().then(() => s.nestStreamer.teardown()).then(() => undefined, error => {
                this.log.error('Error tearing down recording SDM stream: ' + error, this.camera.getDisplayName());
            });
            const teardownTimedOut = await Promise.race([
                s.teardownComplete.then(() => false),
                new Promise(resolve => teardownTimer = setTimeout(() => resolve(true), StreamingDelegate.TEARDOWN_TIMEOUT_MS))
            ]);
            if (teardownTimer)
                clearTimeout(teardownTimer);
            if (teardownTimedOut)
                this.log.error('Timed out tearing down recording SDM stream.', this.camera.getDisplayName());
        })();
        return s.cleanupPromise;
    }
    startSessionWatchdog(s) {
        s.lastProgress = Date.now();
        s.watchdog = setInterval(() => {
            if (!s.cleaned && Date.now() - s.lastProgress > StreamingDelegate.IDLE_MS) {
                this.log.error('Recording stream stalled; releasing the session.', this.camera.getDisplayName());
                void this.cleanupSession(s);
            }
        }, StreamingDelegate.WATCHDOG_INTERVAL_MS);
    }
    startSessionProducer(s) {
        this.startSessionWatchdog(s);
        s.producer = this.runPrewarmProducer(s);
    }
    async runPrewarmProducer(s) {
        const pending = [];
        try {
            // This is the only generator puller for the Session. Adoption changes only
            // the destination queue; it never starts a second generator.
            for await (const box of s.hksvStreamer.generator()) {
                if (s.cleaned)
                    break;
                pending.push(box.header, box.data);
                this.log.debug("mp4 box type " + box.type + " and length " + box.length);
                if (box.type !== 'moov' && box.type !== 'mdat')
                    continue;
                const fragment = Buffer.concat(pending);
                pending.length = 0;
                s.lastProgress = Date.now();
                if (s.initFragment === null && box.type === 'moov') {
                    s.initFragment = fragment;
                    s.resolveInitReady();
                    continue;
                }
                if (box.type !== 'mdat')
                    continue;
                if (!s.adopted) {
                    s.ring.push(fragment);
                    let ringBytes = s.ring.reduce((total, item) => total + item.length, 0);
                    while (s.ring.length > StreamingDelegate.PREBUFFER_MAX_FRAGMENTS
                        || ringBytes > StreamingDelegate.PREBUFFER_MAX_BYTES) {
                        const dropped = s.ring.shift();
                        if (dropped)
                            ringBytes -= dropped.length;
                    }
                    if (s.ending)
                        break;
                    continue;
                }
                if (s.live.length >= StreamingDelegate.LIVE_MAX_FRAGMENTS
                    || s.liveBytes + fragment.length > StreamingDelegate.LIVE_MAX_BYTES) {
                    this.log.error('Recording consumer stopped draining; releasing the session.', this.camera.getDisplayName());
                    await this.cleanupSession(s);
                    return;
                }
                s.live.push(fragment);
                s.liveBytes += fragment.length;
                const notify = s.notify;
                s.notify = null;
                notify === null || notify === void 0 ? void 0 : notify();
                if (s.ending)
                    break;
            }
        }
        catch (error) {
            if (!s.cleaned)
                this.log.error("Encountered unexpected error on recording producer " + (error.stack || error));
        }
        finally {
            s.ended = true;
            s.resolveInitReady();
            const notify = s.notify;
            s.notify = null;
            notify === null || notify === void 0 ? void 0 : notify();
            if (!s.adopted)
                await this.cleanupSession(s);
        }
    }
    armPrewarmTtl(s) {
        if (s.cleaned)
            return;
        if (s.ttl)
            clearTimeout(s.ttl);
        s.ttl = setTimeout(() => {
            this.log.debug("Idle pre-warm timed out; released the stream.", this.camera.getDisplayName());
            void this.cleanupSession(s);
        }, StreamingDelegate.PREWARM_TTL_MS);
    }
    motionDetected() {
        var _a;
        return Boolean((_a = this.accessory.getService(this.hap.Service.MotionSensor)) === null || _a === void 0 ? void 0 : _a.getCharacteristic(this.platform.Characteristic.MotionDetected).value);
    }
    beginAcquisition(kind, replacingToken) {
        if (this.shuttingDown)
            throw new Error('Homebridge is shutting down.');
        if (this.acquiring)
            throw new Error('A recording stream is already being acquired.');
        if (this.recordingSessionInfo && this.recordingSessionInfo.token !== replacingToken)
            throw new Error('A recording stream is already active.');
        const configuration = this.cameraRecordingConfiguration;
        if (!configuration)
            throw new Error('No recording configuration for this camera.');
        let resolveDone;
        const done = new Promise(resolve => resolveDone = resolve);
        const acquisition = {
            token: this.nextSessionToken++,
            cancel: false,
            configuration,
            kind,
            done,
            resolveDone,
        };
        this.acquiring = acquisition;
        this.inFlightAcquisitions.add(acquisition);
        return acquisition;
    }
    clearAcquisition(token) {
        var _a;
        if (((_a = this.acquiring) === null || _a === void 0 ? void 0 : _a.token) === token)
            this.acquiring = undefined;
    }
    settleAcquisition(acquisition) {
        this.inFlightAcquisitions.delete(acquisition);
        acquisition.resolveDone();
    }
    currentAcquisition() {
        return this.acquiring;
    }
    cancelAcquisition() {
        if (this.acquiring) {
            const acquisition = this.acquiring;
            acquisition.cancel = true;
            this.clearAcquisition(acquisition.token);
            if (acquisition.session)
                void this.cleanupSession(acquisition.session);
        }
    }
    cancelPrewarmAcquisition() {
        var _a;
        if (((_a = this.acquiring) === null || _a === void 0 ? void 0 : _a.kind) === 'prewarm') {
            const acquisition = this.acquiring;
            acquisition.cancel = true;
            this.clearAcquisition(acquisition.token);
            if (acquisition.session)
                void this.cleanupSession(acquisition.session);
        }
    }
    /**
     * FFmpeg video args for the HKSV recording path. Always transcodes, and always pins the
     * output geometry -- see the notes inside for why copy was tried and withdrawn.
     *
     * Background: #238 (ajplotkin, issue #235) established that nothing hub-side validates
     * delivered media against the negotiation -- hap-nodejs's RecordingManagement chunks each
     * fragment and ships it without parsing moof/mdat/SPS -- and proposed copying the camera's
     * H.264 instead of re-encoding it, which roughly halves the CPU cost of a recording. That
     * reasoning is sound, and copy is correct for any camera holding a single resolution.
     *
     * It is not used here because newer Nest cameras change resolution mid-stream. See below.
     */
    recordingVideoArgs(configuration) {
        // No "-an" in here: HksvStreamer pushes audioOutputArgs BEFORE videoOutputArgs, so an
        // unconditional -an at the head of videoArgs silently overrides the AAC-ELD block and
        // records every clip mute. Audio-off is expressed from audioArgs instead.
        // NOTE: this used to return ["-sn","-dn","-codec:v","copy"] for WebRtcNestStreamer, which
        // halves the CPU cost of a recording and is correct for any camera that holds one
        // resolution. It is deliberately not done here.
        //
        // "-codec:v copy" passes frames through at whatever geometry they arrive with; it cannot
        // rescale, by definition. Newer Nest cameras re-negotiate resolution mid-stream as the link
        // changes, and a fragmented-MP4 track declares its dimensions ONCE in the init segment and
        // can never re-declare them. So a copied recording from such a camera produces a track whose
        // declared size stops matching its samples partway through: players freeze the video and
        // never recover, while audio is unaffected. Measured and confirmed on real hardware -- see
        // the scale filter below.
        //
        // Transcoding unconditionally is the safe choice: it costs CPU on every recording, including
        // for the cameras that would have been fine, but it works on all of them. Restoring copy
        // would require detecting a resolution change and falling back mid-recording, which is the
        // better answer and considerably more work.
        const profile = configuration.videoCodec.parameters.profile === 2 /* HIGH */ ? "high"
            : configuration.videoCodec.parameters.profile === 1 /* MAIN */ ? "main" : "baseline";
        const level = configuration.videoCodec.parameters.level === 2 /* LEVEL4_0 */ ? "4.0"
            : configuration.videoCodec.parameters.level === 1 /* LEVEL3_2 */ ? "3.2" : "3.1";
        return [
            "-sn",
            "-dn",
            "-codec:v",
            "libx264",
            // Placed before the profile/level/bitrate args below so those explicit settings still
            // override the preset/tune defaults. zerolatency disables the frame lookahead and
            // B-frame reordering that otherwise buffer several frames before the first fragment --
            // at low Nest frame rates that buffering is a large chunk of recording-start latency.
            // libx264's default is the slower "medium" preset with a ~40-frame lookahead; the
            // live-view path already uses these same two flags.
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            // Pin the output geometry. A fragmented-MP4 track writes its dimensions ONCE, into the
            // init segment, and they cannot change afterwards -- so any mid-stream resolution change
            // produces a track whose declared size stops matching its samples, and players freeze on
            // the video while audio (which has no geometry) keeps running. It never recovers, because
            // the track cannot be re-declared.
            //
            // Nest cameras re-negotiate resolution adaptively when the link degrades. Measured on this
            // deployment: one camera emitted 640x368 (31 times) and 1920x1088 (9 times) within the same
            // sessions, while every other camera held a single resolution for its lifetime. That one
            // camera froze ~2s into every clip and never recovered; the others were fine.
            //
            // This path has never scaled, which is why it went unnoticed for years -- a camera with a
            // stable resolution never triggers it. force_original_aspect_ratio + pad rather than a bare
            // scale, because the two resolutions above are not the same aspect ratio (1.76 vs 1.74) and
            // stretching would be visible.
            "-vf", `scale=${configuration.videoCodec.resolution[0]}:${configuration.videoCodec.resolution[1]}:force_original_aspect_ratio=decrease,pad=${configuration.videoCodec.resolution[0]}:${configuration.videoCodec.resolution[1]}:(ow-iw)/2:(oh-ih)/2,setsar=1`,
            "-pix_fmt",
            "yuv420p",
            "-profile:v", profile,
            "-level:v", level,
            "-b:v", `${configuration.videoCodec.parameters.bitRate}k`,
            "-force_key_frames", `expr:eq(t,n_forced*${configuration.videoCodec.parameters.iFrameInterval / 1000})`,
            "-r", configuration.videoCodec.resolution[2].toString(),
        ];
    }
    async createRecordingSession(acquisition) {
        var _a, _b, _c;
        const configuration = acquisition.configuration;
        try {
            if (configuration.videoCodec.type !== 0 /* H264 */)
                throw new Error('Unsupported recording codec type.');
            let samplerate;
            switch (configuration.audioCodec.samplerate) {
                case 0 /* KHZ_8 */:
                    samplerate = "8";
                    break;
                case 1 /* KHZ_16 */:
                    samplerate = "16";
                    break;
                case 2 /* KHZ_24 */:
                    samplerate = "24";
                    break;
                case 3 /* KHZ_32 */:
                    samplerate = "32";
                    break;
                case 4 /* KHZ_44_1 */:
                    samplerate = "44.1";
                    break;
                case 5 /* KHZ_48 */:
                    samplerate = "48";
                    break;
                default:
                    throw new Error("Unsupported audio sample rate: " + configuration.audioCodec.samplerate);
            }
            // .value, not the Characteristic object: getCharacteristic() returns the object, which is
            // always truthy, so this branch was taken regardless of the Home app's "Record Audio"
            // setting. That was moot while videoArgs led with an unconditional "-an" (HksvStreamer
            // pushes audioOutputArgs BEFORE videoOutputArgs, so the -an overrode this whole block and
            // every clip recorded mute), which is likely why it went unnoticed. Both halves are fixed
            // together: the -an is gone from videoArgs, and audio-off is expressed from the else-branch
            // here where it can actually be conditional.
            const audioArgs = ((_c = (_b = (_a = this.controller) === null || _a === void 0 ? void 0 : _a.recordingManagement) === null || _b === void 0 ? void 0 : _b.recordingManagementService.getCharacteristic(this.platform.Characteristic.RecordingAudioActive)) === null || _c === void 0 ? void 0 : _c.value)
                ? [
                    "-acodec", "libfdk_aac",
                    ...(configuration.audioCodec.type === 0 /* AAC_LC */
                        ? ["-profile:a", "aac_low"]
                        : ["-profile:a", "aac_eld"]),
                    "-ar", `${samplerate}k`,
                    "-b:a", `${configuration.audioCodec.bitrate}k`,
                    "-ac", `${configuration.audioCodec.audioChannels}`,
                ]
                : ["-an"];
            let s;
            let timeout;
            let operationSettled = false;
            const operation = (async () => {
                var _a, _b, _c;
                try {
                    const nestStreamer = await (0, NestStreamer_1.getStreamer)(this.log, this.camera, this.config);
                    // Built here rather than above: the copy-vs-transcode decision depends on which
                    // streamer was actually constructed.
                    const videoArgs = this.recordingVideoArgs(configuration);
                    s = this.newSession(acquisition.token, configuration, nestStreamer);
                    acquisition.session = s;
                    if (acquisition.cancel || ((_a = this.acquiring) === null || _a === void 0 ? void 0 : _a.token) !== acquisition.token)
                        throw new Error('Recording acquisition cancelled.');
                    const nestStream = await nestStreamer.initialize();
                    if (acquisition.cancel || s.cleaned || ((_b = this.acquiring) === null || _b === void 0 ? void 0 : _b.token) !== acquisition.token)
                        throw new Error('Recording acquisition cancelled.');
                    s.hksvStreamer = new HksvStreamer_1.default(this.log, nestStream, audioArgs, videoArgs, this.platform.debugMode, this.platform.ffmpegPath, this.snapshotOutputArgs());
                    await s.hksvStreamer.start();
                    if (acquisition.cancel || s.cleaned || s.hksvStreamer.destroyed
                        || ((_c = this.acquiring) === null || _c === void 0 ? void 0 : _c.token) !== acquisition.token)
                        throw new Error('Recording acquisition cancelled.');
                    return s;
                }
                catch (error) {
                    if (s)
                        void this.cleanupSession(s);
                    else
                        this.clearAcquisition(acquisition.token);
                    throw error;
                }
                finally {
                    operationSettled = true;
                    if (s) {
                        s.acquisitionSettled = true;
                        if (s.cleaned)
                            this.clearAcquisition(s.token);
                    }
                    else {
                        this.clearAcquisition(acquisition.token);
                    }
                }
            })();
            try {
                return await Promise.race([
                    operation,
                    new Promise((_, reject) => {
                        timeout = setTimeout(() => {
                            acquisition.cancel = true;
                            if (s)
                                void this.cleanupSession(s);
                            reject(new Error(`Recording acquisition timed out after ${StreamingDelegate.ACQUIRE_TIMEOUT_MS}ms.`));
                        }, StreamingDelegate.ACQUIRE_TIMEOUT_MS);
                    })
                ]);
            }
            catch (error) {
                acquisition.cancel = true;
                if (s)
                    void this.cleanupSession(s);
                else if (operationSettled)
                    this.clearAcquisition(acquisition.token);
                throw error;
            }
            finally {
                if (timeout)
                    clearTimeout(timeout);
            }
        }
        finally {
            this.settleAcquisition(acquisition);
        }
    }
    /**
     * Pre-warm, not pre-buffer. This runs when a motion event is DELIVERED, so the ring it
     * fills starts at delivery and by construction cannot hold a frame from before the event.
     * It hides the SDM dial and FFmpeg connect/keyframe time (~1s), not Pub/Sub delivery
     * latency and not the lag between Google's own footage and the timestamp it publishes.
     * Real pre-trigger footage needs a continuously running source; see prebufferLength in
     * getController() and issue #233.
     */
    async notifyMotion() {
        var _a;
        if (this.shuttingDown
            || this.config.motionPrewarm === false
            || !this.recordingActive
            || !this.cameraRecordingConfiguration
            || this.acquiring
            || this.recordingSessionInfo
            || this.prewarmSetup
            || ((_a = this.prewarm) === null || _a === void 0 ? void 0 : _a.adopted))
            return;
        if (this.prewarm) {
            const previousPrewarm = this.prewarm;
            if (!previousPrewarm.cleaned
                && previousPrewarm.configuration === this.cameraRecordingConfiguration) {
                if (!previousPrewarm.adopted)
                    this.armPrewarmTtl(previousPrewarm);
                return;
            }
            this.prewarm = undefined;
            void this.cleanupSession(previousPrewarm);
            if (this.acquiring || this.prewarmSetup || this.recordingSessionInfo
                || !this.recordingActive || !this.cameraRecordingConfiguration)
                return;
        }
        const acquisition = this.beginAcquisition('prewarm');
        const setup = (async () => {
            var _a;
            let s;
            try {
                this.log.debug("Pre-warming recording stream on motion", this.camera.getDisplayName());
                s = await this.createRecordingSession(acquisition);
                if (acquisition.cancel
                    || ((_a = this.acquiring) === null || _a === void 0 ? void 0 : _a.token) !== s.token
                    || !this.recordingActive
                    || this.cameraRecordingConfiguration !== s.configuration
                    || this.recordingSessionInfo) {
                    void this.cleanupSession(s);
                    return;
                }
                this.prewarm = s;
                this.clearAcquisition(s.token);
                this.armPrewarmTtl(s);
                this.startSessionProducer(s);
            }
            catch (error) {
                if (s)
                    void this.cleanupSession(s);
                else
                    this.clearAcquisition(acquisition.token);
                this.log.error("Unable to pre-warm recording stream: " + (error.stack || error), this.camera.getDisplayName());
            }
        })();
        this.prewarmSetup = setup;
        try {
            await setup;
        }
        finally {
            if (this.prewarmSetup === setup)
                this.prewarmSetup = undefined;
        }
    }
    async waitForSessionData(s) {
        if (s.live.length || s.ended)
            return;
        await new Promise(resolve => {
            let settled = false;
            let timer;
            const wake = () => {
                if (settled)
                    return;
                settled = true;
                clearTimeout(timer);
                if (s.notify === wake)
                    s.notify = null;
                resolve();
            };
            timer = setTimeout(wake, 1000);
            s.notify = wake;
            if (s.live.length || s.ended)
                wake();
        });
    }
    async *consumeSession(s, endOnMotionStop) {
        await s.initReady;
        if (!s.initFragment || s.cleaned)
            return;
        yield { data: s.initFragment, isLast: false };
        let heldMedia;
        let loggedMotionEnd = false;
        while (!s.cleaned) {
            if (endOnMotionStop && !this.motionDetected() && !s.ending) {
                // The producer may already be assembling an mdat. It observes ending
                // after appending that current fragment, then sets ended in its finally.
                s.ending = true;
                loggedMotionEnd = true;
            }
            let fragment;
            if (s.ring.length) {
                fragment = s.ring.shift();
            }
            else if (s.live.length) {
                fragment = s.live.shift();
                if (fragment)
                    s.liveBytes -= fragment.length;
            }
            if (fragment) {
                if (heldMedia) {
                    if (s.cleaned)
                        return;
                    yield { data: heldMedia, isLast: false };
                }
                heldMedia = fragment;
                continue;
            }
            if (s.ended)
                break;
            await this.waitForSessionData(s);
        }
        if (s.cleaned)
            return;
        if (heldMedia)
            yield { data: heldMedia, isLast: true };
        if (loggedMotionEnd)
            this.log.debug("Ending session after draining the motion backlog.", this.camera.getDisplayName());
    }
    async *consumeColdSession(s, endOnMotionStop) {
        const pending = [];
        let heldMedia;
        let producerError;
        try {
            // The cold path deliberately keeps its original direct loop. It pulls the
            // local Session streamer exactly once and never dereferences mutable
            // recordingSessionInfo to choose a generator.
            for await (const box of s.hksvStreamer.generator()) {
                if (s.cleaned)
                    return;
                pending.push(box.header, box.data);
                this.log.debug("mp4 box type " + box.type + " and length " + box.length);
                if (box.type !== 'moov' && box.type !== 'mdat')
                    continue;
                const fragment = Buffer.concat(pending);
                pending.length = 0;
                s.lastProgress = Date.now();
                if (s.initFragment === null && box.type === 'moov') {
                    s.initFragment = fragment;
                    s.resolveInitReady();
                    yield { data: fragment, isLast: false };
                    continue;
                }
                if (box.type !== 'mdat')
                    continue;
                if (heldMedia) {
                    if (s.cleaned)
                        return;
                    yield { data: heldMedia, isLast: false };
                }
                heldMedia = fragment;
                if (endOnMotionStop && !this.motionDetected()) {
                    s.ending = true;
                    this.log.debug("Ending session due to motion stopped!", this.camera.getDisplayName());
                    break;
                }
            }
        }
        catch (error) {
            producerError = error;
        }
        finally {
            s.ended = true;
            s.resolveInitReady();
        }
        if (s.cleaned)
            return;
        // A one-fragment delay lets both motion termination and producer EOF mark
        // the final media fragment, never the initialization (moov) fragment.
        if (heldMedia)
            yield { data: heldMedia, isLast: true };
        if (producerError)
            this.log.error("Encountered unexpected error on cold recording producer "
                + (producerError.stack || producerError), this.camera.getDisplayName());
    }
    closeRecordingStream(streamId, reason) {
        var _a;
        // CameraRecordingDelegate's close hook is synchronous. Cancellation
        // prevents an in-flight acquisition from publishing; its own bounded
        // cleanup continues independently.
        this.cancelAcquisition();
        const info = this.recordingSessionInfo;
        if (!info)
            return;
        if (info.streamId !== streamId) {
            this.log.debug(`Ignoring recording close for a stale/replaced session id ${streamId}.`, this.camera.getDisplayName());
            return;
        }
        // Capture and compare the token, not just the stream id. If the session was
        // replaced between lookup and close, this stale close is a no-op.
        if (((_a = this.recordingSessionInfo) === null || _a === void 0 ? void 0 : _a.token) !== info.token)
            return;
        void this.cleanupSession(info.session);
    }
    acknowledgeStream(streamId) {
        this.closeRecordingStream(streamId, undefined);
    }
    async *handleRecordingStreamRequest(streamId) {
        var _a, _b, _c, _d, _e;
        this.log.debug('Recording request received.');
        if (this.recordingSessionInfo || ((_a = this.prewarm) === null || _a === void 0 ? void 0 : _a.adopted)
            || ((_b = this.acquiring) === null || _b === void 0 ? void 0 : _b.kind) === 'recording') {
            this.log.error('Ignoring overlapping recording request while another session is active.', this.camera.getDisplayName());
            return;
        }
        const endOnMotionStop = (_c = this.config.endRecordingOnMotionStop) !== null && _c !== void 0 ? _c : true;
        let s;
        let adoptedPrewarm = false;
        let acquisition;
        try {
            // If a pre-warm is in flight (or already ready), WAIT for it and adopt it —
            // adopting the warm stream is the entire point of pre-warming. A recording
            // produces its first frame at init+keyframe time whether cold or warm, so
            // waiting for the in-flight pre-warm (started ~1s earlier on this same motion
            // event) is no slower than a cold start, and it captures the head-start
            // pre-roll. Bounded by ACQUIRE_TIMEOUT so a stalled setup can't hang the
            // request; on timeout we fall through to cold and the pre-warm retains
            // cleanup ownership of its own late stream.
            if (this.prewarmSetup && !this.prewarm) {
                await Promise.race([
                    this.prewarmSetup.catch(() => { }),
                    new Promise(resolve => setTimeout(resolve, StreamingDelegate.ACQUIRE_TIMEOUT_MS)),
                ]);
            }
            if (this.recordingSessionInfo || ((_d = this.prewarm) === null || _d === void 0 ? void 0 : _d.adopted)) {
                this.log.error('Ignoring overlapping recording request while another session is active.', this.camera.getDisplayName());
                return;
            }
            if (!this.recordingActive)
                throw new Error('Recording is inactive.');
            let prewarm = this.prewarm;
            if (prewarm && prewarm.configuration !== this.cameraRecordingConfiguration) {
                this.prewarm = undefined;
                void this.cleanupSession(prewarm);
                prewarm = undefined;
            }
            if (prewarm) {
                // Claim before awaiting init: no TTL or second request can race adoption.
                s = prewarm;
                this.prewarm = undefined;
                if (s.ttl) {
                    clearTimeout(s.ttl);
                    s.ttl = undefined;
                }
                s.adopted = true;
                adoptedPrewarm = true;
                this.recordingSessionInfo = {
                    streamId,
                    token: s.token,
                    session: s,
                };
                await s.initReady;
                if (s.cleaned
                    || this.cameraRecordingConfiguration !== s.configuration
                    || !s.initFragment) {
                    // A pre-warm can be cleaned by its watchdog while adoption is awaiting
                    // initReady. Treat that exactly like any other stale/unusable pre-warm:
                    // reserve the replacement token and retry cold instead of losing the
                    // HomeKit recording request.
                    const fallbackAcquisition = this.beginAcquisition('recording', s.token);
                    acquisition = fallbackAcquisition;
                    if (((_e = this.recordingSessionInfo) === null || _e === void 0 ? void 0 : _e.token) === s.token)
                        this.recordingSessionInfo = undefined;
                    void this.cleanupSession(s);
                    s = undefined;
                    if (fallbackAcquisition.cancel || !this.recordingActive) {
                        // settleAcquisition (not just clearAcquisition): resolve the acquisition's `done`
                        // promise and drop it from inFlightAcquisitions, or shutdown's await on `done` hangs.
                        this.settleAcquisition(fallbackAcquisition);
                        throw new Error('Recording acquisition cancelled.');
                    }
                    s = await this.createRecordingSession(fallbackAcquisition);
                    adoptedPrewarm = false;
                }
            }
            if (!s) {
                acquisition = this.beginAcquisition('recording');
                s = await this.createRecordingSession(acquisition);
            }
            if (!adoptedPrewarm) {
                while (true) {
                    const owner = this.currentAcquisition();
                    if (!owner || owner.token !== s.token || owner.cancel
                        || !this.recordingActive || s.cleaned) {
                        void this.cleanupSession(s);
                        throw new Error('Recording acquisition cancelled before publication.');
                    }
                    if (this.cameraRecordingConfiguration === s.configuration)
                        break;
                    // Configuration changed while the stream was being built. Detach it
                    // immediately and retry cold with the now-current configuration.
                    owner.cancel = true;
                    this.clearAcquisition(owner.token);
                    void this.cleanupSession(s);
                    if (!this.recordingActive)
                        throw new Error('Recording acquisition cancelled.');
                    acquisition = this.beginAcquisition('recording');
                    s = await this.createRecordingSession(acquisition);
                }
                s.adopted = true;
                this.recordingSessionInfo = {
                    streamId,
                    token: s.token,
                    session: s,
                };
                this.clearAcquisition(s.token);
                this.startSessionWatchdog(s);
            }
            if (adoptedPrewarm)
                yield* this.consumeSession(s, endOnMotionStop);
            else
                yield* this.consumeColdSession(s, endOnMotionStop);
        }
        catch (error) {
            this.log.error("Encountered unexpected error on recording generator " + (error.stack || error));
            throw error;
        }
        finally {
            if (s)
                await this.cleanupSession(s);
            if (acquisition)
                this.clearAcquisition(acquisition.token);
        }
    }
    updateRecordingActive(active) {
        var _a;
        this.recordingActive = active;
        this.log.debug("Recording active set to " + active);
        if (!active) {
            if (this.acquiring)
                this.acquiring.cancel = true;
            const prewarm = this.prewarm;
            const recording = (_a = this.recordingSessionInfo) === null || _a === void 0 ? void 0 : _a.session;
            if (prewarm)
                void this.cleanupSession(prewarm);
            if (recording && recording.token !== (prewarm === null || prewarm === void 0 ? void 0 : prewarm.token))
                void this.cleanupSession(recording);
        }
    }
    updateRecordingConfiguration(configuration) {
        this.cameraRecordingConfiguration = configuration;
        if (this.prewarm && !this.prewarm.adopted)
            void this.cleanupSession(this.prewarm);
    }
}
exports.StreamingDelegate = StreamingDelegate;
StreamingDelegate.PREBUFFER_MAX_FRAGMENTS = 8;
StreamingDelegate.PREBUFFER_MAX_BYTES = 3 * 1024 * 1024;
StreamingDelegate.LIVE_MAX_FRAGMENTS = 256;
StreamingDelegate.LIVE_MAX_BYTES = 24 * 1024 * 1024;
StreamingDelegate.PREWARM_TTL_MS = 20000;
StreamingDelegate.WATCHDOG_INTERVAL_MS = 2000;
StreamingDelegate.IDLE_MS = 15000;
StreamingDelegate.ACQUIRE_TIMEOUT_MS = 8000;
StreamingDelegate.TEARDOWN_TIMEOUT_MS = 3000;
// A stream-written snapshot older than this is treated as absent: a day-old
// "last seen" frame is still useful, but an ancient one masquerades as current
// and shadows the fresher event-image path in camera.getSnapshot().
StreamingDelegate.SNAPSHOT_MAX_AGE_MS = 24 * 60 * 60 * 1000;
//# sourceMappingURL=StreamingDelegate.js.map