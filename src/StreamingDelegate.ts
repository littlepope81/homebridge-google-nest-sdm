import {
  API,
  APIEvent, AudioBitrate, AudioRecordingCodecType, AudioRecordingSamplerate,
  AudioStreamingCodecType,
  AudioStreamingSamplerate,
  CameraController,
  CameraControllerOptions, CameraRecordingConfiguration, CameraRecordingDelegate,
  CameraStreamingDelegate, H264Level, H264Profile,
  HAP, HDSProtocolSpecificErrorReason,
  Logger, MediaContainerType, PlatformAccessory,
  PrepareStreamCallback,
  PrepareStreamRequest,
  PrepareStreamResponse, RecordingPacket, ResourceRequestReason, SnapshotRequest,
  SnapshotRequestCallback,
  SRTPCryptoSuites,
  StartStreamRequest,
  StreamingRequest,
  StreamRequestCallback,
  StreamRequestTypes, VideoInfo
} from 'homebridge';
import { VideoCodecType } from 'hap-nodejs'
import {createSocket, Socket} from 'dgram';
import * as fs from 'fs';
import * as path from 'path';
import os from 'os';
import {networkInterfaceDefault} from 'systeminformation';
import {Config} from './Config'
import {FfmpegProcess} from './FfMpegProcess';
import {Camera} from "./sdm/Camera";
import {getStreamer, NestStream, NestStreamer} from "./NestStreamer";
import {Platform} from "./Platform";
import HksvStreamer from "./HksvStreamer";
import pickPort, { pickPortOptions } from 'pick-port';

type SessionInfo = {
  address: string; // address of the HAP controller
  localAddress: string;
  ipv6: boolean;

  videoPort: number;
  videoReturnPort: number;
  videoCryptoSuite: SRTPCryptoSuites; // should be saved if multiple suites are supported
  videoSRTP: Buffer; // key and salt concatenated
  videoSSRC: number; // rtp synchronisation source

  audioPort: number;
  audioReturnPort: number;
  audioCryptoSuite: SRTPCryptoSuites;
  audioSRTP: Buffer;
  audioSSRC: number;
};

type ActiveSession = {
  mainProcess?: FfmpegProcess;
  returnProcess?: FfmpegProcess;
  timeout?: NodeJS.Timeout;
  socket?: Socket;
  streamer: NestStreamer;
};

type ResolutionInfo = {
  width: number;
  height: number;
  videoFilter: string;
};

type RecordingSessionInfo = {
  streamId: number,
  token: number,
  session: Session
}

type RecordingAcquisition = {
  token: number;
  cancel: boolean;
}

type Session = {
  token: number;
  nestStreamer: NestStreamer;
  hksvStreamer: HksvStreamer;
  initFragment: Buffer | null;
  initReady: Promise<void>;
  resolveInitReady: () => void;
  ring: Buffer[];
  live: Buffer[];
  liveBytes: number;
  adopted: boolean;
  ending: boolean;
  ended: boolean;
  notify: (() => void) | null;
  producer: Promise<void>;
  lastProgress: number;
  watchdog: ReturnType<typeof setInterval>;
  ttl?: ReturnType<typeof setTimeout>;
  acquisitionSettled: boolean;
  cleaned: boolean;
  teardownComplete?: Promise<void>;
  cleanupPromise?: Promise<void>;
}

export abstract class StreamingDelegate<T extends CameraController> implements CameraStreamingDelegate, CameraRecordingDelegate {
  private static readonly PREBUFFER_MAX_FRAGMENTS = 8;
  private static readonly PREBUFFER_MAX_BYTES = 3 * 1024 * 1024;
  private static readonly LIVE_MAX_FRAGMENTS = 256;
  private static readonly LIVE_MAX_BYTES = 24 * 1024 * 1024;
  private static readonly PREWARM_TTL_MS = 20000;
  private static readonly WATCHDOG_INTERVAL_MS = 2000;
  private static readonly IDLE_MS = 15000;
  private static readonly PREWARM_SETUP_NOTICE_MS = 3000;
  private static readonly ACQUIRE_TIMEOUT_MS = 8000;
  private static readonly TEARDOWN_TIMEOUT_MS = 3000;

  protected hap: HAP;
  protected log: Logger;

  // keep track of sessions
  protected pendingSessions: Record<string, SessionInfo> = {};
  protected ongoingSessions: Record<string, ActiveSession> = {};
  // Bitrate from a RECONFIGURE that arrived before its START finished setting up
  // the session; applied once the session registers.
  private pendingMaxBitrate: Record<string, number> = {};
  protected config: Config;
  protected accessory: PlatformAccessory;
  protected camera: Camera;
  protected platform: Platform;
  protected options: CameraControllerOptions;
  protected controller!: T;

  // minimal secure video properties.
  protected cameraRecordingConfiguration?: CameraRecordingConfiguration;
  protected recordingSessionInfo?: RecordingSessionInfo;
  private recordingActive = false;
  private prewarm?: Session;
  private prewarmSetup?: Promise<void>;
  private acquiring?: RecordingAcquisition;
  private nextSessionToken = 1;

  constructor(log: Logger, api: API, platform: Platform, camera: Camera, accessory: PlatformAccessory) {
    this.platform = platform;
    this.log = log;
    this.hap = api.hap;
    this.config = platform.platformConfig as unknown as Config
    this.camera = camera;
    this.accessory = accessory;

    api.on(APIEvent.SHUTDOWN, async () => {
      if (this.acquiring)
        this.acquiring.cancel = true;

      for (const session in this.ongoingSessions) {
        await this.stopStream(session);
      }

      try {
        await this.prewarmSetup;
      } catch {
        // notifyMotion logs setup failures.
      }

      const recording = this.recordingSessionInfo?.session;
      const prewarm = this.prewarm;
      if (prewarm)
        await this.cleanupSession(prewarm);
      if (recording && recording.token !== prewarm?.token)
        await this.cleanupSession(recording);
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
      ...(motionService ? {sensors: {motion: motionService}} : {}),
      streamingOptions: {
        supportedCryptoSuites: [this.hap.SRTPCryptoSuites.AES_CM_128_HMAC_SHA1_80],
        video: {
          resolutions: camera.getResolutions(),
          codec: {
            profiles: [this.hap.H264Profile.MAIN],
            levels: [this.hap.H264Level.LEVEL3_1]
          }
        },
        audio: {
          twoWayAudio: false,
          codecs: [
            {
              type: AudioStreamingCodecType.AAC_ELD,
              samplerate: AudioStreamingSamplerate.KHZ_16,
              audioChannels: 1
            }
          ]
        }
      },
      recording: {
        delegate: this,
        options: {
          prebufferLength: 4000,
          mediaContainerConfiguration: {
            type: MediaContainerType.FRAGMENTED_MP4,
            fragmentLength: 4000,
          },
          video: {
            type: VideoCodecType.H264,
            parameters: {
              profiles: [H264Profile.HIGH],
              levels: [H264Level.LEVEL4_0],
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
              type: AudioRecordingCodecType.AAC_ELD,
              audioChannels: 1,
              samplerate: AudioRecordingSamplerate.KHZ_48,
              bitrateMode: AudioBitrate.VARIABLE,
            },
          },
        }
      }
    };
  }

  abstract getController(): T;

  /**
   * Path of the periodically-refreshed JPEG that live and HKSV streams write
   * for this camera (see the snapshot output appended to the FFmpeg commands).
   */
  private snapshotFilePath(): string {
    return path.join(this.platform.snapshotDir, this.accessory.UUID + '.jpg');
  }

  /**
   * FFmpeg output group that decodes the (otherwise stream-copied) video at a
   * low rate and keeps a single JPEG updated on disk, giving HomeKit tiles a
   * real "last seen" frame — SDM offers no snapshot API, so without this the
   * tiles only ever show a static placeholder logo.
   */
  private snapshotOutputArgs(): Array<string> {
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

  // A stream-written snapshot older than this is treated as absent: a day-old
  // "last seen" frame is still useful, but an ancient one masquerades as current
  // and shadows the fresher event-image path in camera.getSnapshot().
  private static readonly SNAPSHOT_MAX_AGE_MS = 24 * 60 * 60 * 1000;

  handleSnapshotRequest(request: SnapshotRequest, callback: SnapshotRequestCallback): void {
    this.log.debug(`Snapshot requested (reason: ${request.reason === undefined ? 'unspecified' : request.reason === ResourceRequestReason.PERIODIC ? 'periodic' : 'event'})`, this.camera.getDisplayName());

    if (request.reason === ResourceRequestReason.EVENT) {
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
          } else {
            throw new Error('incomplete snapshot file');
          }
        })
        .catch(() => this.camera.getSnapshot()
            .then(result => callback(undefined, result))
            .catch(error => callback(error)));
  }

  private static determineResolution(request: VideoInfo): ResolutionInfo {
    let width = request.width;
    let height = request.height;

    const filters: Array<string> = [];
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

  async getIpAddress(ipv6: boolean): Promise<string> {

    const interfaceName = await networkInterfaceDefault();
    const interfaces = os.networkInterfaces();
    // @ts-ignore
    const externalInfo = interfaces[interfaceName]?.filter((info: { internal: any; }) => {
      return !info.internal;
    });
    const preferredFamily = ipv6 ? 'IPv6' : 'IPv4';
    const addressInfo = externalInfo?.find((info: { family: string; }) => {
      return info.family === preferredFamily;
    }) || externalInfo?.[0];
    if (!addressInfo) {
      throw new Error('Unable to get network address for "' + interfaceName + '"!');
    }
    return addressInfo.address;
  }

  /**
   * Some callback methods do not log anything if they are called with an error.
   */
  logThenCallback(callback: (error?: Error) => void, message: string) {
    this.log.error(message);
    callback(new Error(message));
  }

  async prepareStream(request: PrepareStreamRequest, callback: PrepareStreamCallback): Promise<void> {

    const camaraInfo = await this.camera.getCameraLiveStream();

    if (!camaraInfo) {
      this.logThenCallback(callback, 'Unable to start stream! Camera info was not received');
      return;
    }

    const ipv6 = request.addressVersion === 'ipv6';

    const options: pickPortOptions = {
      type: 'udp',
      ip: ipv6 ? '::' : '0.0.0.0',
      reserveTimeout: 15
    };
    const videoReturnPort = await pickPort(options);
    const videoSSRC = this.hap.CameraController.generateSynchronisationSource();
    const audioReturnPort = await pickPort(options);
    const audioSSRC = this.hap.CameraController.generateSynchronisationSource();


    const currentAddress = await this.getIpAddress(ipv6);

    const sessionInfo: SessionInfo = {
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

    const response: PrepareStreamResponse = {
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

  private async startStream(request: StartStreamRequest, callback: StreamRequestCallback): Promise<void> {

    const sessionInfo = this.pendingSessions[request.sessionID];
    const resolution = StreamingDelegate.determineResolution(request.video);
    const bitrate = request.video.max_bit_rate * 4;
    const vEncoder = this.config.vEncoder || 'libx264 -preset ultrafast -tune zerolatency'

    this.log.debug(`Video stream requested: ${request.video.width} x ${request.video.height}, ${request.video.fps} fps, ${request.video.max_bit_rate} kbps`, this.camera.getDisplayName());

    const nestStreamer = await getStreamer(this.log, this.camera, this.config);

    let ffmpegArgs: string;
    let nestStream: NestStream;

    try {
      nestStream = await nestStreamer.initialize(); // '-analyzeduration 15000000 -probesize 100000000 -i ' + streamInfo.streamUrls.rtspUrl;
      ffmpegArgs = nestStream.args;
    } catch (error: any) {
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
    } else if (snapshotArgs.length > 0) {
      this.log.debug('Snapshot path contains whitespace; skipping snapshot output on the live stream.', this.camera.getDisplayName());
    }

    if (this.platform.debugMode) {
      ffmpegArgs += ' -loglevel level+verbose';
    }

    const activeSession: ActiveSession = { streamer: nestStreamer };

    try {
      activeSession.socket = createSocket(sessionInfo.ipv6 ? 'udp6' : 'udp4');
      activeSession.socket.on('error', (err: Error) => {
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
    } catch (error: any) {
      this.logThenCallback(callback, error);
      return;
    }

    activeSession.mainProcess = new FfmpegProcess(this.camera.getDisplayName(), request.sessionID, ffmpegArgs, nestStream.stdin, this.log, this.platform.debugMode, this, callback);

    this.ongoingSessions[request.sessionID] = activeSession;
    delete this.pendingSessions[request.sessionID];

    // A RECONFIGURE that raced this (async) START stashed its bitrate; apply it now.
    const pendingBitrate = this.pendingMaxBitrate[request.sessionID];
    if (pendingBitrate) {
      delete this.pendingMaxBitrate[request.sessionID];
      activeSession.streamer.setMaxBitrate(pendingBitrate);
    }
  }

  async handleStreamRequest(request: StreamingRequest, callback: StreamRequestCallback): Promise<void> {
    switch (request.type) {
      case StreamRequestTypes.START:
        this.startStream(request, callback);
        break;
      case StreamRequestTypes.RECONFIGURE:
        // Resolution/fps can't change on a stream-copied feed, but the requested
        // bitrate can be re-advertised to the camera via REMB — which in copy
        // mode adapts the HomeKit-facing rate directly. A RECONFIGURE can arrive
        // while START is still initializing (session not yet registered), so
        // stash it and apply on register rather than dropping it.
        this.log.debug(`Received request to reconfigure: ${request.video.width} x ${request.video.height}, ${request.video.fps} fps, ${request.video.max_bit_rate} kbps`, this.camera.getDisplayName());
        if (this.ongoingSessions[request.sessionID]) {
          this.ongoingSessions[request.sessionID].streamer.setMaxBitrate(request.video.max_bit_rate * 1000);
        } else {
          this.pendingMaxBitrate[request.sessionID] = request.video.max_bit_rate * 1000;
        }
        callback();
        break;
      case StreamRequestTypes.STOP:
        await this.stopStream(request.sessionID);
        callback();
        break;
    }
  }

  public async stopStream(sessionId: string): Promise<void> {
    const session = this.ongoingSessions[sessionId];
    if (session) {
      if (session.timeout) {
        clearTimeout(session.timeout);
      }
      try {
        session.socket?.close();
      } catch (err) {
        this.log.error('Error occurred closing socket: ' + err, this.camera.getDisplayName());
      }
      try {
        session.mainProcess?.stop();
      } catch (err) {
        this.log.error('Error occurred terminating main FFmpeg process: ' + err, this.camera.getDisplayName());
      }
      try {
        session.returnProcess?.stop();
      } catch (err) {
        this.log.error('Error occurred terminating two-way FFmpeg process: ' + err, this.camera.getDisplayName());
      }
      try {
        await session.streamer.teardown();
      } catch (err) {
        this.log.error('Error terminating SDM stream: ' + err, this.camera.getDisplayName());
      }
    }

    delete this.ongoingSessions[sessionId];
    delete this.pendingMaxBitrate[sessionId];
    this.log.debug('Stopped video stream.', this.camera.getDisplayName());
  }

  private newSession(token: number, nestStreamer: NestStreamer): Session {
    let resolvePromise!: () => void;
    let initReadyResolved = false;
    const initReady = new Promise<void>(resolve => resolvePromise = resolve);

    return {
      token,
      nestStreamer,
      // Assigned by createRecordingSession after initialize() returns. Keeping the
      // Session identity alive before then lets failed initialization use the same
      // cleanup owner (NestStreamer.teardown is guarded for an unset token).
      hksvStreamer: undefined as unknown as HksvStreamer,
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
      watchdog: undefined as unknown as ReturnType<typeof setInterval>,
      acquisitionSettled: false,
      cleaned: false,
    };
  }

  private async cleanupSession(s: Session): Promise<void> {
    if (s.cleanupPromise)
      return s.cleanupPromise;

    s.cleanupPromise = (async () => {
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
      notify?.();
      (s.hksvStreamer as HksvStreamer | undefined)?.destroy();

      let teardownTimer: ReturnType<typeof setTimeout> | undefined;
      s.teardownComplete = Promise.resolve().then(() => s.nestStreamer.teardown()).then(
        () => undefined,
        error => {
          this.log.error('Error tearing down recording SDM stream: ' + error, this.camera.getDisplayName());
        });
      const teardownTimedOut = await Promise.race([
        s.teardownComplete.then(() => false),
        new Promise<boolean>(resolve => teardownTimer = setTimeout(
          () => resolve(true), StreamingDelegate.TEARDOWN_TIMEOUT_MS))
      ]);
      if (teardownTimer)
        clearTimeout(teardownTimer);
      if (teardownTimedOut)
        this.log.error('Timed out tearing down recording SDM stream.', this.camera.getDisplayName());

      if (this.prewarm === s)
        this.prewarm = undefined;
      if (this.recordingSessionInfo?.token === s.token) {
        this.recordingSessionInfo = undefined;
      }
      if (s.acquisitionSettled && this.acquiring?.token === s.token)
        this.acquiring = undefined;
    })();

    return s.cleanupPromise;
  }

  private startSessionWatchdog(s: Session): void {
    s.lastProgress = Date.now();
    s.watchdog = setInterval(() => {
      if (!s.cleaned && Date.now() - s.lastProgress > StreamingDelegate.IDLE_MS) {
        this.log.error('Recording stream stalled; releasing the session.', this.camera.getDisplayName());
        void this.cleanupSession(s);
      }
    }, StreamingDelegate.WATCHDOG_INTERVAL_MS);
  }

  private startSessionProducer(s: Session): void {
    this.startSessionWatchdog(s);
    s.producer = this.runPrewarmProducer(s);
  }

  private async runPrewarmProducer(s: Session): Promise<void> {
    const pending: Buffer[] = [];

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
        notify?.();
        if (s.ending)
          break;
      }
    } catch (error: any) {
      if (!s.cleaned)
        this.log.error("Encountered unexpected error on recording producer " + (error.stack || error));
    } finally {
      s.ended = true;
      s.resolveInitReady();
      const notify = s.notify;
      s.notify = null;
      notify?.();
      if (!s.adopted)
        await this.cleanupSession(s);
    }
  }

  private armPrewarmTtl(s: Session): void {
    if (s.cleaned)
      return;
    if (s.ttl)
      clearTimeout(s.ttl);
    s.ttl = setTimeout(() => {
      this.log.debug("Idle pre-warm timed out; released the stream.", this.camera.getDisplayName());
      void this.cleanupSession(s);
    }, StreamingDelegate.PREWARM_TTL_MS);
  }

  private motionDetected(): boolean {
    return Boolean(this.accessory.getService(this.hap.Service.MotionSensor)
      ?.getCharacteristic(this.platform.Characteristic.MotionDetected).value);
  }

  private beginAcquisition(replacingToken?: number): RecordingAcquisition {
    if (this.acquiring)
      throw new Error('A recording stream is already being acquired.');
    if (this.recordingSessionInfo && this.recordingSessionInfo.token !== replacingToken)
      throw new Error('A recording stream is already active.');

    const acquisition = {
      token: this.nextSessionToken++,
      cancel: false,
    };
    this.acquiring = acquisition;
    return acquisition;
  }

  private clearAcquisition(token: number): void {
    if (this.acquiring?.token === token)
      this.acquiring = undefined;
  }

  private currentAcquisition(): RecordingAcquisition | undefined {
    return this.acquiring;
  }

  private async cancelAcquisitionAndAwaitPrewarmSetup(): Promise<void> {
    if (this.acquiring)
      this.acquiring.cancel = true;

    const setup = this.prewarmSetup;
    if (!setup)
      return;
    try {
      await setup;
    } catch {
      // notifyMotion owns setup failure logging.
    }
  }

  private async createRecordingSession(acquisition: RecordingAcquisition): Promise<Session> {
    const configuration = this.cameraRecordingConfiguration;
    if (!configuration)
      throw new Error('No recording configuration for this camera.');

    if (configuration.videoCodec.type !== VideoCodecType.H264)
      throw new Error('Unsupported recording codec type.');

    const profile = configuration.videoCodec.parameters.profile === H264Profile.HIGH ? "high"
        : configuration.videoCodec.parameters.profile === H264Profile.MAIN ? "main" : "baseline";

    const level = configuration.videoCodec.parameters.level === H264Level.LEVEL4_0 ? "4.0"
        : configuration.videoCodec.parameters.level === H264Level.LEVEL3_2 ? "3.2" : "3.1";

    const videoArgs: Array<string> = [
      "-an",
      "-sn",
      "-dn",
      "-codec:v",
      "libx264",
      "-pix_fmt",
      "yuv420p",
      "-profile:v", profile,
      "-level:v", level,
      "-b:v", `${configuration.videoCodec.parameters.bitRate}k`,
      "-force_key_frames", `expr:eq(t,n_forced*${configuration.videoCodec.parameters.iFrameInterval / 1000})`,
      "-r", configuration.videoCodec.resolution[2].toString(),
    ];

    let samplerate: string;
    switch (configuration.audioCodec.samplerate) {
      case AudioRecordingSamplerate.KHZ_8:
        samplerate = "8";
        break;
      case AudioRecordingSamplerate.KHZ_16:
        samplerate = "16";
        break;
      case AudioRecordingSamplerate.KHZ_24:
        samplerate = "24";
        break;
      case AudioRecordingSamplerate.KHZ_32:
        samplerate = "32";
        break;
      case AudioRecordingSamplerate.KHZ_44_1:
        samplerate = "44.1";
        break;
      case AudioRecordingSamplerate.KHZ_48:
        samplerate = "48";
        break;
      default:
        throw new Error("Unsupported audio sample rate: " + configuration.audioCodec.samplerate);
    }

    const audioArgs: Array<string> = this.controller?.recordingManagement?.recordingManagementService
      .getCharacteristic(this.platform.Characteristic.RecordingAudioActive)
        ? [
          "-acodec", "libfdk_aac",
          ...(configuration.audioCodec.type === AudioRecordingCodecType.AAC_LC
            ? ["-profile:a", "aac_low"]
            : ["-profile:a", "aac_eld"]),
          "-ar", `${samplerate}k`,
          "-b:a", `${configuration.audioCodec.bitrate}k`,
          "-ac", `${configuration.audioCodec.audioChannels}`,
        ]
        : [];

    let s: Session | undefined;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    let operationSettled = false;

    const operation = (async (): Promise<Session> => {
      try {
        const nestStreamer = await getStreamer(this.log, this.camera, this.config);
        s = this.newSession(acquisition.token, nestStreamer);
        if (acquisition.cancel || this.acquiring?.token !== acquisition.token)
          throw new Error('Recording acquisition cancelled.');

        const nestStream = await nestStreamer.initialize();
        if (acquisition.cancel || s.cleaned || this.acquiring?.token !== acquisition.token)
          throw new Error('Recording acquisition cancelled.');

        s.hksvStreamer = new HksvStreamer(
          this.log,
          nestStream,
          audioArgs,
          videoArgs,
          this.platform.debugMode,
          this.snapshotOutputArgs()
        );
        await s.hksvStreamer.start();
        if (acquisition.cancel || s.cleaned || s.hksvStreamer.destroyed
            || this.acquiring?.token !== acquisition.token)
          throw new Error('Recording acquisition cancelled.');

        return s;
      } catch (error) {
        if (s)
          await this.cleanupSession(s);
        else
          this.clearAcquisition(acquisition.token);
        throw error;
      } finally {
        operationSettled = true;
        if (s) {
          s.acquisitionSettled = true;
          // cleanupSession is deliberately bounded, but acquisition ownership
          // must outlive a teardown promise that is still retiring the Nest
          // stream. Otherwise a successor could overlap that retiring stream.
          if (s.cleaned && s.teardownComplete)
            await s.teardownComplete;
          if (s.cleaned)
            this.clearAcquisition(s.token);
        } else {
          this.clearAcquisition(acquisition.token);
        }
      }
    })();

    try {
      return await Promise.race([
        operation,
        new Promise<never>((_, reject) => {
          timeout = setTimeout(() => {
            acquisition.cancel = true;
            if (s)
              void this.cleanupSession(s);
            reject(new Error(`Recording acquisition timed out after ${StreamingDelegate.ACQUIRE_TIMEOUT_MS}ms.`));
          }, StreamingDelegate.ACQUIRE_TIMEOUT_MS);
        })
      ]);
    } catch (error) {
      acquisition.cancel = true;
      if (s)
        await this.cleanupSession(s);
      else if (operationSettled)
        this.clearAcquisition(acquisition.token);
      throw error;
    } finally {
      if (timeout)
        clearTimeout(timeout);
    }
  }

  public async notifyMotion(): Promise<void> {
    if (this.config.motionPrebuffer === false
        || !this.recordingActive
        || !this.cameraRecordingConfiguration
        || this.acquiring
        || this.recordingSessionInfo
        || this.prewarmSetup
        || this.prewarm?.adopted)
      return;

    const configuration = this.cameraRecordingConfiguration;
    if (this.prewarm) {
      if (!this.prewarm.cleaned) {
        if (!this.prewarm.adopted)
          this.armPrewarmTtl(this.prewarm);
        return;
      }
      await this.cleanupSession(this.prewarm);
      if (this.prewarm || this.acquiring || this.prewarmSetup || this.recordingSessionInfo
          || !this.recordingActive || this.cameraRecordingConfiguration !== configuration)
        return;
    }

    const acquisition = this.beginAcquisition();
    const setup = (async () => {
      let s: Session | undefined;
      try {
        this.log.debug("Pre-warming recording stream on motion", this.camera.getDisplayName());
        s = await this.createRecordingSession(acquisition);

        if (acquisition.cancel
            || this.acquiring?.token !== s.token
            || !this.recordingActive
            || this.cameraRecordingConfiguration !== configuration
            || this.recordingSessionInfo) {
          await this.cleanupSession(s);
          return;
        }

        this.prewarm = s;
        this.clearAcquisition(s.token);
        this.armPrewarmTtl(s);
        this.startSessionProducer(s);
      } catch (error: any) {
        if (s)
          await this.cleanupSession(s);
        else
          this.clearAcquisition(acquisition.token);
        this.log.error("Unable to pre-warm recording stream: " + (error.stack || error), this.camera.getDisplayName());
      }
    })();

    this.prewarmSetup = setup;
    try {
      await setup;
    } finally {
      if (this.prewarmSetup === setup)
        this.prewarmSetup = undefined;
    }
  }

  private async awaitPrewarmSetup(): Promise<void> {
    const setup = this.prewarmSetup;
    if (!setup)
      return;

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = await Promise.race([
      setup.then(() => false, () => false),
      new Promise<boolean>(resolve => timer = setTimeout(
        () => resolve(true), StreamingDelegate.PREWARM_SETUP_NOTICE_MS))
    ]);
    if (timer)
      clearTimeout(timer);

    if (timedOut)
      this.log.debug('Pre-warm setup is still running; waiting before recording acquisition.', this.camera.getDisplayName());

    // initialize() cannot be cancelled. Even after the diagnostic timeout, wait
    // for setup to finish so a cold path can never open a second Nest stream.
    try {
      await setup;
    } catch {
      // notifyMotion owns logging and cleanup for setup failures.
    }
  }

  private async waitForSessionData(s: Session): Promise<void> {
    if (s.live.length || s.ended)
      return;

    await new Promise<void>(resolve => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;
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

  private async *consumeSession(s: Session, endOnMotionStop: boolean): AsyncGenerator<RecordingPacket> {
    await s.initReady;
    if (!s.initFragment || s.cleaned)
      return;

    yield {data: s.initFragment, isLast: false};

    let heldMedia: Buffer | undefined;
    let loggedMotionEnd = false;

    while (!s.cleaned) {
      if (endOnMotionStop && !this.motionDetected() && !s.ending) {
        // The producer may already be assembling an mdat. It observes ending
        // after appending that current fragment, then sets ended in its finally.
        s.ending = true;
        loggedMotionEnd = true;
      }

      let fragment: Buffer | undefined;
      if (s.ring.length) {
        fragment = s.ring.shift();
      } else if (s.live.length) {
        fragment = s.live.shift();
        if (fragment)
          s.liveBytes -= fragment.length;
      }

      if (fragment) {
        if (heldMedia) {
          if (s.cleaned)
            return;
          yield {data: heldMedia, isLast: false};
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
      yield {data: heldMedia, isLast: true};
    if (loggedMotionEnd)
      this.log.debug("Ending session after draining the motion backlog.", this.camera.getDisplayName());
  }

  private async *consumeColdSession(s: Session, endOnMotionStop: boolean): AsyncGenerator<RecordingPacket> {
    const pending: Buffer[] = [];
    let heldMedia: Buffer | undefined;
    let producerError: any;

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
          yield {data: fragment, isLast: false};
          continue;
        }

        if (box.type !== 'mdat')
          continue;

        if (heldMedia) {
          if (s.cleaned)
            return;
          yield {data: heldMedia, isLast: false};
        }
        heldMedia = fragment;

        if (endOnMotionStop && !this.motionDetected()) {
          s.ending = true;
          this.log.debug("Ending session due to motion stopped!", this.camera.getDisplayName());
          break;
        }
      }
    } catch (error: any) {
      producerError = error;
    } finally {
      s.ended = true;
      s.resolveInitReady();
    }

    if (s.cleaned)
      return;

    // A one-fragment delay lets both motion termination and producer EOF mark
    // the final media fragment, never the initialization (moov) fragment.
    if (heldMedia)
      yield {data: heldMedia, isLast: true};

    if (producerError)
      this.log.error("Encountered unexpected error on cold recording producer "
        + (producerError.stack || producerError), this.camera.getDisplayName());
  }

  closeRecordingStream(streamId: number, reason: HDSProtocolSpecificErrorReason | undefined): void {
    // CameraRecordingDelegate's close hook is synchronous. Continue the
    // cancellation asynchronously so an in-flight pre-warm setup is awaited and
    // cannot publish after this close.
    void this.cancelAcquisitionAndAwaitPrewarmSetup();

    const info = this.recordingSessionInfo;
    if (!info)
      return;

    if (info.streamId !== streamId) {
      this.log.debug(`Ignoring recording close for a stale/replaced session id ${streamId}.`, this.camera.getDisplayName());
      return;
    }

    // Capture and compare the token, not just the stream id. If the session was
    // replaced between lookup and close, this stale close is a no-op.
    if (this.recordingSessionInfo?.token !== info.token)
      return;

    void this.cleanupSession(info.session);
  }

  acknowledgeStream(streamId: number): void {
    this.closeRecordingStream(streamId, undefined);
  }

  async *handleRecordingStreamRequest(streamId: number): AsyncGenerator<RecordingPacket> {
    this.log.debug('Recording request received.')

    if (this.recordingSessionInfo || this.prewarm?.adopted
        || (this.acquiring && !this.prewarmSetup)) {
      this.log.error('Ignoring overlapping recording request while another session is active.', this.camera.getDisplayName());
      return;
    }

    const endOnMotionStop = this.config.endRecordingOnMotionStop ?? true;
    let s: Session | undefined;
    let adoptedPrewarm = false;
    let acquisition: RecordingAcquisition | undefined;

    try {
      await this.awaitPrewarmSetup();

      // Multiple requests can await the same pre-warm setup. The first one to
      // resume claims or starts a token owner; every later one sees that owner.
      if (this.recordingSessionInfo || this.acquiring || this.prewarm?.adopted) {
        this.log.error('Ignoring overlapping recording request while another session is active.', this.camera.getDisplayName());
        return;
      }
      if (!this.recordingActive)
        throw new Error('Recording is inactive.');

      const prewarm = this.prewarm;
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
        if (!s.initFragment && !s.cleaned) {
          // Reserve the replacement token before awaiting teardown, closing the
          // adoption-to-cold gap against motion and overlapping requests.
          const fallbackAcquisition = this.beginAcquisition(s.token);
          acquisition = fallbackAcquisition;
          await this.cleanupSession(s);
          s = undefined;
          if (fallbackAcquisition.cancel || !this.recordingActive) {
            this.clearAcquisition(fallbackAcquisition.token);
            throw new Error('Recording acquisition cancelled.');
          }
          s = await this.createRecordingSession(fallbackAcquisition);
          adoptedPrewarm = false;
        } else if (s.cleaned) {
          throw new Error('Recording session was closed during adoption.');
        }
      }

      if (!s) {
        acquisition = this.beginAcquisition();
        s = await this.createRecordingSession(acquisition);
      }

      if (!adoptedPrewarm) {
        const owner = this.currentAcquisition();
        if (!owner || owner.token !== s.token || owner.cancel
            || !this.recordingActive || s.cleaned) {
          await this.cleanupSession(s);
          throw new Error('Recording acquisition cancelled before publication.');
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
    } catch (error: any) {
      this.log.error("Encountered unexpected error on recording generator " + (error.stack || error));
      throw error;
    } finally {
      if (s)
        await this.cleanupSession(s);
      if (acquisition)
        this.clearAcquisition(acquisition.token);
    }
  }

  updateRecordingActive(active: boolean): void {
    this.recordingActive = active;
    this.log.debug("Recording active set to " + active);
    if (!active) {
      if (this.acquiring)
        this.acquiring.cancel = true;
      const prewarm = this.prewarm;
      const recording = this.recordingSessionInfo?.session;
      if (prewarm)
        void this.cleanupSession(prewarm);
      if (recording && recording.token !== prewarm?.token)
        void this.cleanupSession(recording);
    }
  }

  updateRecordingConfiguration(configuration: CameraRecordingConfiguration | undefined): void {
    this.cameraRecordingConfiguration = configuration;
    if (this.prewarm && !this.prewarm.adopted)
      void this.cleanupSession(this.prewarm);
  }
}
