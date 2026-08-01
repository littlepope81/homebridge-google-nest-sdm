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
import {getStreamer, NestStream, NestStreamer, WebRtcNestStreamer} from "./NestStreamer";
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
  configuration: CameraRecordingConfiguration;
  kind: 'prewarm' | 'recording';
  done: Promise<void>;
  resolveDone: () => void;
  session?: Session;
}

type Session = {
  token: number;
  configuration: CameraRecordingConfiguration;
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
  private inFlightAcquisitions = new Set<RecordingAcquisition>();
  private nextSessionToken = 1;
  private shuttingDown = false;

  constructor(log: Logger, api: API, platform: Platform, camera: Camera, accessory: PlatformAccessory) {
    this.platform = platform;
    this.log = log;
    this.hap = api.hap;
    this.config = platform.platformConfig as unknown as Config
    this.camera = camera;
    this.accessory = accessory;

    api.on(APIEvent.SHUTDOWN, async () => {
      this.shuttingDown = true;

      const acquisitions = Array.from(this.inFlightAcquisitions);
      acquisitions.forEach(acquisition => acquisition.cancel = true);

      const recording = this.recordingSessionInfo?.session;
      const prewarm = this.prewarm;
      const cleanup: Promise<void>[] = [];
      acquisitions.forEach(acquisition => {
        if (acquisition.session)
          cleanup.push(this.cleanupSession(acquisition.session));
      });
      if (prewarm)
        cleanup.push(this.cleanupSession(prewarm));
      if (recording && recording.token !== prewarm?.token)
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
        new Promise<void>(resolve => setTimeout(resolve, 5000)),
      ]);

      // A cancelled acquisition can expose its Session only while settling.
      // Recheck after joining it and initiate cleanup for anything it published.
      const lateRecording = this.recordingSessionInfo?.session;
      const latePrewarm = this.prewarm;
      const lateCleanup: Promise<void>[] = [];
      if (latePrewarm)
        lateCleanup.push(this.cleanupSession(latePrewarm));
      if (lateRecording && lateRecording.token !== latePrewarm?.token)
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
          // Mandatory in CameraRecordingOptions with a documented floor of 4000ms, so this
          // cannot be set to "none" -- every plugin advertises at least 4000 whether or not
          // it has a prebuffer behind it. Nothing backs it here yet: motionPrewarm only
          // starts buffering once an event has been delivered. See issue #233.
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
    // Detach first so a successor never waits for this bounded teardown and an
    // old stop cannot later delete a newly-published session with the same id.
    delete this.ongoingSessions[sessionId];
    delete this.pendingMaxBitrate[sessionId];

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
        let teardownTimer: ReturnType<typeof setTimeout> | undefined;
        const teardownComplete = Promise.resolve().then(() => session.streamer.teardown()).then(
          () => false,
          err => {
            this.log.error('Error terminating SDM stream: ' + err, this.camera.getDisplayName());
            return false;
          });
        const teardownTimedOut = await Promise.race([
          teardownComplete,
          new Promise<boolean>(resolve => teardownTimer = setTimeout(
            () => resolve(true), StreamingDelegate.TEARDOWN_TIMEOUT_MS))
        ]);
        if (teardownTimer)
          clearTimeout(teardownTimer);
        if (teardownTimedOut)
          this.log.error('Timed out terminating SDM stream.', this.camera.getDisplayName());
      } catch (err) {
        this.log.error('Error initiating SDM stream teardown: ' + err, this.camera.getDisplayName());
      }
    }

    this.log.debug('Stopped video stream.', this.camera.getDisplayName());
  }

  private newSession(
    token: number,
    configuration: CameraRecordingConfiguration,
    nestStreamer: NestStreamer
  ): Session {
    let resolvePromise!: () => void;
    let initReadyResolved = false;
    const initReady = new Promise<void>(resolve => resolvePromise = resolve);

    return {
      token,
      configuration,
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

      // Relinquish publication/ownership before retiring the remote stream.
      // New acquisition is allowed to overlap this bounded teardown.
      if (this.prewarm === s)
        this.prewarm = undefined;
      if (this.recordingSessionInfo?.token === s.token)
        this.recordingSessionInfo = undefined;
      if (s.acquisitionSettled && this.acquiring?.token === s.token)
        this.acquiring = undefined;

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

  private beginAcquisition(kind: 'prewarm' | 'recording', replacingToken?: number): RecordingAcquisition {
    if (this.shuttingDown)
      throw new Error('Homebridge is shutting down.');
    if (this.acquiring)
      throw new Error('A recording stream is already being acquired.');
    if (this.recordingSessionInfo && this.recordingSessionInfo.token !== replacingToken)
      throw new Error('A recording stream is already active.');

    const configuration = this.cameraRecordingConfiguration;
    if (!configuration)
      throw new Error('No recording configuration for this camera.');

    let resolveDone!: () => void;
    const done = new Promise<void>(resolve => resolveDone = resolve);
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

  private clearAcquisition(token: number): void {
    if (this.acquiring?.token === token)
      this.acquiring = undefined;
  }

  private settleAcquisition(acquisition: RecordingAcquisition): void {
    this.inFlightAcquisitions.delete(acquisition);
    acquisition.resolveDone();
  }

  private currentAcquisition(): RecordingAcquisition | undefined {
    return this.acquiring;
  }

  private cancelAcquisition(): void {
    if (this.acquiring) {
      const acquisition = this.acquiring;
      acquisition.cancel = true;
      this.clearAcquisition(acquisition.token);
      if (acquisition.session)
        void this.cleanupSession(acquisition.session);
    }
  }

  private cancelPrewarmAcquisition(): void {
    if (this.acquiring?.kind === 'prewarm') {
      const acquisition = this.acquiring;
      acquisition.cancel = true;
      this.clearAcquisition(acquisition.token);
      if (acquisition.session)
        void this.cleanupSession(acquisition.session);
    }
  }

  /**
   * FFmpeg video args for the HKSV recording path.
   *
   * On the WebRTC path the camera's H.264 is copied rather than re-encoded. Nothing hub-side
   * validates delivered media against the negotiation -- hap-nodejs's RecordingManagement
   * chunks each fragment and ships it without parsing moof/mdat/SPS -- and this recording path
   * has never applied a scale filter, so the plugin has been delivering un-negotiated
   * resolutions for years without complaint. HksvStreamer's "-movflags frag_keyframe" still
   * starts every fragment on a keyframe, and fragmentLength is a maximum, so the source's own
   * IDR cadence stays within contract.
   *
   * RTSP cameras keep the encoder. WebRtcNestStreamer runs a FIR/PLI keyframe-request loop
   * that holds the IDR interval near 2s; RtspNestStreamer has no equivalent and the Nest RTSP
   * IDR cadence is unmeasured. If it exceeded the negotiated fragmentLength, copied fragments
   * would breach the one limit "-force_key_frames" was guaranteeing.
   *
   * Gated on the streamer instance actually constructed rather than a second
   * getVideoProtocol() call, so the decision cannot drift from the stream fed to ffmpeg.
   *
   * Credit: ajplotkin, potmat/homebridge-google-nest-sdm#238 (issue #235).
   */
  private recordingVideoArgs(
    configuration: CameraRecordingConfiguration,
    nestStreamer: NestStreamer
  ): Array<string> {
    // No "-an" in here: HksvStreamer pushes audioOutputArgs BEFORE videoOutputArgs, so an
    // unconditional -an at the head of videoArgs silently overrides the AAC-ELD block and
    // records every clip mute. Audio-off is expressed from audioArgs instead.
    if (nestStreamer instanceof WebRtcNestStreamer)
      return ["-sn", "-dn", "-codec:v", "copy"];

    const profile = configuration.videoCodec.parameters.profile === H264Profile.HIGH ? "high"
        : configuration.videoCodec.parameters.profile === H264Profile.MAIN ? "main" : "baseline";

    const level = configuration.videoCodec.parameters.level === H264Level.LEVEL4_0 ? "4.0"
        : configuration.videoCodec.parameters.level === H264Level.LEVEL3_2 ? "3.2" : "3.1";

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
      "-pix_fmt",
      "yuv420p",
      "-profile:v", profile,
      "-level:v", level,
      "-b:v", `${configuration.videoCodec.parameters.bitRate}k`,
      "-force_key_frames", `expr:eq(t,n_forced*${configuration.videoCodec.parameters.iFrameInterval / 1000})`,
      "-r", configuration.videoCodec.resolution[2].toString(),
    ];
  }

  private async createRecordingSession(acquisition: RecordingAcquisition): Promise<Session> {
    const configuration = acquisition.configuration;
    try {

      if (configuration.videoCodec.type !== VideoCodecType.H264)
        throw new Error('Unsupported recording codec type.');

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

    // .value, not the Characteristic object: getCharacteristic() returns the object, which is
    // always truthy, so this branch was taken regardless of the Home app's "Record Audio"
    // setting. That was moot while videoArgs led with an unconditional "-an" (HksvStreamer
    // pushes audioOutputArgs BEFORE videoOutputArgs, so the -an overrode this whole block and
    // every clip recorded mute), which is likely why it went unnoticed. Both halves are fixed
    // together: the -an is gone from videoArgs, and audio-off is expressed from the else-branch
    // here where it can actually be conditional.
    const audioArgs: Array<string> = this.controller?.recordingManagement?.recordingManagementService
      .getCharacteristic(this.platform.Characteristic.RecordingAudioActive)?.value
        ? [
          "-acodec", "libfdk_aac",
          ...(configuration.audioCodec.type === AudioRecordingCodecType.AAC_LC
            ? ["-profile:a", "aac_low"]
            : ["-profile:a", "aac_eld"]),
          "-ar", `${samplerate}k`,
          "-b:a", `${configuration.audioCodec.bitrate}k`,
          "-ac", `${configuration.audioCodec.audioChannels}`,
        ]
        : ["-an"];

    let s: Session | undefined;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    let operationSettled = false;

    const operation = (async (): Promise<Session> => {
      try {
        const nestStreamer = await getStreamer(this.log, this.camera, this.config);
        // Built here rather than above: the copy-vs-transcode decision depends on which
        // streamer was actually constructed.
        const videoArgs = this.recordingVideoArgs(configuration, nestStreamer);
        s = this.newSession(acquisition.token, configuration, nestStreamer);
        acquisition.session = s;
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
          void this.cleanupSession(s);
        else
          this.clearAcquisition(acquisition.token);
        throw error;
      } finally {
        operationSettled = true;
        if (s) {
          s.acquisitionSettled = true;
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
        void this.cleanupSession(s);
      else if (operationSettled)
        this.clearAcquisition(acquisition.token);
      throw error;
    } finally {
      if (timeout)
        clearTimeout(timeout);
    }
    } finally {
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
  public async notifyMotion(): Promise<void> {
    if (this.shuttingDown
        || this.config.motionPrewarm === false
        || !this.recordingActive
        || !this.cameraRecordingConfiguration
        || this.acquiring
        || this.recordingSessionInfo
        || this.prewarmSetup
        || this.prewarm?.adopted)
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
      let s: Session | undefined;
      try {
        this.log.debug("Pre-warming recording stream on motion", this.camera.getDisplayName());
        s = await this.createRecordingSession(acquisition);

        if (acquisition.cancel
            || this.acquiring?.token !== s.token
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
      } catch (error: any) {
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
    } finally {
      if (this.prewarmSetup === setup)
        this.prewarmSetup = undefined;
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
        || this.acquiring?.kind === 'recording') {
      this.log.error('Ignoring overlapping recording request while another session is active.', this.camera.getDisplayName());
      return;
    }

    const endOnMotionStop = this.config.endRecordingOnMotionStop ?? true;
    let s: Session | undefined;
    let adoptedPrewarm = false;
    let acquisition: RecordingAcquisition | undefined;

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
          this.prewarmSetup.catch(() => { /* setup failure -> fall through to cold */ }),
          new Promise<void>(resolve => setTimeout(resolve, StreamingDelegate.ACQUIRE_TIMEOUT_MS)),
        ]);
      }

      if (this.recordingSessionInfo || this.prewarm?.adopted) {
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
          if (this.recordingSessionInfo?.token === s.token)
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
