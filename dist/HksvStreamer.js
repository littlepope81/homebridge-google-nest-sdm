"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = require("events");
const child_process_1 = require("child_process");
const net_1 = require("net");
const stream_1 = require("stream");
class HksvStreamer {
    constructor(log, nestStream, audioOutputArgs, videoOutputArgs, debugMode, ffmpegPath, snapshotOutputArgs = [], 
    /**
     * Names the camera this process belongs to, for the ffmpeg output below.
     * Without it the debug log carries every ffmpeg's stderr with no attribution
     * at all, and since periodic snapshots run one process per camera on a timer,
     * a recording's own output is interleaved with five other cameras' -- which is
     * unreadable, and worse, invites confident conclusions drawn from whichever
     * camera name happened to sit nearby. Measured wrongly that way more than once.
     */
    label = '', 
    /**
     * Reports each input geometry this recording decodes, so the owner can learn what
     * a camera is actually capable of across recordings. See the note on
     * largestRecordingGeometry in StreamingDelegate.
     */
    onGeometry) {
        this.snapshotOutputArgs = snapshotOutputArgs;
        this.label = label;
        this.onGeometry = onGeometry;
        this.destroyed = false;
        this.nestStream = nestStream;
        this.debugMode = debugMode;
        this.log = log;
        this.connectPromise = new Promise(resolve => this.connectResolve = resolve);
        this.server = (0, net_1.createServer)(this.handleConnection.bind(this));
        // Resolved once by Platform (see FfmpegPath.ts) and passed in, so the recording
        // path and the live path can never disagree about which ffmpeg runs.
        this.ffmpegPath = ffmpegPath;
        this.args = [];
        // BEFORE the input, deliberately. "+genpts" is a DEMUXER flag: it tells the input
        // to synthesise PTS for packets that arrive without one. Placed after "-i" it binds
        // to the output format context instead, where it cannot fix a missing input
        // timestamp — which is where this option sat, harmlessly, for as long as the
        // recording path transcoded. libx264 re-timestamped every frame on the way out, so
        // nothing downstream ever saw the gap.
        //
        // With "-codec:v copy" there is no encoder, and untimestamped packets pass straight
        // into the mp4. The audio track is still re-encoded (libfdk_aac emits clean PTS), so
        // the result is a clip whose video freezes while audio plays on — on every camera,
        // regardless of link quality. Measured here: 48 "Timestamps are unset in a packet
        // for stream 0" across 23 recordings, every one naming stream 0 (video), none naming
        // audio.
        // Verbose, deliberately, and filtered on the way out (see the stderr handler below).
        // The decoder only reports a mid-stream resolution change as "Reinit context to WxH",
        // and only at verbose -- measured on this bridge's ffmpeg 8.0: 0 such lines at the
        // default "info", 3 at "verbose". Running at info is why homebridge.log has never been
        // able to answer whether a camera changes resolution mid-recording, and why the absence
        // of those lines was nearly read as evidence that it does not.
        this.args.push("-loglevel", "verbose");
        this.args.push("-fflags", "+genpts");
        this.args.push(...nestStream.args.split(/ /g));
        this.args.push(...audioOutputArgs);
        this.args.push("-f", "mp4");
        this.args.push(...videoOutputArgs);
        // -reset_timestamps stays an output option; it is a muxer setting, unlike +genpts.
        this.args.push("-reset_timestamps", "1");
        this.args.push("-movflags", "frag_keyframe+empty_moov+default_base_moof");
    }
    convertStringToStream(stringToConvert) {
        const stream = new stream_1.Readable();
        stream._read = () => { };
        stream.push(stringToConvert);
        stream.push(null);
        return stream;
    }
    async start() {
        var _a, _b;
        this.log.debug('HksvStreamer start command received.');
        const promise = (0, events_1.once)(this.server, "listening");
        this.server.listen(); // listen on random port
        await promise;
        if (this.destroyed) {
            return;
        }
        const port = this.server.address().port;
        this.args.push("tcp://127.0.0.1:" + port);
        // Additional output keeping the camera's on-disk snapshot JPEG fresh
        // while a recording runs, so tiles update on every motion event.
        this.args.push(...this.snapshotOutputArgs);
        this.log.debug(this.ffmpegPath + " " + this.args.join(" "));
        this.childProcess = (0, child_process_1.spawn)(this.ffmpegPath, this.args, { env: process.env, stdio: 'pipe' });
        this.childProcess.on('error', (error) => {
            this.log.error(error.message);
            this.handleDisconnect();
        });
        this.childProcess.on('exit', this.handleDisconnect.bind(this));
        if (!this.childProcess.stdin && this.nestStream.stdin) {
            this.log.error('HksvStreamer failed to start stream: input to ffmpeg was provided as stdin, but the process does not support stdin.');
        }
        if (this.childProcess.stdin) {
            if (this.nestStream.stdin) {
                const sdpStream = this.convertStringToStream(this.nestStream.stdin);
                sdpStream.resume();
                sdpStream.pipe(this.childProcess.stdin);
            }
        }
        // Per LINE, not per chunk. A single stderr 'data' event routinely carries a dozen
        // lines, and logging the chunk whole means only its first line gets a timestamp and
        // a tag -- the rest land bare, indistinguishable from any other process's output.
        //
        // Attached unconditionally, not just in debugMode: the geometry watch below has to see
        // every line. The full ffmpeg firehose is still debug-only, so a normal install gets at
        // most one extra line per recording.
        const emit = (data) => {
            for (const line of data.toString().split(/\r?\n/)) {
                if (!line.trim().length)
                    continue;
                this.watchGeometry(line);
                if (this.debugMode && !HksvStreamer.isVerboseOnlyNoise(line))
                    this.log.debug(line, this.label);
            }
        };
        (_a = this.childProcess.stdout) === null || _a === void 0 ? void 0 : _a.on("data", emit);
        (_b = this.childProcess.stderr) === null || _b === void 0 ? void 0 : _b.on("data", emit);
    }
    static isVerboseOnlyNoise(line) {
        return HksvStreamer.VERBOSE_ONLY_NOISE.some(pattern => line.includes(pattern));
    }
    /**
     * Reports the geometry ffmpeg is actually decoding, and any mid-recording change to it.
     *
     * This exists because the recording path no longer pins output geometry with a filter: a
     * transcode is already geometry-stable, since ffmpeg's default -autoscale locks output to
     * the first frame. That makes a resolution change harmless to the clip -- but it also makes
     * it invisible, and whether these cameras change resolution mid-recording is still an open
     * question that no log has ever answered. One line per recording answers it.
     *
     * Two sources, because they catch different things: the input banner gives the geometry a
     * recording STARTED at (enough to show a camera is adaptive across recordings), while
     * "Reinit context" is the only report of a change DURING one.
     */
    watchGeometry(line) {
        var _a;
        let geometry;
        const reinit = line.match(/Reinit context to (\d{2,5}x\d{2,5})/);
        if (reinit)
            geometry = reinit[1];
        else if (line.includes('Video: h264') && !line.includes(' q=')) {
            // The " q=" exclusion keeps this on the INPUT banner. ffmpeg prints a second
            // "Video: h264" line for the output stream, and matching it would report a
            // spurious change the moment input and output geometry ever differ.
            const banner = line.match(/,\s(\d{2,5}x\d{2,5})[,\s]/);
            if (banner)
                geometry = banner[1];
        }
        if (!geometry || geometry === this.lastGeometry)
            return;
        if (this.lastGeometry)
            this.log.info(`Recording input geometry changed ${this.lastGeometry} -> ${geometry}. `
                + `The clip keeps its first geometry; a copied stream would have broken here.`, this.label);
        else
            this.log.info(`Recording input geometry ${geometry}.`, this.label);
        this.lastGeometry = geometry;
        const [width, height] = geometry.split('x').map(Number);
        if (width > 0 && height > 0)
            (_a = this.onGeometry) === null || _a === void 0 ? void 0 : _a.call(this, width, height);
    }
    destroy() {
        var _a, _b;
        this.log.debug('HksvStreamer destroy command received, ending process.');
        const child = this.childProcess;
        this.childProcess = undefined;
        this.destroyed = true;
        if (child) {
            child.kill(); // SIGTERM
            // Escalate to SIGKILL if it doesn't exit: a stuck ffmpeg that ignores SIGTERM would
            // otherwise be orphaned (a leaked process — the class of bug #150 is about).
            const killTimer = setTimeout(() => { try {
                child.kill('SIGKILL');
            }
            catch (e) { /* already gone */ } }, 2000);
            child.once('exit', () => clearTimeout(killTimer));
        }
        // Destroy the accepted socket too. read() listens for 'close', so this unblocks a generator
        // stuck in read() immediately, whether or not ffmpeg actually exits — without it, a hung
        // ffmpeg leaves the read() awaiting forever and the recording generator never returns.
        (_a = this.socket) === null || _a === void 0 ? void 0 : _a.destroy();
        this.socket = undefined;
        // Close the listening server if a client never connected (otherwise the socket leaks for the
        // life of the process), and resolve connectPromise so a generator still awaiting a connection
        // that will now never come unblocks (it then throws the "Unexpected state!" guard below).
        try {
            this.server.close(() => { });
        }
        catch (e) { /* not listening */ }
        (_b = this.connectResolve) === null || _b === void 0 ? void 0 : _b.call(this);
    }
    handleDisconnect() {
        var _a, _b;
        this.log.debug('Socket destroyed.');
        (_a = this.socket) === null || _a === void 0 ? void 0 : _a.destroy();
        this.socket = undefined;
        // If ffmpeg exits or errors before ever connecting (e.g. a bad input), connectPromise would
        // otherwise never settle and generator() would await it forever, hanging the recording and
        // leaking the server. Close it and resolve so the generator fails fast and visibly instead.
        try {
            this.server.close(() => { });
        }
        catch (e) { /* not listening */ }
        (_b = this.connectResolve) === null || _b === void 0 ? void 0 : _b.call(this);
    }
    handleConnection(socket) {
        var _a;
        this.server.close(); // don't accept any further clients
        this.socket = socket;
        (_a = this.connectResolve) === null || _a === void 0 ? void 0 : _a.call(this);
    }
    /**
     * Generator for `MP4Atom`s.
     * Throws error to signal EOF when socket is closed.
     */
    async *generator() {
        await this.connectPromise;
        if (!this.socket || !this.childProcess) {
            this.log.debug("Socket undefined " + !!this.socket + " childProcess undefined " + !!this.childProcess);
            throw new Error("Unexpected state!");
        }
        while (this.childProcess) {
            const header = await this.read(8);
            const length = header.readInt32BE(0) - 8;
            const type = header.slice(4).toString();
            const data = await this.read(length);
            yield {
                header: header,
                length: length,
                type: type,
                data: data,
            };
        }
    }
    async read(length) {
        if (!this.socket) {
            throw Error("FFMPEG tried reading from closed socket!");
        }
        if (!length) {
            return Buffer.alloc(0);
        }
        const value = this.socket.read(length);
        if (value) {
            return value;
        }
        return new Promise((resolve, reject) => {
            const cleanup = () => {
                var _a, _b;
                (_a = this.socket) === null || _a === void 0 ? void 0 : _a.removeListener("readable", readHandler);
                (_b = this.socket) === null || _b === void 0 ? void 0 : _b.removeListener("close", endHandler);
            };
            const readHandler = () => {
                const value = this.socket.read(length);
                if (value) {
                    cleanup();
                    resolve(value);
                }
            };
            const endHandler = () => {
                cleanup();
                reject(new Error(`FFMPEG socket closed during read for ${length} bytes!`));
            };
            if (!this.socket) {
                throw new Error("FFMPEG socket is closed now!");
            }
            this.socket.on("readable", readHandler);
            this.socket.on("close", endHandler);
        });
    }
}
exports.default = HksvStreamer;
/**
 * Lines that exist ONLY because this process now runs at verbose, and that a debug user was
 * not getting before. Without this filter, raising the log level to catch one diagnostic
 * line would quietly undo #220, which was specifically about ffmpeg drowning the debug log.
 *
 * Chosen by measurement, not guesswork: ffmpeg's own output was diffed at info vs verbose on
 * the same input (38 lines vs 57), and each pattern below was then checked to appear zero
 * times at info. That check earned its keep -- "Stream #0:0" looks verbose-only in a naive
 * diff because verbose words it differently, but it appears three times at BOTH levels, so
 * suppressing it would have cost debug users a line they always had and blinded the geometry
 * watch to the input banner.
 *
 * "Reinit context" is verbose-only too and is deliberately NOT here: it is the line the
 * verbosity was raised for, and it is worth seeing.
 *
 * Fail-safe by construction. If a future ffmpeg renames one of these, the line simply
 * reappears in the debug log -- a stale pattern can cost noise, never signal. The one known
 * wrinkle is that ffmpeg glues its "frame=..." progress output onto the front of the next
 * line, so a progress line can be dropped if it happens to be glued to a suppressed one.
 */
HksvStreamer.VERBOSE_ONLY_NOISE = [
    "Statistics:",
    "Terminating demuxer",
    "Terminating muxer",
    "All streams finished",
    "No more output streams",
    "EOF in input file",
    "Total: ",
    "packets read (",
    "frames encoded",
    "[graph ",
    "[scaler_out_",
    "Input file #",
    "Output file #",
];
//# sourceMappingURL=HksvStreamer.js.map