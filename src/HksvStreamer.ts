import { once } from "events";
import { ChildProcess, spawn } from "child_process";
import { AddressInfo, createServer, Server, Socket } from "net";
import {Logger} from "homebridge";
import {Readable} from "stream";
import {NestStream} from "./NestStreamer";

interface MP4Atom {
    header: Buffer;
    length: number;
    type: string;
    data: Buffer;
}

export default class HksvStreamer {
    readonly server: Server;
    readonly ffmpegPath: string;
    readonly args: string[];
    private nestStream: NestStream;
    private debugMode: boolean;
    private log: Logger;

    socket?: Socket;
    childProcess?: ChildProcess;
    destroyed = false;

    connectPromise: Promise<void>;
    connectResolve?: () => void;

    constructor(log: Logger, nestStream: NestStream, audioOutputArgs: Array<string>, videoOutputArgs: Array<string>, debugMode: boolean,
                ffmpegPath: string,
                private readonly snapshotOutputArgs: Array<string> = [],
                /**
                 * Names the camera this process belongs to, for the ffmpeg output below.
                 * Without it the debug log carries every ffmpeg's stderr with no attribution
                 * at all, and since periodic snapshots run one process per camera on a timer,
                 * a recording's own output is interleaved with five other cameras' -- which is
                 * unreadable, and worse, invites confident conclusions drawn from whichever
                 * camera name happened to sit nearby. Measured wrongly that way more than once.
                 */
                private readonly label: string = '',
                /**
                 * Reports each input geometry this recording decodes, so the owner can learn what
                 * a camera is actually capable of across recordings. See the note on
                 * largestRecordingGeometry in StreamingDelegate.
                 */
                private readonly onGeometry?: (width: number, height: number) => void) {
        this.nestStream = nestStream;
        this.debugMode = debugMode;
        this.log = log;
        this.connectPromise = new Promise(resolve => this.connectResolve = resolve);

        this.server = createServer(this.handleConnection.bind(this));

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
        this.args.push("-reset_timestamps",
            "1");
        this.args.push(
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        );
    }

    convertStringToStream(stringToConvert: string) {
        const stream = new Readable();
        stream._read = () => { };
        stream.push(stringToConvert);
        stream.push(null);
        return stream;
    }

    async start() {

        this.log.debug('HksvStreamer start command received.');

        const promise = once(this.server, "listening");
        this.server.listen(); // listen on random port
        await promise;

        if (this.destroyed) {
            return;
        }

        const port = (this.server.address() as AddressInfo).port;
        this.args.push("tcp://127.0.0.1:" + port);
        // Additional output keeping the camera's on-disk snapshot JPEG fresh
        // while a recording runs, so tiles update on every motion event.
        this.args.push(...this.snapshotOutputArgs);

        this.log.debug(this.ffmpegPath + " " + this.args.join(" "));

        this.childProcess = spawn(this.ffmpegPath, this.args, { env: process.env, stdio: 'pipe' });

        this.childProcess.on('error', (error: Error) => {
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
        //
        // Buffered across chunks. A 'data' event is a byte boundary, not a line boundary: ffmpeg
        // can split "Reinit context to 1920x" / "1088, pix_fmt: yuv420p" across two chunks, and
        // splitting each chunk independently drops the record silently. Split on BARE \r too,
        // because ffmpeg delimits its "frame=..." progress with carriage returns, so several
        // logical records otherwise arrive glued into one line.
        const consume = (chunk: any, stream: { remainder: string }) => {
            const text = stream.remainder + chunk.toString();
            const parts = text.split(/\r\n|\r|\n/);
            stream.remainder = parts.pop() ?? '';
            for (const line of parts)
                handleLine(line);
        };

        const handleLine = (line: string) => {
            if (!line.trim().length) return;
            this.watchGeometry(line);
            if (this.debugMode && !HksvStreamer.isVerboseOnlyNoise(line))
                this.log.debug(line, this.label);
        };

        const outState = { remainder: '' };
        const errState = { remainder: '' };
        this.childProcess.stdout?.on("data", (chunk: any) => consume(chunk, outState));
        this.childProcess.stderr?.on("data", (chunk: any) => consume(chunk, errState));
        // Whatever is left when the pipe closes is a complete line that never got its newline.
        this.childProcess.stdout?.on("end", () => handleLine(outState.remainder));
        this.childProcess.stderr?.on("end", () => handleLine(errState.remainder));
    }

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
    private static readonly VERBOSE_ONLY_NOISE = [
        /\[AVIOContext @ [^\]]*\] Statistics:/,
        /\[[^\]]*\] Terminating (demuxer|muxer) thread/,
        /\[[^\]]*\] All streams finished/,
        /No more output streams to write to, finishing\./,
        /EOF in input file \d+/,
        /Total: \d+ packets \([\d ]+bytes\) (demuxed|muxed)/,
        /Input stream #\d+:\d+ \([a-z]+\): \d+ packets read/,
        /Output stream #\d+:\d+ \([a-z]+\): \d+ frames encoded/,
        /\[graph[^\]]*\] w:\d+ h:\d+ pixfmt:/,
        /\[scaler_out_[^\]]*\] w:\d+ h:\d+ (flags|fmt):/,
        /^Input file #\d+ \(/,
        /^Output file #\d+ \(/,
    ];

    /**
     * Never suppress anything that smells like a failure. The earlier version of this list used
     * bare substrings such as "[scaler_out_" and "[graph ", which are COMPONENT prefixes, not
     * message kinds -- so "[scaler_out_0_0] Failed to configure output pad" was silently eaten,
     * hiding a failure in the exact filter that recording geometry depends on. The claim that a
     * stale pattern "can cost noise, never signal" was simply wrong, and this is the guard.
     */
    private static readonly NEVER_SUPPRESS = /error|fail|fatal|invalid|unable|cannot|corrupt|overflow|denied/i;

    private static isVerboseOnlyNoise(line: string): boolean {
        if (HksvStreamer.NEVER_SUPPRESS.test(line))
            return false;
        return HksvStreamer.VERBOSE_ONLY_NOISE.some(pattern => pattern.test(line));
    }

    /**
     * Last input geometry seen from this recording's ffmpeg, so only CHANGES are logged.
     */
    private lastGeometry?: string;

    /**
     * Reports the geometry ffmpeg is actually decoding, and any mid-recording change to it.
     *
     * Reads ONE line shape: "[graph N input from stream N:N @ addr] w:W h:H ...". That choice is
     * the whole point of this function, so do not "simplify" it back to the obvious sources:
     *
     *   "Reinit context to WxH" reports the decoder's CODED allocation size, 16-aligned, NOT the
     *   visible frame. A perfectly constant 1920x1080 stream reports "Reinit context to
     *   1920x1088", and 640x360 reports 640x368. Learning from it means pinning recordings to a
     *   geometry the camera never emitted, and alternating between it and the real size logs
     *   phantom "changed" lines on a stream that never changed. Measured, not assumed.
     *
     *   The input banner "Stream #0:0: Video: h264 ..." does carry the visible size, but it is
     *   also printed for the OUTPUT stream, and telling them apart by looking for " q=" is a
     *   formatter detail that another ffmpeg or encoder can break -- at which point output
     *   geometry feeds back into learning.
     *
     * The graph input line has neither problem: it is the frame as it enters the filtergraph, it
     * is reprinted whenever the graph reinitialises, and it is emitted once per actual change.
     * Verified on a 640x360 -> 1920x1080 switch: two graph lines, the two real geometries, while
     * Reinit emitted 640x368 twice and 1920x1088 once.
     */
    private watchGeometry(line: string) {
        const match = line.match(/\[graph \d+ input from stream [\d:]+ @ [^\]]*\] w:(\d+) h:(\d+)/);
        if (!match)
            return;

        const width = Number(match[1]);
        const height = Number(match[2]);
        if (!HksvStreamer.isPlausibleGeometry(width, height)) {
            this.log.warn(`Ignoring implausible recording geometry ${width}x${height}.`, this.label);
            return;
        }

        const geometry = `${width}x${height}`;
        if (geometry === this.lastGeometry)
            return;

        if (this.lastGeometry)
            this.log.info(`Recording input geometry changed ${this.lastGeometry} -> ${geometry}. `
                + `The clip keeps its first geometry; a copied stream would have broken here.`,
                this.label);
        else
            this.log.info(`Recording input geometry ${geometry}.`, this.label);

        this.lastGeometry = geometry;
        this.onGeometry?.(width, height);
    }

    /**
     * Guards the learned geometry against garbage, because it is fed by log text and is used to
     * size a real encoder. A corrupt H.264 stream can make ffmpeg report absurd dimensions --
     * "Reinit context to 512x20448" is a documented case -- and since the learned value only ever
     * grows, one bad reading would pin every later recording on this camera to an enormous frame
     * until Homebridge restarts, burning CPU and likely failing outright.
     */
    private static isPlausibleGeometry(width: number, height: number): boolean {
        const withinBounds = (value: number) => Number.isInteger(value) && value >= 128 && value <= 4096;
        if (!withinBounds(width) || !withinBounds(height))
            return false;

        // Nothing a camera sends is this far from a normal picture shape.
        const aspect = width / height;
        return aspect >= 0.5 && aspect <= 4;
    }

    destroy() {
        this.log.debug('HksvStreamer destroy command received, ending process.');

        const child = this.childProcess;
        this.childProcess = undefined;
        this.destroyed = true;
        if (child) {
            child.kill(); // SIGTERM
            // Escalate to SIGKILL if it doesn't exit: a stuck ffmpeg that ignores SIGTERM would
            // otherwise be orphaned (a leaked process — the class of bug #150 is about).
            const killTimer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) { /* already gone */ } }, 2000);
            child.once('exit', () => clearTimeout(killTimer));
        }
        // Destroy the accepted socket too. read() listens for 'close', so this unblocks a generator
        // stuck in read() immediately, whether or not ffmpeg actually exits — without it, a hung
        // ffmpeg leaves the read() awaiting forever and the recording generator never returns.
        this.socket?.destroy();
        this.socket = undefined;
        // Close the listening server if a client never connected (otherwise the socket leaks for the
        // life of the process), and resolve connectPromise so a generator still awaiting a connection
        // that will now never come unblocks (it then throws the "Unexpected state!" guard below).
        try { this.server.close(() => { /* ignore "not running" */ }); } catch (e) { /* not listening */ }
        this.connectResolve?.();
    }

    handleDisconnect() {
        this.log.debug('Socket destroyed.')
        this.socket?.destroy();
        this.socket = undefined;
        // If ffmpeg exits or errors before ever connecting (e.g. a bad input), connectPromise would
        // otherwise never settle and generator() would await it forever, hanging the recording and
        // leaking the server. Close it and resolve so the generator fails fast and visibly instead.
        try { this.server.close(() => { /* ignore "not running" */ }); } catch (e) { /* not listening */ }
        this.connectResolve?.();
    }

    handleConnection(socket: Socket): void {
        this.server.close(); // don't accept any further clients
        this.socket = socket;
        this.connectResolve?.();
    }

    /**
     * Generator for `MP4Atom`s.
     * Throws error to signal EOF when socket is closed.
     */
    async* generator(): AsyncGenerator<MP4Atom> {

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

    async read(length: number): Promise<Buffer> {
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
                this.socket?.removeListener("readable", readHandler);
                this.socket?.removeListener("close", endHandler);
            };

            const readHandler = () => {
                const value = this.socket!.read(length);
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
