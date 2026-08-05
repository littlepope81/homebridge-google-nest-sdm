import { execFile } from 'child_process';
import { Config } from './Config';

/**
 * Single source of truth for "which ffmpeg binary do we run?".
 *
 * Both spawn sites (live view via FfmpegProcess, HKSV recording via HksvStreamer)
 * used to carry their own copy of this fallback chain. They now take the path as a
 * parameter and this module is the only place that decides it.
 *
 *   config.videoProcessor set? ──yes──► use it            (user's explicit choice)
 *            │no
 *            ▼
 *   ffmpeg-for-homebridge resolves? ──yes──► use it       (default: nearly every user)
 *            │no
 *            ▼
 *   'ffmpeg' on PATH                                      (last resort)
 *
 * WHY THE BUNDLED BINARY IS THE DEFAULT: HKSV requires AAC-ELD, which requires the
 * libfdk_aac encoder. libfdk_aac is non-free and is therefore omitted from the default
 * ffmpeg builds shipped by Debian, Ubuntu, Alpine and most other distributions. The
 * bundled ffmpeg-for-homebridge build is what guarantees it exists. A user who points
 * videoProcessor at their distro ffmpeg will lose audio on BOTH paths, because
 * libfdk_aac is hard-coded in StreamingDelegate for live (-codec:a) and recording
 * (-acodec) alike. probeFfmpeg() exists to tell them that at startup rather than
 * leaving them to discover it from a silent clip weeks later.
 */

/** How long probeFfmpeg waits before giving up on the configured binary. */
export const PROBE_TIMEOUT_MS = 5000;

/**
 * Why a probe failed. Each kind needs a different message to be actionable: a typo in
 * the path is a different fix from a binary that runs but cannot encode AAC-ELD.
 */
export type ProbeFailureKind =
    | 'not_found'          // ENOENT — bad path, or arguments baked into the path
    | 'not_executable'     // EACCES — exists but not runnable by the Homebridge user
    | 'timeout'            // ran too long — stalled mount, or an unhealthy binary
    | 'exit_error'         // ran, exited non-zero — not an ffmpeg, or a broken build
    | 'missing_libfdk_aac' // ran fine, but cannot encode AAC-ELD: no HomeKit audio
    ;

export type ProbeResult =
    | { ok: true }
    | { ok: false, kind: ProbeFailureKind, detail: string };

/** Injectable so tests can drive the bundled-resolution branch without the package installed. */
export type BundledResolver = () => string | undefined | null | false;

const defaultBundledResolver: BundledResolver = () => {
    try {
        return require('ffmpeg-for-homebridge');
    } catch (error) {
        // Package absent or unsupported platform — fall through to PATH.
        return undefined;
    }
};

/**
 * Decide which ffmpeg binary to run. Pure apart from the bundled lookup, which is
 * injectable for tests.
 */
export function resolveFfmpegPath(
    config: Pick<Config, 'videoProcessor'> | undefined,
    bundledResolver: BundledResolver = defaultBundledResolver
): string {
    // Trim and reject whitespace-only, so a blank field in the config UI behaves as
    // "not set" rather than spawning ''.
    const configured = config?.videoProcessor?.trim();
    if (configured)
        return configured;

    const bundled = bundledResolver();
    if (bundled)
        return bundled;

    return 'ffmpeg';
}

/**
 * Run the resolved binary and check it can actually encode AAC-ELD.
 *
 * execFile is used rather than spawn so the timeout and the "never started" case are
 * handled by Node instead of hand-rolled process lifecycle code. The path is passed as
 * the command with an argv array, so a path containing spaces is safe — but a path with
 * ARGUMENTS baked into it ("/usr/bin/ffmpeg -hide_banner") is treated as one filename
 * and correctly reported as not_found.
 */
export function probeFfmpeg(ffmpegPath: string, timeoutMs: number = PROBE_TIMEOUT_MS): Promise<ProbeResult> {
    return new Promise<ProbeResult>(resolve => {
        execFile(ffmpegPath, ['-hide_banner', '-encoders'], { timeout: timeoutMs }, (error: any, stdout, stderr) => {
            if (error) {
                if (error.code === 'ENOENT')
                    return resolve({ ok: false, kind: 'not_found', detail: ffmpegPath });
                if (error.code === 'EACCES' || error.code === 'EPERM')
                    return resolve({ ok: false, kind: 'not_executable', detail: ffmpegPath });
                // execFile reports a timeout by killing the child; killed is the reliable signal.
                if (error.killed || error.signal)
                    return resolve({ ok: false, kind: 'timeout', detail: `${timeoutMs}ms` });
                return resolve({ ok: false, kind: 'exit_error', detail: String(error.message ?? error) });
            }

            // -encoders writes to stdout, but tolerate builds that report on stderr.
            const output = `${stdout ?? ''}${stderr ?? ''}`;
            if (!output.includes('libfdk_aac'))
                return resolve({ ok: false, kind: 'missing_libfdk_aac', detail: ffmpegPath });

            resolve({ ok: true });
        });
    });
}

/**
 * Human-readable explanation for a failed probe. Kept next to the failure kinds so a new
 * kind cannot be added without someone writing the message that goes with it.
 */
export function describeProbeFailure(result: Extract<ProbeResult, { ok: false }>): string {
    switch (result.kind) {
        case 'not_found':
            return `videoProcessor "${result.detail}" was not found. Check the path. Note that it must be a path to a binary only — arguments cannot be included.`;
        case 'not_executable':
            return `videoProcessor "${result.detail}" is not executable by the Homebridge user. Check its permissions.`;
        case 'timeout':
            return `videoProcessor did not respond within ${result.detail}. If it lives on a network mount or removable media, that is the likely cause.`;
        case 'exit_error':
            return `videoProcessor could not be queried for its encoders: ${result.detail}`;
        case 'missing_libfdk_aac':
            return `videoProcessor "${result.detail}" cannot encode AAC-ELD (no libfdk_aac). Live view and HKSV recordings will BOTH fail on audio. Most distribution ffmpeg builds omit libfdk_aac because it is non-free; remove the videoProcessor setting to use the bundled ffmpeg instead.`;
    }
}
