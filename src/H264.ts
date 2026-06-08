/**
 * Minimal H.264 RTP payload parsing, just enough to pull the SPS (NAL type 7)
 * and PPS (NAL type 8) parameter sets out of an incoming WebRTC video stream.
 *
 * These parameter sets are stable for a given camera and carry the video
 * dimensions FFmpeg needs before it can write the output header. By caching
 * them we can hand them to FFmpeg up front (via sprop-parameter-sets) instead
 * of waiting for it to probe them out of the live stream.
 *
 * See RFC 6184 for the H.264 RTP packetization rules referenced below.
 */

export interface ParameterSets {
    sps?: Buffer;
    pps?: Buffer;
}

const NAL_TYPE_IDR = 5;
const NAL_TYPE_SPS = 7;
const NAL_TYPE_PPS = 8;
const NAL_TYPE_STAP_A = 24;
const NAL_TYPE_FU_A = 28;

/**
 * Extract any SPS/PPS NAL units contained in a single H.264 RTP payload.
 *
 * SPS/PPS are tiny and are sent either as single NAL units or aggregated in a
 * STAP-A packet, so those are the only two packetization modes handled here.
 * Fragmented (FU-A) packets never carry parameter sets and are ignored.
 *
 * The returned buffers are the raw NAL units (including the 1-byte NAL header,
 * without any start code or length prefix) — exactly the bytes that go into an
 * SDP sprop-parameter-sets attribute once base64 encoded.
 */
export function extractParameterSets(payload: Buffer): ParameterSets {
    const result: ParameterSets = {};

    if (!payload || payload.length < 1)
        return result;

    const nalType = payload[0] & 0x1f;

    if (nalType === NAL_TYPE_SPS) {
        result.sps = Buffer.from(payload);
    } else if (nalType === NAL_TYPE_PPS) {
        result.pps = Buffer.from(payload);
    } else if (nalType === NAL_TYPE_STAP_A) {
        // STAP-A: [STAP-A header (1 byte)] then repeated [size (2 bytes BE)][NAL unit].
        let offset = 1;
        while (offset + 2 <= payload.length) {
            const size = payload.readUInt16BE(offset);
            offset += 2;
            if (size === 0 || offset + size > payload.length)
                break;
            const nalu = payload.subarray(offset, offset + size);
            const type = nalu[0] & 0x1f;
            if (type === NAL_TYPE_SPS)
                result.sps = Buffer.from(nalu);
            else if (type === NAL_TYPE_PPS)
                result.pps = Buffer.from(nalu);
            offset += size;
        }
    }

    return result;
}

/**
 * Whether an H.264 RTP payload carries (the start of) an IDR keyframe — the
 * first decodable picture FFmpeg can actually emit. Used only for diagnostics
 * timing of how long after stream start the first keyframe arrives.
 */
export function containsKeyframe(payload: Buffer): boolean {
    if (!payload || payload.length < 1)
        return false;

    const nalType = payload[0] & 0x1f;

    if (nalType === NAL_TYPE_IDR)
        return true;

    if (nalType === NAL_TYPE_FU_A && payload.length >= 2) {
        // FU-A: payload[1] is the FU header; its start bit marks the first fragment
        // and its low 5 bits carry the real NAL type.
        const start = (payload[1] & 0x80) !== 0;
        return start && (payload[1] & 0x1f) === NAL_TYPE_IDR;
    }

    if (nalType === NAL_TYPE_STAP_A) {
        let offset = 1;
        while (offset + 2 <= payload.length) {
            const size = payload.readUInt16BE(offset);
            offset += 2;
            if (size === 0 || offset + size > payload.length)
                break;
            if ((payload[offset] & 0x1f) === NAL_TYPE_IDR)
                return true;
            offset += size;
        }
    }

    return false;
}
