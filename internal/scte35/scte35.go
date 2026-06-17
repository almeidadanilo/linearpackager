// Package scte35 generates binary SCTE-35 splice_info_section structures.
//
// Reference: SCTE-35 2022 specification.
// splice_insert() with timed splice (splice_immediate_flag=0) is supported;
// pass ptsTime90kHz=0 to fall back to splice_immediate_flag=1.
package scte35

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// EncodeSpliceInsert builds a complete binary splice_info_section for a
// splice_insert() command.
//
//   - eventID         : splice_event_id (from ESAM spliceEventID attribute)
//   - outOfNetwork    : out_of_network_indicator (true = start of ad break)
//   - durationSec     : break duration in seconds (0 = no duration flag)
//   - uniqueProgramID : unique_program_id (from ESAM uniqueProgramID attribute)
//   - ptsTime90kHz    : splice_time PTS in 90 kHz ticks; 0 = use splice_immediate_flag=1
func EncodeSpliceInsert(eventID uint32, outOfNetwork bool, durationSec float64, uniqueProgramID uint16, ptsTime90kHz uint64) ([]byte, error) {
	// ── splice_insert() command body ────────────────────────────────────────
	var cmd []byte

	// splice_event_id                32 bits
	cmd = appendUint32(cmd, eventID)

	// splice_event_cancel_indicator(1) + reserved(7)
	cmd = append(cmd, 0x00)

	// flags byte:
	//   out_of_network_indicator  1
	//   program_splice_flag       1  = 1 (always)
	//   duration_flag             1  = 1 when durationSec > 0
	//   splice_immediate_flag     1  = 0 when ptsTime90kHz > 0, else 1
	//   reserved                  4
	immediate := ptsTime90kHz == 0
	durFlag := byte(0)
	if durationSec > 0 {
		durFlag = 0x20
	}
	immFlag := byte(0x10)
	if !immediate {
		immFlag = 0x00
	}
	flags := byte(0x40) | immFlag | durFlag // program_splice=1
	if outOfNetwork {
		flags |= 0x80
	}
	cmd = append(cmd, flags)

	// splice_time() — only present when program_splice=1 and immediate=0: 40 bits
	//   time_specified_flag(1)=1 + reserved(6)=111111 + pts_time(33)
	if !immediate {
		pts := ptsTime90kHz & 0x1FFFFFFFF // clamp to 33 bits
		cmd = append(cmd,
			byte(0xFE)|(byte(pts>>32)&0x01),
			byte(pts>>24),
			byte(pts>>16),
			byte(pts>>8),
			byte(pts),
		)
	}

	// break_duration (only when duration_flag=1): 40 bits
	//   auto_return(1) + reserved(6) + duration_90khz(33)
	if durationSec > 0 {
		ticks := uint64(durationSec * 90000)
		if ticks > 0x1FFFFFFFF {
			return nil, fmt.Errorf("duration_sec %.3f overflows 33-bit 90kHz tick field", durationSec)
		}
		bd := (uint64(1) << 39) | ticks // auto_return = 1
		cmd = append(cmd,
			byte(bd>>32),
			byte(bd>>24),
			byte(bd>>16),
			byte(bd>>8),
			byte(bd),
		)
	}

	// unique_program_id              16 bits
	cmd = appendUint16(cmd, uniqueProgramID)

	// avail_num(8) + avails_expected(8)
	cmd = append(cmd, 0x00, 0x00)

	cmdLen := len(cmd)

	// ── splice_info_section body (after section_length) ─────────────────────
	var body []byte

	// protocol_version = 0
	body = append(body, 0x00)

	// encrypted_packet(1)=0 + encryption_algorithm(6)=0 + pts_adjustment(33)=0
	// = 40 bits all zero = 5 bytes
	body = append(body, 0x00, 0x00, 0x00, 0x00, 0x00)

	// cw_index = 0
	body = append(body, 0x00)

	// tier(12)=0xFFF | splice_command_length(12)=cmdLen — packed into 3 bytes
	body = append(body,
		0xFF,
		byte(0xF0|(cmdLen>>8)),
		byte(cmdLen),
	)

	// splice_command_type = 5 (splice_insert)
	body = append(body, 0x05)
	body = append(body, cmd...)

	// descriptor_loop_length = 0
	body = append(body, 0x00, 0x00)

	// ── Final section assembly ───────────────────────────────────────────────
	// section_length = len(body) + 4 (CRC32 appended below)
	sectionLen := len(body) + 4

	var section []byte

	// table_id = 0xFC
	section = append(section, 0xFC)

	// section_syntax_indicator(1)=0, private_indicator(1)=0,
	// reserved(2)=11, section_length(12)
	section = append(section,
		byte(0x30|(sectionLen>>8)),
		byte(sectionLen),
	)
	section = append(section, body...)

	// CRC32 (MPEG-2 / SCTE-35)
	crc := crc32MPEG2(section)
	section = appendUint32(section, crc)

	return section, nil
}

// Base64Encode returns the standard-base64 encoding of a binary section —
// suitable for DASH EventStream payloads.
func Base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// HexEncode returns the upper-case hex representation prefixed with "0x" —
// suitable for HLS EXT-OATCLS-SCTE35 / EXT-X-DATERANGE tags.
func HexEncode(data []byte) string {
	return "0x" + hex.EncodeToString(data)
}

// ── helpers ─────────────────────────────────────────────────────────────────

func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendUint16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

// crc32MPEG2 computes the MPEG-2 CRC-32 used in SCTE-35 (polynomial 0x04C11DB7).
// It is NOT the same as the standard Go crc32.IEEE polynomial.
func crc32MPEG2(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b) << 24
		for j := 0; j < 8; j++ {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04C11DB7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
