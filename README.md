# Linear Packager

A live linear transcoder/packager written in Go. Takes a local TS/MP4 file or an SRT SPTS stream as input and produces:

- **HLS** (multi-bitrate, sliding-window)
- **DASH** (dynamic MPD, SegmentList)
- **SCTE-35 ad-break signalling** via an inbound ESAM HTTP endpoint

Each "channel" runs as an independent process with its own `config.json`. Multiple channels can run on the same machine using the included systemd template unit.

---

## Prerequisites

| Dependency | Version | Notes |
|---|---|---|
| Go | 1.22+ | Build only |
| FFmpeg | 4.4+ | Must be in `$PATH` |
| systemd | Any | EC2/server deployment only |

Install on Amazon Linux 2 / Ubuntu:
```bash
# FFmpeg (static build)
sudo apt-get install -y ffmpeg        # Ubuntu
# or
sudo yum install -y ffmpeg            # Amazon Linux (may need RPM Fusion)

# Go
wget https://go.dev/dl/go1.22.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.22.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

---

## Build

```bash
git clone <repo>
cd linear-packager
make build
```

This produces a single statically-linked binary: `./linear-packager`.

To embed a version tag:
```bash
make build VERSION=1.2.3
# or rely on git tags:
git tag v1.2.3
make build     # VERSION resolved automatically via git describe
```

---

## Local quick-start

1. **Edit `config.json`** (see [Configuration reference](#configuration-reference))
2. **Run:**
   ```bash
   ./linear-packager -config config.json
   ```
3. **Play (VLC / ffplay):**
   - HLS:  `http://localhost:8080/hls/master.m3u8`
   - DASH: `http://localhost:8080/dash/manifest.mpd`
4. **Health check:**
   ```bash
   curl http://localhost:8080/health
   ```

---

## Creating a new channel

A "channel" is just a directory containing a `config.json`.  
Copy and customise the template:

```bash
cp config.json /path/to/channels/ch2/config.json
$EDITOR /path/to/channels/ch2/config.json
```

Minimum fields to change for each channel:

```jsonc
{
  "channel": {
    "id": "ch2",                    // unique — also used as systemd instance name
    "name": "Channel Two"
  },
  "input": {
    "type": "file",                 // or "srt"
    "file": {
      "path": "/media/ch2/input.ts",
      "loop": true,
      "live_simulation": true       // false = encode as fast as possible (testing only)
    }
  },
  "server": {
    "http_port": 8081,              // different port per channel on the same host
    "base_url": "http://my-ec2-ip:8081"
  },
  "esam": {
    "enabled": true,
    "listen_port": 9090,            // informational only — ESAM shares the http_port above
    "path": "/esam/notify",
    "acquisition_point_identity": "ch2-origin"
  }
}
```

The ABR ladder, segment duration, and output directories are all per-channel fields — see the full [Configuration reference](#configuration-reference) below.

---

## SRT input

Switch the input type in `config.json` to receive an SPTS via SRT:

```json
"input": {
  "type": "srt",
  "srt": {
    "host": "0.0.0.0",
    "port": 1234,
    "mode": "listener",
    "latency_ms": 200,
    "passphrase": ""
  }
}
```

**Modes:**
- `listener` — the packager listens; the encoder pushes to it
- `caller`   — the packager calls out to an SRT server

---

## Sending a SCTE-35 ad-break signal (ESAM)

The packager exposes an HTTP endpoint that accepts a CableLabs ESAM `SignalProcessingNotification` POST.  
On receipt it generates a binary SCTE-35 `splice_insert()` section and injects it into both HLS and DASH output at the next segment boundary.

### Example — 30-second ad break

```bash
curl -s -X POST http://localhost:8080/esam/notify \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ns3:SignalProcessingNotification acquisitionPointIdentity="ch1-origin"
    xmlns:sig="urn:cablelabs:md:xsd:signaling:3.0"
    xmlns:ns5="urn:cablelabs:iptvservices:esam:xsd:common:1"
    xmlns:ns2="urn:cablelabs:md:xsd:core:3.0"
    xmlns:ns4="urn:cablelabs:md:xsd:content:3.0"
    xmlns:ns3="urn:cablelabs:iptvservices:esam:xsd:signal:1">
  <ns5:StatusCode classCode="0"/>
  <ns3:ResponseSignal action="create" signalPointID="sp1"
      acquisitionTime="2026-06-05T13:00:00Z"
      acquisitionSignalID="sig001"
      acquisitionPointIdentity="ch1-origin">
    <sig:UTCPoint utcPoint="2026-06-05T13:00:00Z"/>
    <sig:SCTE35PointDescriptor spliceCommandType="5">
      <sig:SpliceInsert spliceEventID="2001"
          outOfNetworkIndicator="true"
          uniqueProgramID="1"
          duration="PT30S">
      </sig:SpliceInsert>
    </sig:SCTE35PointDescriptor>
  </ns3:ResponseSignal>
  <ns3:ConditioningInfo acquisitionSignalIDRef="sig001" duration="PT30S"/>
</ns3:SignalProcessingNotification>'
```

**Expected response:** HTTP 200 with `<ns5:StatusCode classCode="0" detail="OK"/>`

**What appears in the HLS playlist** at the next segment boundary:
```
#EXT-OATCLS-SCTE35:0xfc3020...
#EXT-X-CUE-OUT:30.000
#EXTINF:6.000,
../../segments/1080p/seg00004.ts
#EXT-X-CUE-OUT-CONT:ElapsedTime=6.000,Duration=30.000
...
#EXT-X-CUE-IN
```

**What appears in the DASH MPD:**
```xml
<EventStream schemeIdUri="urn:scte:scte35:2013:bin" timescale="1">
  <Event presentationTime="24" duration="30" id="2001">
    /DAlAAAA...base64...
  </Event>
</EventStream>
```

---

## EC2 deployment

### 1. Provision the instance

Recommended: `t3.medium` or larger (the packager runs one FFmpeg process per channel).  
Open inbound TCP ports:
- Your `http_port` per channel (default 8080)
- Optionally 22 (SSH)

### 2. Install binary and systemd unit

```bash
# On your build machine:
make build
scp linear-packager ec2-user@<ip>:/tmp/

# On the EC2 instance:
sudo mv /tmp/linear-packager /usr/local/bin/
sudo chmod +x /usr/local/bin/linear-packager
sudo cp deploy/linear-packager@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

Or use `make install` if you build directly on the instance:
```bash
sudo make install
```

### 3. Create the packager user (recommended)

```bash
sudo useradd -r -s /sbin/nologin packager
```

### 4. Set up a channel

```bash
CHANNEL=ch1
sudo mkdir -p /opt/linear-packager/channels/$CHANNEL
sudo cp config.json /opt/linear-packager/channels/$CHANNEL/config.json
sudo $EDITOR /opt/linear-packager/channels/$CHANNEL/config.json
sudo chown -R packager:packager /opt/linear-packager/
```

Or use the Makefile helper:
```bash
sudo make new-channel CHANNEL=ch1
```

### 5. Start the channel

```bash
sudo systemctl enable --now linear-packager@ch1
```

### 6. Check status and logs

```bash
systemctl status linear-packager@ch1
journalctl -u linear-packager@ch1 -f
```

### 7. Multiple channels on the same instance

Each channel needs a **different `http_port`** in its `config.json`:

| Channel | http_port | URL |
|---|---|---|
| ch1 | 8080 | `http://<ec2-ip>:8080/hls/master.m3u8` |
| ch2 | 8081 | `http://<ec2-ip>:8081/hls/master.m3u8` |

```bash
sudo make new-channel CHANNEL=ch2
# edit /opt/linear-packager/channels/ch2/config.json → set http_port: 8081
sudo systemctl enable --now linear-packager@ch2
```

Stop / restart a channel:
```bash
sudo systemctl restart linear-packager@ch1
sudo systemctl stop    linear-packager@ch1
```

---

## Configuration reference

```jsonc
{
  // ── Channel identity ────────────────────────────────────────────────────
  "channel": {
    "id": "ch1",           // unique channel identifier
    "name": "Channel One"  // human-readable name (appears in /health)
  },

  // ── Input ────────────────────────────────────────────────────────────────
  "input": {
    "type": "file",        // "file" or "srt"

    "file": {
      "path": "/media/input.mp4",
      "loop": true,        // loop forever (for 24/7 playout from file)
      "live_simulation": true  // read at native rate (-re); set false only for testing
    },

    "srt": {
      "host": "0.0.0.0",
      "port": 1234,
      "mode": "listener",  // "listener" (push-to-us) or "caller" (we pull)
      "latency_ms": 200,
      "passphrase": ""     // leave empty for unencrypted
    }
  },

  // ── Transcoder / ABR ladder ───────────────────────────────────────────────
  "transcoder": {
    "video_codec": "libx264",  // FFmpeg video encoder
    "audio_codec": "aac",
    "preset": "veryfast",      // libx264 preset: ultrafast → veryslow
    "keyframe_interval": 2,    // seconds; must divide segment_duration evenly

    "ladder": [
      {
        "name": "1080p",
        "width": 1920, "height": 1080,
        "video_bitrate": "4000k",
        "audio_bitrate": "192k",
        "framerate": 30
      },
      {
        "name": "720p",
        "width": 1280, "height": 720,
        "video_bitrate": "2000k",
        "audio_bitrate": "128k",
        "framerate": 30
      },
      {
        "name": "360p",
        "width": 640, "height": 360,
        "video_bitrate": "800k",
        "audio_bitrate": "96k",
        "framerate": 25
      }
    ]
  },

  // ── Packaging ─────────────────────────────────────────────────────────────
  "packaging": {
    "segment_duration": 6,        // seconds per segment (keyframe_interval must divide this)
    "work_dir": "./output/segments",  // where raw TS segments are written

    // Minimum TS files to keep on disk per rung.
    // 0 = auto: max(hls.playlist_window, dash.window_size) + 2
    // For linear (no startover) keep this at the minimum — default auto is fine.
    "segment_retention": 7,

    "hls": {
      "enabled": true,
      "playlist_window": 5,       // segments kept in sliding m3u8 window
      "output_dir": "./output/hls"
    },
    "dash": {
      "enabled": true,
      "window_size": 5,           // segments kept in MPD SegmentList
      "output_dir": "./output/dash"
    }
  },

  // ── ESAM inbound endpoint (SCTE-35 ad signals) ────────────────────────────
  "esam": {
    "enabled": true,
    "listen_port": 9090,          // informational; endpoint shares server.http_port
    "path": "/esam/notify",       // HTTP path to POST ESAM payloads to
    "acquisition_point_identity": "ch1-origin"
  },

  // ── HTTP server ────────────────────────────────────────────────────────────
  "server": {
    "http_port": 8080,
    "base_url": "http://localhost:8080"  // used to build URLs in /health response
  }
}
```

---

## Output URL structure

All content is served from a single HTTP port:

| URL | Content |
|---|---|
| `/health` | JSON status + version |
| `/hls/master.m3u8` | HLS multi-bitrate master playlist |
| `/hls/<rung>/media.m3u8` | HLS per-rendition sliding-window playlist |
| `/segments/<rung>/seg#####.ts` | Raw MPEG-TS segments (referenced by HLS playlists) |
| `/dash/manifest.mpd` | DASH dynamic MPD |
| `/dash/<rung>/seg#####.mp4` | Fragmented MP4 DASH segments |
| `/esam/notify` | Inbound ESAM SOAP endpoint (POST) |

---

## Troubleshooting

**FFmpeg not found**
```
ERROR starting ffmpeg: exec: "ffmpeg": executable file not found in $PATH
```
Make sure `ffmpeg` is in `$PATH` for the user running the packager.  
Check: `which ffmpeg` or `ffmpeg -version`.

**Port already in use**
```
ERROR HTTP server error: listen tcp :8080: bind: address already in use
```
Another process owns that port. Change `server.http_port` in `config.json` or kill the other process.

**No segments after startup**
Normal — the first segment takes `segment_duration` seconds plus FFmpeg startup time (typically 3–10 s).  
Check: `ls output/segments/1080p/`

**HLS lag / large lag warnings from FFmpeg**
The encoder is running slower than real-time. Options:
- Switch to a faster preset: `"preset": "ultrafast"`
- Remove a high-resolution rung from the ladder
- Upgrade to a CPU with more cores

**ESAM returns 400**
Check that the `acquisitionPointIdentity` in the XML matches the value in `config.json → esam.acquisition_point_identity`.  
Inspect the response body for the specific parse error.

**Segments not deleted / disk fills up**
Verify `segment_retention` is set to a small value (default auto = `max(window) + 2 = 7`).  
With 3 rungs at 4+2+0.8 Mbps and 6 s segments, 7 files per rung uses ~7×3×(~1.8 MB avg) ≈ 38 MB maximum on disk.
