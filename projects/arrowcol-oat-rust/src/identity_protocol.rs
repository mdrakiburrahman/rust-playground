//! Bounded transport frames for the isolated identity reader.

use std::{fmt, io::{self, ErrorKind}};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const HEADER_BYTES: u64 = 5;
const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameKind {
    Ready,
    Request,
    Snapshot,
    Identity,
    Terminal,
}

impl FrameKind {
    fn wire(self) -> u8 {
        match self {
            Self::Ready => 1,
            Self::Request => 2,
            Self::Snapshot => 3,
            Self::Identity => 4,
            Self::Terminal => 5,
        }
    }

    fn from_wire(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Ready),
            2 => Ok(Self::Request),
            3 => Ok(Self::Snapshot),
            4 => Ok(Self::Identity),
            5 => Ok(Self::Terminal),
            _ => Err(io::Error::new(ErrorKind::InvalidData, "unknown identity frame kind")),
        }
    }
}

#[derive(Eq, PartialEq)]
pub struct Frame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Ready,
    Snapshot,
    Identities,
    Terminal,
    Failed,
}

/// Output ordering and row admission only; a terminal frame is not a successful verdict.
pub struct OutputOrder {
    phase: Phase,
    rows: u64,
    max_rows: u64,
}

impl OutputOrder {
    /// Construct a finite row bound before processing any output.
    ///
    /// # Errors
    /// Rejects zero or unlimited bounds.
    pub fn new(max_rows: u64) -> io::Result<Self> {
        if max_rows == 0 || max_rows == u64::MAX {
            return Err(io::Error::new(ErrorKind::InvalidInput, "invalid identity row limit"));
        }
        Ok(Self {
            phase: Phase::Ready,
            rows: 0,
            max_rows,
        })
    }

    /// Admit the next frame without deduplicating identities.
    ///
    /// # Errors
    /// Rejects unexpected, duplicate, or out-of-order frames and excess rows.
    pub fn accept(&mut self, kind: FrameKind) -> io::Result<()> {
        match (self.phase, kind) {
            (Phase::Ready, FrameKind::Ready) => self.phase = Phase::Snapshot,
            (Phase::Snapshot, FrameKind::Snapshot) => self.phase = Phase::Identities,
            (Phase::Identities, FrameKind::Identity) => {
                if self.rows == self.max_rows {
                    self.phase = Phase::Failed;
                    return Err(io::Error::new(ErrorKind::InvalidData, "identity row budget exhausted"));
                }
                self.rows += 1;
            }
            (Phase::Identities, FrameKind::Terminal) => self.phase = Phase::Terminal,
            _ => {
                self.phase = Phase::Failed;
                return Err(io::Error::new(ErrorKind::InvalidData, "unexpected identity frame"));
            }
        }
        Ok(())
    }

    /// Complete framing after actual EOF, returning only the provisional row count.
    ///
    /// # Errors
    /// Rejects EOF before a terminal frame.
    pub fn finish_eof(self) -> io::Result<u64> {
        if self.phase != Phase::Terminal {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "identity stream ended before terminal frame"));
        }
        Ok(self.rows)
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Frame")
            .field("kind", &self.kind)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Read one frame; EOF is accepted only before the next header begins.
/// Any error terminates the stream; callers must discard partial results.
///
/// # Errors
/// Rejects invalid limits, malformed/truncated frames and exhausted byte budgets.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_payload: usize,
    remaining: &mut u64,
) -> io::Result<Option<Frame>> {
    validate_limit(max_payload)?;
    let mut tag = [0_u8; 1];
    if reader.read(&mut tag).await? == 0 {
        return Ok(None);
    }
    let kind = FrameKind::from_wire(tag[0])?;
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).await?;
    let size = u32::from_be_bytes(length) as usize;
    debit(size, max_payload, remaining)?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(size)
        .map_err(|error| io::Error::other(format!("identity frame allocation failed: {error}")))?;
    payload.resize(size, 0);
    reader.read_exact(&mut payload).await?;
    Ok(Some(Frame { kind, payload }))
}

/// Write a single bounded frame, debiting its complete wire size first.
/// Any error terminates the stream; callers must discard partial results.
///
/// # Errors
/// Rejects invalid limits, oversized frames, exhausted budgets and failed writes.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    max_payload: usize,
    remaining: &mut u64,
) -> io::Result<()> {
    validate_limit(max_payload)?;
    debit(frame.payload.len(), max_payload, remaining)?;
    let size = u32::try_from(frame.payload.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "identity frame length overflow"))?;
    writer.write_all(&[frame.kind.wire()]).await?;
    writer.write_all(&size.to_be_bytes()).await?;
    writer.write_all(&frame.payload).await
}

fn validate_limit(max_payload: usize) -> io::Result<()> {
    if max_payload == 0 || max_payload > MAX_FRAME_BYTES {
        return Err(io::Error::new(ErrorKind::InvalidInput, "invalid identity frame limit"));
    }
    Ok(())
}

fn debit(size: usize, max_payload: usize, remaining: &mut u64) -> io::Result<()> {
    if size > max_payload {
        return Err(io::Error::new(ErrorKind::InvalidData, "identity frame exceeds payload limit"));
    }
    let wire_size = u64::try_from(size)
        .ok()
        .and_then(|size| size.checked_add(HEADER_BYTES))
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "identity frame length overflow"))?;
    *remaining = remaining
        .checked_sub(wire_size)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "identity frame byte budget exhausted"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn preserves_frame_payloads_and_charges_wire_bytes() -> io::Result<()> {
        let (mut output, mut input) = tokio::io::duplex(128);
        let frame = Frame {
            kind: FrameKind::Identity,
            payload: b"repeated".to_vec(),
        };
        let mut written = 26;
        write_frame(&mut output, &frame, 16, &mut written).await?;
        write_frame(&mut output, &frame, 16, &mut written).await?;
        drop(output);
        let mut remaining = 26;
        assert_eq!(read_frame(&mut input, 16, &mut remaining).await?, Some(frame));
        assert_eq!(
            read_frame(&mut input, 16, &mut remaining).await?,
            Some(Frame {
                kind: FrameKind::Identity,
                payload: b"repeated".to_vec(),
            })
        );
        assert!(read_frame(&mut input, 16, &mut remaining).await?.is_none());
        assert_eq!((written, remaining), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_lengths_kinds_and_truncated_frames() -> io::Result<()> {
        for bytes in [
            vec![4, 0, 0, 0, 17],
            vec![9, 0, 0, 0, 0],
            vec![4, 0],
            vec![4, 0, 0, 0, 2, b'x'],
        ] {
            let (mut output, mut input) = tokio::io::duplex(64);
            output.write_all(&bytes).await?;
            drop(output);
            assert!(read_frame(&mut input, 16, &mut 32).await.is_err());
        }
        let (mut output, mut input) = tokio::io::duplex(64);
        output.write_all(&[4, 0, 0, 0, 4]).await?;
        assert!(read_frame(&mut input, 16, &mut 8).await.is_err());
        let mut budget = 32;
        assert!(
            write_frame(
                &mut output,
                &Frame { kind: FrameKind::Identity, payload: vec![0; 17] },
                16,
                &mut budget,
            ).await.is_err()
        );
        assert_eq!(budget, 32);
        Ok(())
    }

    #[test]
    fn frame_debug_redacts_private_payload() {
        let frame = Frame { kind: FrameKind::Request, payload: b"private-token".to_vec() };
        let debug = format!("{frame:?}");
        assert_eq!(debug, "Frame { kind: Request, payload_len: 13 }");
    }

    #[test]
    fn output_order_rejects_missing_extra_and_excess_rows() -> io::Result<()> {
        let mut order = OutputOrder::new(2)?;
        order.accept(FrameKind::Ready)?;
        order.accept(FrameKind::Snapshot)?;
        order.accept(FrameKind::Identity)?;
        order.accept(FrameKind::Identity)?;
        order.accept(FrameKind::Terminal)?;
        assert_eq!(order.finish_eof()?, 2);
        let mut extra = OutputOrder::new(1)?;
        extra.accept(FrameKind::Ready)?;
        extra.accept(FrameKind::Snapshot)?;
        extra.accept(FrameKind::Terminal)?;
        assert!(extra.accept(FrameKind::Identity).is_err());
        assert!(extra.finish_eof().is_err());
        let mut excessive = OutputOrder::new(1)?;
        excessive.accept(FrameKind::Ready)?;
        excessive.accept(FrameKind::Snapshot)?;
        excessive.accept(FrameKind::Identity)?;
        assert!(excessive.accept(FrameKind::Identity).is_err());
        assert!(excessive.accept(FrameKind::Terminal).is_err());
        assert!(excessive.finish_eof().is_err());
        let mut unordered = OutputOrder::new(1)?;
        assert!(unordered.accept(FrameKind::Identity).is_err());
        assert!(unordered.finish_eof().is_err());
        let mut incomplete = OutputOrder::new(1)?;
        incomplete.accept(FrameKind::Ready)?;
        incomplete.accept(FrameKind::Snapshot)?;
        assert!(incomplete.finish_eof().is_err());
        assert!(OutputOrder::new(0).is_err());
        assert!(OutputOrder::new(u64::MAX).is_err());
        Ok(())
    }
}
