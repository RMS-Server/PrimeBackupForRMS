#!/usr/bin/env python3
"""
Analyze potential savings from chunk-level deduplication on .mca files.

Usage:
    python analyze_mca_dedup.py <pb_files_directory>

Example:
    python analyze_mca_dedup.py ./pb_files

This script reads the PrimeBackup database and blob files directly.
It finds all .mca files with multiple stored versions, parses each
version's chunk layout, and calculates how much space chunk-level
deduplication would save compared to whole-file deduplication.
"""

import hashlib
import io
import os
import sqlite3
import struct
import sys
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Blob decompression
# ---------------------------------------------------------------------------

def _decompress_blob(data: bytes, compress: str) -> bytes:
    if compress == 'plain':
        return data
    if compress == 'zstd':
        try:
            import zstandard
            return zstandard.ZstdDecompressor().decompress(data, max_output_size=256 * 1024 * 1024)
        except ImportError:
            raise RuntimeError("zstandard not installed. Run: pip install zstandard")
    if compress == 'lzma':
        import lzma
        return lzma.decompress(data)
    if compress == 'gzip':
        import gzip
        return gzip.decompress(data)
    if compress == 'lz4':
        try:
            import lz4.frame
            return lz4.frame.decompress(data)
        except ImportError:
            raise RuntimeError("lz4 not installed. Run: pip install lz4")
    raise ValueError(f"Unknown compress method: {compress!r}")


def read_blob(blobs_dir: Path, blob_hash: str, compress: str) -> Optional[bytes]:
    blob_path = blobs_dir / blob_hash[:2] / blob_hash
    if not blob_path.exists():
        return None
    try:
        raw = blob_path.read_bytes()
        return _decompress_blob(raw, compress)
    except Exception as e:
        print(f"  [warn] Failed to read blob {blob_hash[:8]}...: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# .mca region file parser
# ---------------------------------------------------------------------------

def parse_mca_chunks(mca_data: bytes) -> Dict[Tuple[int, int], bytes]:
    """
    Parse a Minecraft Anvil region file and return the raw chunk payloads.

    Returns a dict mapping (chunk_x, chunk_z) relative coords (0-31) to
    the raw bytes stored in the region file for that chunk.  These bytes
    include the compression-type byte followed by the compressed NBT data,
    exactly as Minecraft wrote them.  Hashing these bytes gives a stable
    fingerprint: if two snapshots produce identical bytes for a chunk,
    the chunk content (and its compression artefacts) are identical.
    """
    if len(mca_data) < 8192:
        return {}

    chunks: Dict[Tuple[int, int], bytes] = {}

    for idx in range(1024):
        cx = idx % 32
        cz = idx // 32

        # Location table entry: 3-byte sector offset + 1-byte sector count.
        entry = struct.unpack_from('>I', mca_data, idx * 4)[0]
        sector_offset = (entry >> 8) & 0xFFFFFF
        sector_count  = entry & 0xFF

        if sector_offset == 0 and sector_count == 0:
            continue  # chunk not present

        byte_offset = sector_offset * 4096
        if byte_offset + 5 > len(mca_data):
            continue

        # 4-byte big-endian length (counts the compression-type byte too).
        length = struct.unpack_from('>I', mca_data, byte_offset)[0]
        if length < 1 or byte_offset + 4 + length > len(mca_data):
            continue

        # Payload: [1-byte compression type][compressed NBT data].
        payload = mca_data[byte_offset + 4: byte_offset + 4 + length]
        chunks[(cx, cz)] = payload

    return chunks


def chunk_fingerprint(payload: bytes) -> str:
    """SHA-1 digest of a chunk payload (fast enough, no collision risk here)."""
    return hashlib.sha1(payload).hexdigest()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def load_mca_blobs(db_path: Path) -> Dict[str, List[Tuple[str, str, int]]]:
    """
    Query the database for all distinct .mca blob versions.

    Returns:
        {relative_path: [(blob_hash, compress, stored_size), ...]}
        Only paths that have >= 2 distinct blob hashes are included.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        # role values: 1=standalone, 2=delta_override, 3=delta_add
        cur.execute("""
            SELECT DISTINCT f.path, f.blob_hash, f.blob_compress, f.blob_stored_size
            FROM file f
            WHERE f.path LIKE '%.mca'
              AND f.blob_hash IS NOT NULL
              AND f.role IN (1, 2, 3)
            ORDER BY f.path, f.blob_hash
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    path_versions: Dict[str, List[Tuple[str, str, int]]] = defaultdict(list)
    for path, blob_hash, compress, stored_size in rows:
        path_versions[path].append((blob_hash, compress or 'plain', stored_size or 0))

    # Keep only paths with multiple distinct blobs (files that actually changed).
    return {p: v for p, v in path_versions.items() if len(v) > 1}


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def format_bytes(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def analyze_path(
    path: str,
    versions: List[Tuple[str, str, int]],
    blobs_dir: Path,
    verbose: bool,
) -> Tuple[int, int, int]:
    """
    Analyze one .mca path across all its stored versions.

    Returns (current_stored_bytes, chunk_dedup_bytes, version_count).
    chunk_dedup_bytes is a lower bound: each unique chunk payload stored once,
    uncompressed (real savings would be similar since chunks are already zlib'd
    internally and PrimeBackup just re-wraps them).
    """
    current_stored = sum(sz for _, _, sz in versions)

    # Collect every unique chunk payload across all versions.
    unique_chunks: Dict[str, int] = {}  # fingerprint -> payload size

    for blob_hash, compress, _ in versions:
        mca_data = read_blob(blobs_dir, blob_hash, compress)
        if mca_data is None:
            continue

        chunks = parse_mca_chunks(mca_data)
        for (cx, cz), payload in chunks.items():
            fp = chunk_fingerprint(payload)
            unique_chunks[fp] = len(payload)

    # Estimate storage with chunk-level dedup: sum of unique chunk sizes.
    # This is raw (uncompressed) chunk data; real stored size would be similar
    # because Minecraft's zlib chunks compress poorly with a second pass.
    chunk_dedup_estimate = sum(unique_chunks.values())

    if verbose:
        changed = sum(
            1 for fp, sz in unique_chunks.items()
        )
        print(f"  {path}")
        print(f"    versions       : {len(versions)}")
        print(f"    current stored : {format_bytes(current_stored)}")
        print(f"    unique chunks  : {len(unique_chunks)}")
        print(f"    chunk dedup est: {format_bytes(chunk_dedup_estimate)}")
        savings = current_stored - chunk_dedup_estimate
        if current_stored > 0:
            pct = 100.0 * savings / current_stored
            print(f"    potential save : {format_bytes(savings)} ({pct:.0f}%)")
        print()

    return current_stored, chunk_dedup_estimate, len(versions)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <pb_files_directory>")
        sys.exit(1)

    storage_root = Path(sys.argv[1])
    db_path = storage_root / 'prime_backup.db'
    blobs_dir = storage_root / 'blobs'

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)
    if not blobs_dir.is_dir():
        print(f"ERROR: Blobs directory not found at {blobs_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading .mca blob versions from database …")
    changed = load_mca_blobs(db_path)
    print(f"  {len(changed)} .mca paths with multiple versions found\n")

    if not changed:
        print("Nothing to analyze. Either no backups contain .mca files,")
        print("or all .mca files are identical across all backups.")
        return

    print("=" * 60)
    print("Per-file analysis (verbose, all changed .mca paths)")
    print("=" * 60 + "\n")

    total_current = 0
    total_chunk_dedup = 0
    total_versions = 0

    for path, versions in sorted(changed.items()):
        cur, dedup, nver = analyze_path(path, versions, blobs_dir, verbose=True)
        total_current    += cur
        total_chunk_dedup += dedup
        total_versions    += nver

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Changed .mca paths analysed : {len(changed)}")
    print(f"  Total version blobs          : {total_versions}")
    print(f"  Current stored size          : {format_bytes(total_current)}")
    print(f"  Estimated with chunk dedup   : {format_bytes(total_chunk_dedup)}")
    savings = total_current - total_chunk_dedup
    if total_current > 0:
        pct = 100.0 * savings / total_current
        print(f"  Potential savings            : {format_bytes(savings)} ({pct:.0f}%)")
    print()
    print("Note: 'current stored' counts only blobs for changed .mca files.")
    print("Unchanged .mca files already achieve full dedup via whole-file hash.")


if __name__ == '__main__':
    main()
