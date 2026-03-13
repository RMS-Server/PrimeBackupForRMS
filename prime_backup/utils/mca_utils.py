import dataclasses
import struct
from typing import Dict, Tuple

from prime_backup.types.hash_method import HashMethod

_SECTOR_SIZE = 4096
_HEADER_SECTORS = 2
_HEADER_SIZE = _HEADER_SECTORS * _SECTOR_SIZE
_CHUNKS_PER_REGION = 1024  # 32 * 32

_MANIFEST_MAGIC = b'MCAF'
_MANIFEST_VERSION = 1

ChunkCoord = Tuple[int, int]  # (cx, cz), 0..31


@dataclasses.dataclass(frozen=True)
class McaRegion:
	timestamps: bytes  # 4096 bytes raw timestamp table
	chunks: Dict[ChunkCoord, bytes]  # (cx, cz) -> chunk payload (length prefix + compression + data)


@dataclasses.dataclass(frozen=True)
class McaManifest:
	hash_byte_len: int
	timestamps: bytes
	chunk_hashes: Dict[ChunkCoord, str]  # (cx, cz) -> hex hash string


def _chunk_index(cx: int, cz: int) -> int:
	# Matches Minecraft: 4 * ((x & 31) + (z & 31) * 32)
	return (cz & 31) * 32 + (cx & 31)


def parse_mca(data: bytes) -> McaRegion:
	if len(data) < _HEADER_SIZE:
		raise ValueError(f'MCA data too small: {len(data)} bytes, need >= {_HEADER_SIZE}')

	location_table = data[:_SECTOR_SIZE]
	timestamps = data[_SECTOR_SIZE:_HEADER_SIZE]
	chunks: Dict[ChunkCoord, bytes] = {}

	for cz in range(32):
		for cx in range(32):
			idx = _chunk_index(cx, cz)
			loc_entry = struct.unpack_from('>I', location_table, idx * 4)[0]
			sector_offset = (loc_entry >> 8) & 0xFFFFFF
			sector_count = loc_entry & 0xFF
			if sector_offset == 0 and sector_count == 0:
				continue

			byte_offset = sector_offset * _SECTOR_SIZE
			if byte_offset + 4 > len(data):
				raise ValueError(f'Truncated MCA: chunk ({cx}, {cz}) at offset {byte_offset} exceeds data size {len(data)}')
			chunk_length = struct.unpack_from('>I', data, byte_offset)[0]
			if chunk_length == 0:
				continue

			# payload = 4-byte length prefix + compression byte + compressed data
			payload_end = byte_offset + 4 + chunk_length
			if payload_end > len(data):
				raise ValueError(f'Truncated MCA: chunk ({cx}, {cz}) payload end {payload_end} exceeds data size {len(data)}')
			chunks[(cx, cz)] = data[byte_offset:payload_end]

	return McaRegion(timestamps=timestamps, chunks=chunks)


def encode_manifest(chunk_hashes: Dict[ChunkCoord, str], timestamps: bytes, hash_method: HashMethod) -> bytes:
	hash_byte_len = hash_method.value.hex_length // 2
	parts = [
		_MANIFEST_MAGIC,
		struct.pack('BBB', _MANIFEST_VERSION, hash_byte_len, 0),
		timestamps,
	]

	for cz in range(32):
		for cx in range(32):
			h = chunk_hashes.get((cx, cz))
			if h is None:
				parts.append(b'\x00')
			else:
				parts.append(b'\x01')
				parts.append(bytes.fromhex(h))

	return b''.join(parts)


def decode_manifest(data: bytes) -> McaManifest:
	if len(data) < 7:
		raise ValueError(f'Manifest too small: {len(data)} bytes')
	if data[:4] != _MANIFEST_MAGIC:
		raise ValueError(f'Bad manifest magic: {data[:4]!r}')

	version, hash_byte_len, _flags = struct.unpack_from('BBB', data, 4)
	if version != _MANIFEST_VERSION:
		raise ValueError(f'Unsupported manifest version: {version}')

	off = 7
	timestamps = data[off:off + _SECTOR_SIZE]
	if len(timestamps) != _SECTOR_SIZE:
		raise ValueError(f'Truncated timestamps: got {len(timestamps)} bytes')
	off += _SECTOR_SIZE

	chunk_hashes: Dict[ChunkCoord, str] = {}
	for cz in range(32):
		for cx in range(32):
			if off >= len(data):
				raise ValueError(f'Truncated manifest at chunk ({cx}, {cz})')
			present = data[off]
			off += 1
			if present:
				end = off + hash_byte_len
				if end > len(data):
					raise ValueError(f'Truncated hash at chunk ({cx}, {cz})')
				chunk_hashes[(cx, cz)] = data[off:end].hex()
				off = end

	return McaManifest(hash_byte_len=hash_byte_len, timestamps=timestamps, chunk_hashes=chunk_hashes)


def reconstruct_mca(manifest: McaManifest, chunk_data_map: Dict[str, bytes]) -> bytes:
	location_entries = [0] * _CHUNKS_PER_REGION
	indexed_payloads = []

	for (cx, cz), h in manifest.chunk_hashes.items():
		payload = chunk_data_map.get(h)
		if payload is None:
			raise ValueError(f'Missing chunk data for hash {h} at ({cx}, {cz})')
		indexed_payloads.append((_chunk_index(cx, cz), payload))

	indexed_payloads.sort(key=lambda x: x[0])

	current_sector = _HEADER_SECTORS
	data_parts = []
	for idx, payload in indexed_payloads:
		sector_count = (len(payload) + _SECTOR_SIZE - 1) // _SECTOR_SIZE
		location_entries[idx] = (current_sector << 8) | (sector_count & 0xFF)

		padded_len = sector_count * _SECTOR_SIZE
		data_parts.append(payload + b'\x00' * (padded_len - len(payload)))
		current_sector += sector_count

	location_table = struct.pack('>' + 'I' * _CHUNKS_PER_REGION, *location_entries)
	return location_table + manifest.timestamps + b''.join(data_parts)
