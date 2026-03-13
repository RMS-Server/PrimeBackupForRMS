import dataclasses
import time
from collections import deque
from typing import List, Dict, Optional, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.types.size_diff import SizeDiff
from prime_backup.utils import blob_utils, hash_utils, mca_utils


@dataclasses.dataclass
class RepackMcaProgress:
	total: int = 0
	done: int = 0
	skipped: int = 0
	failed: int = 0
	current_path: str = ''

	@property
	def percent(self) -> float:
		return (self.done / self.total * 100) if self.total > 0 else 0.0


class RepackMcaAction(Action[SizeDiff]):
	BATCH_SIZE = 50

	def __init__(self, *, dry_run: bool = False):
		super().__init__()
		self.dry_run = dry_run
		self.progress = RepackMcaProgress()
		self.__affected_fileset_ids: Set[int] = set()
		self.__recent_times: deque = deque(maxlen=10)

	def __estimate_eta(self) -> Optional[float]:
		if len(self.__recent_times) < 2:
			return None
		avg = sum(self.__recent_times) / len(self.__recent_times)
		remaining = self.progress.total - self.progress.done
		return avg * remaining

	def __format_eta(self) -> str:
		eta = self.__estimate_eta()
		if eta is None:
			return '?'
		minutes, seconds = divmod(int(eta), 60)
		if minutes > 0:
			return f'{minutes}m{seconds:02d}s'
		return f'{seconds}s'

	def __scan_mca_blob_hashes(self, session: DbSession) -> List[str]:
		"""Find all unique blob hashes referenced by non-mca_assembled .mca files"""
		result_hashes: Set[str] = set()

		non_mca_roles = {FileRole.mca_assembled.value, FileRole.delta_remove.value}
		for files in session.iterate_file_batch(batch_size=5000):
			for file in files:
				if (
					file.role not in non_mca_roles and
					file.blob_hash is not None and
					file.path.endswith('.mca') and
					file.blob_hash not in result_hashes
				):
					result_hashes.add(file.blob_hash)

		self.logger.info(f'Found {len(result_hashes)} unique standalone .mca blob hashes')
		return list(result_hashes)

	def __find_sample_path(self, session: DbSession, blob_hash: str) -> str:
		"""Get a sample file path for progress display"""
		files = session.get_file_by_blob_hashes([blob_hash], limit=1)
		return files[0].path if files else '?'

	def __repack_one_blob(self, session: DbSession, blob_hash: str) -> bool:
		"""Repack a single standalone .mca blob into chunk blobs + manifest. Returns True on success."""
		blob = session.get_blob(blob_hash)
		blob_path = blob_utils.get_blob_path(blob.hash)
		with Compressor.create(blob.compress).open_decompressed(blob_path) as f:
			mca_data = f.read()

		region = mca_utils.parse_mca(mca_data)
		hash_method = DbAccess.get_hash_method()

		chunk_hashes: Dict[mca_utils.ChunkCoord, str] = {}
		chunk_payloads: Dict[str, bytes] = {}
		for coord, payload in region.chunks.items():
			h = hash_utils.calc_bytes_hash(payload)
			chunk_hashes[coord] = h
			if h not in chunk_payloads:
				chunk_payloads[h] = payload

		existing_blobs = session.get_blobs(list(chunk_payloads.keys()))
		for h, payload in chunk_payloads.items():
			if existing_blobs.get(h) is not None:
				continue
			chunk_blob_path = blob_utils.get_blob_path(h)
			with open(chunk_blob_path, 'wb') as f:
				f.write(payload)
			session.create_and_add_blob(hash=h, compress=CompressMethod.plain.name, raw_size=len(payload), stored_size=len(payload))

		manifest_data = mca_utils.encode_manifest(chunk_hashes, region.timestamps, hash_method)
		manifest_hash = hash_utils.calc_bytes_hash(manifest_data)

		manifest_blob = session.get_blob_opt(manifest_hash)
		if manifest_blob is None:
			compress_method = self.config.backup.get_compress_method_from_size(len(manifest_data))
			compressor = Compressor.create(compress_method)
			manifest_blob_path = blob_utils.get_blob_path(manifest_hash)
			with compressor.open_compressed_bypassed(manifest_blob_path) as (writer, f):
				f.write(manifest_data)
			manifest_blob = session.create_and_add_blob(
				hash=manifest_hash,
				compress=compress_method.name,
				raw_size=len(manifest_data),
				stored_size=writer.get_write_len(),
			)

		files_updated = 0
		for file in session.get_file_by_blob_hashes([blob.hash]):
			if file.path.endswith('.mca') and file.role != FileRole.mca_assembled.value:
				file.blob_hash = manifest_blob.hash
				file.blob_compress = manifest_blob.compress
				file.blob_raw_size = manifest_blob.raw_size
				file.blob_stored_size = manifest_blob.stored_size
				file.role = FileRole.mca_assembled.value
				self.__affected_fileset_ids.add(file.fileset_id)
				files_updated += 1

		old_blob_path = blob_utils.get_blob_path(blob.hash)
		if not session.has_file_with_hash(blob.hash):
			old_blob_path.unlink(missing_ok=True)
			session.delete_blob(blob)

		self.logger.debug(f'Repacked blob {blob.hash[:12]}.. -> manifest {manifest_hash[:12]}.. ({len(chunk_payloads)} chunks, {files_updated} files updated)')
		return True

	def __update_fileset_and_backups(self, session: DbSession):
		if not self.__affected_fileset_ids:
			return

		fileset_ids = list(self.__affected_fileset_ids)
		backup_ids = session.get_backup_ids_by_fileset_ids(fileset_ids)
		self.logger.info(f'Syncing {len(fileset_ids)} affected filesets and {len(backup_ids)} associated backups')

		filesets = session.get_filesets(fileset_ids)
		for fileset in filesets.values():
			fileset.file_raw_size_sum = session.calc_file_raw_size_sum(fileset.id)
			fileset.file_stored_size_sum = session.calc_file_stored_size_sum(fileset.id)

		all_backup_fileset_ids: Set[int] = set()
		backups = list(session.get_backups(backup_ids).values())
		for backup in backups:
			all_backup_fileset_ids.add(backup.fileset_id_base)
			all_backup_fileset_ids.add(backup.fileset_id_delta)
		more_filesets = session.get_filesets(list(all_backup_fileset_ids.difference(set(fileset_ids))))
		filesets.update(more_filesets)

		for backup in backups:
			fs_base = filesets[backup.fileset_id_base]
			fs_delta = filesets[backup.fileset_id_delta]
			backup.file_raw_size_sum = fs_base.file_raw_size_sum + fs_delta.file_raw_size_sum
			backup.file_stored_size_sum = fs_base.file_stored_size_sum + fs_delta.file_stored_size_sum

	@override
	def is_interruptable(self) -> bool:
		return True

	@override
	def run(self) -> SizeDiff:
		self.__affected_fileset_ids.clear()
		self.__recent_times.clear()

		with DbAccess.open_session() as session:
			before_size = session.get_blob_stored_size_sum()
			mca_blob_hashes = self.__scan_mca_blob_hashes(session)

		self.progress.total = len(mca_blob_hashes)
		if self.progress.total == 0:
			self.logger.info('No standalone .mca blobs found, nothing to repack')
			return SizeDiff(0, 0)

		self.logger.info(f'Found {self.progress.total} standalone .mca blobs to repack')
		if self.dry_run:
			self.logger.info('[dry-run] Would repack {} blobs'.format(self.progress.total))
			return SizeDiff(0, 0)

		with DbAccess.open_session() as session:
			for blob_hash in mca_blob_hashes:
				if self.is_interrupted.is_set():
					self.logger.info('Repack interrupted')
					break

				self.progress.current_path = self.__find_sample_path(session, blob_hash)
				t0 = time.monotonic()
				try:
					self.__repack_one_blob(session, blob_hash)
				except Exception as e:
					self.logger.warning(f'Failed to repack blob {blob_hash[:12]}..: {e}')
					self.progress.failed += 1
				else:
					self.progress.done += 1
				elapsed = time.monotonic() - t0
				self.__recent_times.append(elapsed)

				if (self.progress.done + self.progress.failed) % 10 == 0 or self.progress.done + self.progress.failed == self.progress.total:
					self.logger.info(
						'Repacking blob {}/{} ({:.1f}%) | {} | ETA: {}'.format(
							self.progress.done + self.progress.failed, self.progress.total,
							self.progress.percent, self.progress.current_path, self.__format_eta(),
						)
					)

				session.flush_and_expunge_all()

			self.__update_fileset_and_backups(session)
			after_size = session.get_blob_stored_size_sum()

		self.logger.info(f'Repack done: {self.progress.done} repacked, {self.progress.skipped} skipped, {self.progress.failed} failed')
		return SizeDiff(before_size, after_size)


def _slicing_iterate(lst: list, size: int):
	for i in range(0, len(lst), size):
		yield lst[i:i + size]
