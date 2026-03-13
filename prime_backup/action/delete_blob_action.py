import contextlib
import logging
from typing import List, Dict, Optional, Collection, Set

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.compressors import Compressor
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.exceptions import BlobNotFound
from prime_backup.types.blob_info import BlobInfo, BlobListSummary
from prime_backup.utils import blob_utils, collection_utils, mca_utils


class _BlobTrashBin:
	def __init__(self, logger: logging.Logger):
		self.trash_blobs: List[BlobInfo] = []
		self.logger = logger
		self.errors: List[Exception] = []

	def add(self, blob_info: BlobInfo):
		self.trash_blobs.append(blob_info)

	def make_summary(self) -> BlobListSummary:
		return BlobListSummary.of(self.trash_blobs)

	def erase_all(self):
		for trash in self.trash_blobs:
			try:
				trash.blob_path.unlink(missing_ok=True)
			except Exception as e:
				self.logger.error('Error erasing blob {} at {!r}'.format(trash.hash, trash.blob_path))
				self.errors.append(e)


class DeleteBlobsAction(Action[BlobListSummary]):
	def __init__(self, blob_hashes: List[str], *, raise_if_not_found: bool = True):
		super().__init__()
		self.blob_hashes = blob_hashes
		self.raise_if_not_found = raise_if_not_found

	@override
	def run(self, *, session: Optional[DbSession] = None) -> BlobListSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		trash_bin = _BlobTrashBin(self.logger)
		self_blob_hashes_set = set(self.blob_hashes)

		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			blobs: Dict[str, schema.Blob] = session.get_blobs(self.blob_hashes)
			collected_hashes: List[str] = []
			for blob_hash, blob in blobs.items():
				if blob is None and self.raise_if_not_found:
					raise BlobNotFound(blob_hash)
				else:
					if blob_hash not in self_blob_hashes_set:
						raise AssertionError('got unexpected blob hash {!r}, should be in {}'.format(blob_hash, self_blob_hashes_set))
					collected_hashes.append(blob_hash)
					trash_bin.add(BlobInfo.of(blob))

			session.delete_blobs(self.blob_hashes)
			session.commit()

		s = trash_bin.make_summary()
		trash_bin.erase_all()

		if len(errors := trash_bin.errors) > 0:
			self.logger.error('Found {} blob erasing failure in total'.format(len(errors)))
			raise errors[0]

		return s


class DeleteOrphanBlobsAction(Action[BlobListSummary]):
	def __init__(self, blob_hashes_to_check: Collection[str]):
		super().__init__()
		self.blob_hashes_to_check = collection_utils.deduplicated_list(blob_hashes_to_check)

	@classmethod
	def _collect_manifest_chunk_hashes(cls, session: DbSession, orphan_blob_hashes: List[str]) -> Set[str]:
		"""For orphan manifest blobs, extract the chunk hashes they reference"""
		chunk_hashes: Set[str] = set()
		blobs = session.get_blobs(orphan_blob_hashes)
		for h, blob in blobs.items():
			if blob is None:
				continue
			blob_path = blob_utils.get_blob_path(h)
			if not blob_path.exists():
				continue
			try:
				with Compressor.create(blob.compress).open_decompressed(blob_path) as f:
					data = f.read()
				manifest = mca_utils.decode_manifest(data)
				chunk_hashes.update(manifest.chunk_hashes.values())
			except (ValueError, OSError):
				pass
		return chunk_hashes

	@classmethod
	def _get_alive_manifest_hashes(cls, session: DbSession) -> Set[str]:
		"""Collect unique manifest blob hashes from all alive mca_assembled files"""
		alive_manifest_hashes: Set[str] = set()
		for files in session.iterate_file_batch(batch_size=5000):
			for file in files:
				if file.role == FileRole.mca_assembled.value and file.blob_hash is not None:
					alive_manifest_hashes.add(file.blob_hash)
		return alive_manifest_hashes

	@classmethod
	def _decode_manifest_chunk_hashes(cls, session: DbSession, manifest_hash: str) -> Set[str]:
		"""Decode a manifest blob and return the set of chunk hashes it references"""
		blob = session.get_blob_opt(manifest_hash)
		if blob is None:
			return set()
		blob_path = blob_utils.get_blob_path(blob.hash)
		if not blob_path.exists():
			return set()
		try:
			with Compressor.create(blob.compress).open_decompressed(blob_path) as f:
				data = f.read()
			manifest = mca_utils.decode_manifest(data)
			return set(manifest.chunk_hashes.values())
		except (ValueError, OSError):
			return set()

	@classmethod
	def collect_all_alive_chunk_hashes(cls, session: DbSession) -> Set[str]:
		"""Collect all chunk hashes referenced by any alive manifest. Used by full orphan scan."""
		alive: Set[str] = set()
		for manifest_hash in cls._get_alive_manifest_hashes(session):
			alive.update(cls._decode_manifest_chunk_hashes(session, manifest_hash))
		return alive

	@classmethod
	def _collect_alive_chunk_hashes(cls, session: DbSession, candidate_chunk_hashes: Set[str]) -> Set[str]:
		"""
		Of the candidate chunk hashes, determine which are still referenced
		by alive mca_assembled manifest blobs. Early-exits when all candidates found.
		"""
		if not candidate_chunk_hashes:
			return set()

		alive: Set[str] = set()
		for manifest_hash in cls._get_alive_manifest_hashes(session):
			chunk_set = cls._decode_manifest_chunk_hashes(session, manifest_hash)
			alive.update(candidate_chunk_hashes & chunk_set)
			if alive == candidate_chunk_hashes:
				break
		return alive

	@override
	def run(self, *, session: Optional[DbSession] = None) -> BlobListSummary:
		"""
		:param session: If provided, use this session for DB operations.
		NOTES: `session.commit()` will be called, so it's better to call this at the end of a `DbAccess.open_session()` block
		"""
		with contextlib.ExitStack() as es:
			if session is None:
				session = es.enter_context(DbAccess.open_session())

			orphan_blob_hashes = session.filtered_orphan_blob_hashes(self.blob_hashes_to_check)

			if len(orphan_blob_hashes) > 0:
				orphan_chunk_hashes = self._collect_manifest_chunk_hashes(session, orphan_blob_hashes)

				action = DeleteBlobsAction(orphan_blob_hashes, raise_if_not_found=True)
				bls = action.run(session=session)

				if len(orphan_chunk_hashes) > 0:
					alive_chunk_hashes = self._collect_alive_chunk_hashes(session, orphan_chunk_hashes)
					deletable = orphan_chunk_hashes - alive_chunk_hashes
					if len(deletable) > 0:
						extra_bls = DeleteBlobsAction(list(deletable), raise_if_not_found=False).run(session=session)
						bls = bls + extra_bls
			else:
				bls = BlobListSummary.zero()
				session.commit()

		return bls
