import shutil
import time
from typing import List, Dict, Set, Tuple

from typing_extensions import override

from prime_backup.action import Action
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.exceptions import PrimeBackupError
from prime_backup.types.hash_method import HashMethod
from prime_backup.utils import blob_utils, hash_utils, collection_utils, mca_utils


class HashCollisionError(PrimeBackupError):
	"""
	Same hash value, between 2 hash methods
	"""
	pass


class MigrateHashMethodAction(Action[None]):
	def __init__(self, new_hash_method: HashMethod):
		super().__init__()
		self.new_hash_method = new_hash_method

	def __migrate_blobs(self, session: DbSession, blob_hashes: List[str], old_hashes: Set[str], processed_hash_mapping: Dict[str, str]):
		hash_mapping: Dict[str, str] = {}
		blobs = list(session.get_blobs(blob_hashes).values())

		# calc blob hashes
		for blob in blobs:
			blob_path = blob_utils.get_blob_path(blob.hash)
			with Compressor.create(blob.compress).open_decompressed(blob_path) as f:
				sah = hash_utils.calc_reader_size_and_hash(f, hash_method=self.new_hash_method)
			hash_mapping[blob.hash] = sah.hash
			if sah.hash in old_hashes:
				raise HashCollisionError(sah.hash)

		# update the objects
		for blob in blobs:
			old_hash, new_hash = blob.hash, hash_mapping[blob.hash]
			old_path = blob_utils.get_blob_path(old_hash)
			new_path = blob_utils.get_blob_path(new_hash)
			try:
				shutil.move(old_path, new_path)
			except Exception as e:
				self.logger.error('Move blob ({} -> {}) from {!r} to {!r} failed: {}'.format(old_hash, new_hash, old_path, new_path, e))
				raise

			processed_hash_mapping[old_hash] = new_hash
			blob.hash = new_hash

		for file in session.get_file_by_blob_hashes(list(hash_mapping.keys())):
			file.blob_hash = hash_mapping[file.blob_hash]

	def __rewrite_manifest_blobs(self, session: DbSession, processed_hash_mapping: Dict[str, str]) -> List[Tuple[str, str]]:
		"""
		After all blobs are rehashed, manifest blobs still contain old chunk hashes
		in their payload. Rewrite them with remapped chunk hashes and new hash method.

		Returns list of (intermediate_hash, final_hash) for post-commit cleanup.
		"""
		manifest_hashes: Set[str] = set()
		for files in session.iterate_file_batch(batch_size=5000):
			for file in files:
				if file.role == FileRole.mca_assembled.value and file.blob_hash is not None:
					manifest_hashes.add(file.blob_hash)

		if not manifest_hashes:
			return []

		self.logger.info('Rewriting {} manifest blobs for hash method migration'.format(len(manifest_hashes)))
		fixups: List[Tuple[str, str]] = []

		for inter_hash in list(manifest_hashes):
			blob = session.get_blob_opt(inter_hash)
			if blob is None:
				continue

			blob_path = blob_utils.get_blob_path(blob.hash)
			compress = CompressMethod[blob.compress]
			with Compressor.create(compress).open_decompressed(blob_path) as f:
				manifest_data = f.read()

			manifest = mca_utils.decode_manifest(manifest_data)

			new_chunk_hashes = {}
			for coord, chunk_hash in manifest.chunk_hashes.items():
				new_chunk_hashes[coord] = processed_hash_mapping.get(chunk_hash, chunk_hash)

			new_manifest_data = mca_utils.encode_manifest(new_chunk_hashes, manifest.timestamps, self.new_hash_method)
			final_hash = hash_utils.calc_bytes_hash(new_manifest_data, hash_method=self.new_hash_method)

			if final_hash == inter_hash:
				continue

			new_blob_path = blob_utils.get_blob_path(final_hash)
			compressor = Compressor.create(compress)
			with compressor.open_compressed_bypassed(new_blob_path) as (writer, f):
				f.write(new_manifest_data)

			session.delete_blob(blob)
			session.create_and_add_blob(
				hash=final_hash,
				compress=compress.name,
				raw_size=len(new_manifest_data),
				stored_size=writer.get_write_len(),
			)

			for file in session.get_file_by_blob_hashes([inter_hash]):
				file.blob_hash = final_hash
				file.blob_compress = compress.name
				file.blob_raw_size = len(new_manifest_data)
				file.blob_stored_size = writer.get_write_len()

			fixups.append((inter_hash, final_hash))

		return fixups

	@override
	def run(self) -> None:
		processed_hash_mapping: Dict[str, str] = {}  # old -> new
		manifest_fixups: List[Tuple[str, str]] = []
		try:
			t = time.time()
			with DbAccess.open_session() as session:
				meta = session.get_db_meta()
				if meta.hash_method == self.new_hash_method.name:
					self.logger.info('Hash method of the database is already {}, no need to migrate'.format(self.new_hash_method.name))
					return

				self.logger.info('Migrating hash method from {} to {}'.format(meta.hash_method, self.new_hash_method.name))

				total_blob_count = session.get_blob_count()
				all_hashes = list(session.iter_all_blob_hashes())
				all_hash_set = set(all_hashes)
				cnt = 0
				for blob_hashes in collection_utils.slicing_iterate(all_hashes, 1000):
					blob_hashes: List[str] = list(blob_hashes)
					cnt += len(blob_hashes)
					self.logger.info('Migrating blobs {} / {}'.format(cnt, total_blob_count))

					self.__migrate_blobs(session, blob_hashes, all_hash_set, processed_hash_mapping)
					session.flush_and_expunge_all()

				manifest_fixups = self.__rewrite_manifest_blobs(session, processed_hash_mapping)
				session.flush_and_expunge_all()

				meta = session.get_db_meta()  # get the meta again, cuz expunge_all() was called
				meta.hash_method = self.new_hash_method.name

			# commit succeeded, clean up intermediate manifest files
			for inter_hash, _final_hash in manifest_fixups:
				blob_utils.get_blob_path(inter_hash).unlink(missing_ok=True)

			self.logger.info('Syncing config and variables')
			DbAccess.sync_hash_method()
			self.config.backup.hash_method = self.new_hash_method.name

			self.logger.info('Hash method migration done, cost {}s'.format(round(time.time() - t, 2)))

		except Exception:
			self.logger.warning('Error occurs during migration, applying rollback')
			for _inter_hash, final_hash in manifest_fixups:
				blob_utils.get_blob_path(final_hash).unlink(missing_ok=True)
			for old_hash, new_hash in processed_hash_mapping.items():
				old_path = blob_utils.get_blob_path(old_hash)
				new_path = blob_utils.get_blob_path(new_hash)
				shutil.move(new_path, old_path)
			raise
