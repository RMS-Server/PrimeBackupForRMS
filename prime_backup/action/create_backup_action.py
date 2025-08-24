import collections
import contextlib
import dataclasses
import enum
import functools
import hashlib
import logging
import os
import stat
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple, Callable, Any, Dict, Generator, Union, Set, Deque, Literal, BinaryIO, ContextManager, overload, TypeVar

import pathspec
from typing_extensions import NoReturn, override, Self

from prime_backup.action.create_backup_action_base import CreateBackupActionBase
from prime_backup.compressors import Compressor, CompressMethod
from prime_backup.db import schema
from prime_backup.db.access import DbAccess
from prime_backup.db.session import DbSession
from prime_backup.db.values import FileRole
from prime_backup.exceptions import PrimeBackupError, UnsupportedFileFormat
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTags
from prime_backup.types.operator import Operator
from prime_backup.types.units import ByteCount
from prime_backup.utils import hash_utils, misc_utils, blob_utils, file_utils, sqlalchemy_utils
from prime_backup.utils.path_like import PathLike
from prime_backup.utils.thread_pool import FailFastBlockingThreadPool
from prime_backup.utils.time_cost_stats import TimeCostStats


class VolatileBlobFile(PrimeBackupError):
	pass


class _BlobFileChanged(PrimeBackupError):
	pass


class _SourceFileNotFound(FileNotFoundError):
	def __init__(self, e: FileNotFoundError, file_path: Path):
		super().__init__(e)
		self.file_path = file_path

	@classmethod
	@contextlib.contextmanager
	def wrap(cls, path: Path) -> Generator[None, None, None]:
		try:
			yield
		except FileNotFoundError as e:
			raise cls(e, path)

	@classmethod
	def open_rb(cls, path: PathLike, flag: Literal['rb']) -> BinaryIO:
		if flag != 'rb':
			raise ValueError('flag should be rb')
		with cls.wrap(path):
			return open(path, 'rb')


class _BlobCreatePolicy(enum.Enum):
	"""
	the policy of how to create a blob from a given file path
	"""
	read_all = enum.auto()   # small files: read all in memory, calc hash                                |  read 1x, write 1x
	hash_once = enum.auto()  # files with unique size: compress+hash to temp file, then move             |  read 1x, write 1x, move 1x
	copy_hash = enum.auto()  # files that keep changing: copy to temp file, calc hash, compress to blob  |  read 2x, write 2x. need more spaces
	default = enum.auto()    # default policy: compress+hash to blob store, check hash again             |  read 2x, write 1x


_BLOB_FILE_CHANGED_RETRY_COUNT = 3
_READ_ALL_SIZE_THRESHOLD = 8 * 1024  # 8KiB
_HASH_ONCE_SIZE_THRESHOLD = 10 * 1024 * 1024  # 10MiB


class _TimeCostKey(enum.Enum):
	kind_db = enum.auto()
	kind_fs = enum.auto()
	kind_io_read = enum.auto()
	kind_io_write = enum.auto()
	kind_io_copy = enum.auto()

	stage_scan_files = enum.auto()
	stage_reuse_unchanged_files = enum.auto()
	stage_pre_calculate_hash = enum.auto()
	stage_prepare_blob_store = enum.auto()
	stage_create_files = enum.auto()
	stage_finalize = enum.auto()
	stage_flush_db = enum.auto()

	def __lt__(self, other: Self) -> bool:
		return self.name < other.name


class BatchFetcherBase(ABC):
	Callback = Callable
	tasks: dict

	def __init__(self, session: DbSession, max_batch_size: int, time_costs: TimeCostStats[_TimeCostKey]):
		self.session = session
		self.max_batch_size = max_batch_size
		self.first_task_scheduled_time = time.time()
		self.time_costs = time_costs

	def _post_query(self):
		now = time.time()
		if len(self.tasks) == 1:
			self.first_task_scheduled_time = now
		self.flush_if_needed()

	def flush_if_needed(self):
		if len(self.tasks) > 0 and (len(self.tasks) >= self.max_batch_size or time.time() - self.first_task_scheduled_time >= 0.1):
			self._batch_run()

	def flush(self):
		if len(self.tasks) > 0:
			self._batch_run()

	@abstractmethod
	def _batch_run(self):
		...


class BlobBySizeFetcher(BatchFetcherBase):
	@dataclasses.dataclass(frozen=True)
	class Req:
		size: int

	@dataclasses.dataclass(frozen=True)
	class Rsp:
		exists: bool

	Callback = Callable[[Rsp], None]
	tasks: Dict[int, List[Callback]]

	def __init__(self, session: DbSession, max_batch_size: int, result_cache: Dict[int, bool], time_costs: TimeCostStats[_TimeCostKey]):
		super().__init__(session, max_batch_size, time_costs)
		self.tasks: List[Tuple[int, BlobBySizeFetcher.Callback]] = []
		self.sizes: Set[int] = set()
		self.result_cache = result_cache

	def query(self, query: Req, callback: Callback):
		self.tasks.append((query.size, callback))
		self.sizes.add(query.size)
		self._post_query()

	@override
	def _batch_run(self):
		with self.time_costs.measure_time_cost(_TimeCostKey.kind_db):
			existence = self.session.has_blob_with_size_batched(list(self.sizes))
		self.result_cache.update(existence)
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for sz, callback in reversed(self.tasks):
			callback(self.Rsp(existence[sz]))
		self.tasks.clear()
		self.sizes.clear()


class BlobByHashFetcher(BatchFetcherBase):
	@dataclasses.dataclass(frozen=True)
	class Req:
		hash: str

	@dataclasses.dataclass(frozen=True)
	class Rsp:
		blob: Optional[schema.Blob]

	Callback = Callable[[Rsp], None]
	tasks: Dict[str, List[Callback]]

	def __init__(self, session: DbSession, max_batch_size: int, result_cache: Dict[str, schema.Blob], time_costs: TimeCostStats[_TimeCostKey]):
		super().__init__(session, max_batch_size, time_costs)
		self.tasks: List[Tuple[str, BlobByHashFetcher.Callback]] = []
		self.hashes: Set[str] = set()
		self.result_cache = result_cache

	def query(self, query: Req, callback: Callback):
		self.tasks.append((query.hash, callback))
		self.hashes.add(query.hash)
		self._post_query()

	@override
	def _batch_run(self):
		with self.time_costs.measure_time_cost(_TimeCostKey.kind_db):
			blobs = self.session.get_blobs(list(self.hashes))
		self.result_cache.update(blobs)
		# reverse since we want to keep the file order, and collections.deque.appendleft is FILO
		for h, callback in reversed(self.tasks):
			callback(self.Rsp(blobs[h]))
		self.tasks.clear()
		self.hashes.clear()


BqmReq = Union[BlobBySizeFetcher.Req, BlobByHashFetcher.Req]
BqmRsp = Union[BlobBySizeFetcher.Rsp, BlobByHashFetcher.Rsp]


class BatchQueryManager:
	def __init__(self, session: DbSession, size_result_cache: dict, hash_result_cache: dict, time_costs: TimeCostStats[_TimeCostKey], *, max_batch_size: int = 100):
		self.fetcher_size = BlobBySizeFetcher(session, max_batch_size, size_result_cache, time_costs)
		self.fetcher_hash = BlobByHashFetcher(session, max_batch_size, hash_result_cache, time_costs)

	@overload
	def query(self, query: BlobBySizeFetcher.Req, callback: Callable[[BlobBySizeFetcher.Rsp], None]): ...
	@overload
	def query(self, query: BlobByHashFetcher.Req, callback: Callable[[BlobByHashFetcher.Rsp], None]): ...

	def query(self,query: BqmReq, callback: Callable[[BqmRsp], None]):
		if isinstance(query, BlobBySizeFetcher.Req):
			self.fetcher_size.query(query, callback)
		elif isinstance(query, BlobByHashFetcher.Req):
			self.fetcher_hash.query(query, callback)
		else:
			raise TypeError('unexpected query: {!r} {!r}'.format(type(query), query))

	def flush_if_needed(self):
		self.fetcher_size.flush_if_needed()
		self.fetcher_hash.flush_if_needed()

	def flush(self):
		self.fetcher_size.flush()
		self.fetcher_hash.flush()


@dataclasses.dataclass(frozen=True)
class _ScanResultEntry:
	path: Path  # full path, including source_root
	stat: os.stat_result

	def is_file(self) -> bool:
		return stat.S_ISREG(self.stat.st_mode)

	def is_dir(self) -> bool:
		return stat.S_ISDIR(self.stat.st_mode)

	def is_symlink(self) -> bool:
		return stat.S_ISLNK(self.stat.st_mode)


@dataclasses.dataclass(frozen=True)
class _ScanResult:
	all_files: List[_ScanResultEntry] = dataclasses.field(default_factory=list)
	root_targets: List[str] = dataclasses.field(default_factory=list)  # list of posix path, related to the source_path


@dataclasses.dataclass(frozen=True)
class _PreCalculationResult:
	stats: Dict[Path, os.stat_result] = dataclasses.field(default_factory=dict)
	hashes: Dict[Path, str] = dataclasses.field(default_factory=dict)
	reused_files: Dict[Path, schema.File] = dataclasses.field(default_factory=dict)


class CreateBackupAction(CreateBackupActionBase):
	def __init__(self, creator: Operator, comment: str, *, tags: Optional[BackupTags] = None, source_path: Optional[Path] = None):
		super().__init__()
		if tags is None:
			tags = BackupTags()

		self.creator = creator
		self.comment = comment
		self.tags = tags

		self.__pre_calc_result = _PreCalculationResult()
		self.__blob_store_st: Optional[os.stat_result] = None
		self.__blob_store_in_cow_fs: Optional[bool] = None

		self.__batch_query_manager: Optional[BatchQueryManager] = None
		self.__blob_by_size_cache: Dict[int, bool] = {}
		self.__blob_by_hash_cache: Dict[str, schema.Blob] = {}

		self.__source_path: Path = source_path or self.config.source_path
		self.__time_costs: TimeCostStats[_TimeCostKey] = TimeCostStats()

	def __file_path_to_db_path(self, path: Path) -> str:
		return path.relative_to(self.__source_path).as_posix()

	def __scan_files(self) -> _ScanResult:
		ignore_patterns = pathspec.GitIgnoreSpec.from_lines(self.config.backup.ignore_patterns)
		result = _ScanResult()
		visited_path: Set[Path] = set()  # full path
		ignored_paths: List[Path] = []   # related path

		def scan(full_path: Path, is_root_target: bool):
			try:
				rel_path = full_path.relative_to(self.__source_path)
			except ValueError:
				self.logger.warning("Skipping backup path {!r} cuz it's not inside the source path {!r}".format(str(full_path), str(self.__source_path)))
				return

			if ignore_patterns.match_file(rel_path) or self.config.backup.is_file_ignore_by_deprecated_ignored_files(rel_path.name):
				ignored_paths.append(rel_path)
				if is_root_target:
					self.logger.warning('Backup target {!r} is ignored by config'.format(str(rel_path)))
				return

			if full_path in visited_path:
				return
			visited_path.add(full_path)

			try:
				st = full_path.lstat()
			except FileNotFoundError:
				if is_root_target:
					self.logger.warning('Backup target {!r} does not exist, skipped. full_path: {!r}'.format(str(rel_path), str(full_path)))
				return

			entry = _ScanResultEntry(full_path, st)
			result.all_files.append(entry)
			if is_root_target:
				result.root_targets.append(rel_path.as_posix())

			if entry.is_dir():
				for child in os.listdir(full_path):
					scan(full_path / child, False)
			elif is_root_target and entry.is_symlink() and self.config.backup.follow_target_symlink:
				symlink_target = full_path.readlink()
				symlink_target_full_path = self.__source_path / symlink_target
				self.logger.info('Following root symlink target {!r} -> {!r} ({!r})'.format(str(rel_path), str(symlink_target), str(symlink_target_full_path)))
				scan(symlink_target_full_path, True)

		self.logger.debug(f'Scan file start, target patterns: {self.config.backup.targets}')
		with self.__time_costs.measure_time_cost(_TimeCostKey.kind_fs) as scan_cost:
			target_patterns = pathspec.GitIgnoreSpec.from_lines(self.config.backup.targets)
			target_paths: List[Path] = []
			for candidate_target_name in sorted(os.listdir(self.__source_path)):
				candidate_target_path = self.__source_path / candidate_target_name
				if target_patterns.match_file(candidate_target_path):
					target_paths.append(candidate_target_path)

			self.logger.debug(f'Scan file found {len(target_paths)} targets, {target_paths[:10]=}')
			for target_path in target_paths:
				scan(target_path, True)

		self.logger.debug('Scan file done, cost {:.2f}s, count {}, root_targets (len={}): {}, ignored_paths[:100] (len={}): {}'.format(
			scan_cost(), len(result.all_files),
			len(result.root_targets), result.root_targets,
			len(ignored_paths), [p.as_posix() for p in ignored_paths][:100],
		))
		return result

	def __pre_calculate_stats(self, scan_result: _ScanResult):
		stats = self.__pre_calc_result.stats
		stats.clear()
		for file_entry in scan_result.all_files:
			stats[file_entry.path] = file_entry.stat

	def __reuse_unchanged_files(self, session: DbSession, scan_result: _ScanResult):
		with self.__time_costs.measure_time_cost(_TimeCostKey.kind_db):
			backup = session.get_last_backup()
		if backup is None:
			return

		@dataclasses.dataclass(frozen=True)
		class StatKey:
			path: str
			size: Optional[int]  # it shouldn't be None, but just in case
			mode: int
			uid: int
			gid: int
			mtime_us: int

		with self.__time_costs.measure_time_cost(_TimeCostKey.kind_db):
			backup_files = session.get_backup_files(backup.id)

		stat_to_files: Dict[StatKey, schema.File] = {}
		for file in backup_files:
			if stat.S_ISREG(file.mode):
				key = StatKey(
					path=file.path,
					size=file.blob_raw_size,
					mode=file.mode,
					uid=file.uid,
					gid=file.gid,
					mtime_us=file.mtime,
				)
				stat_to_files[key] = file

		for file_entry in scan_result.all_files:
			if file_entry.is_file():
				key = StatKey(
					path=self.__file_path_to_db_path(file_entry.path),
					size=file_entry.stat.st_size,
					mode=file_entry.stat.st_mode,
					uid=file_entry.stat.st_uid,
					gid=file_entry.stat.st_gid,
					mtime_us=file_entry.stat.st_mtime_ns // 1000
				)
				if (file := stat_to_files.get(key)) is not None:
					self.__pre_calc_result.reused_files[file_entry.path] = file

	def __pre_calculate_hash(self, session: DbSession, scan_result: _ScanResult):
		hashes = self.__pre_calc_result.hashes
		hashes.clear()

		file_entries_to_hash: List[_ScanResultEntry] = [
			file_entry
			for file_entry in scan_result.all_files
			if file_entry.is_file() and file_entry.path not in self.__pre_calc_result.reused_files
		]

		all_sizes: Set[int] = {file_entry.stat.st_size for file_entry in file_entries_to_hash}
		existed_sizes = session.has_blob_with_size_batched(list(all_sizes))
		self.__blob_by_size_cache.update(existed_sizes)

		def hash_worker(pth: Path):
			hashes[pth] = hash_utils.calc_file_hash(pth)

		with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_read):
			with FailFastBlockingThreadPool(name='hasher') as pool:
				for file_entry in file_entries_to_hash:
					if existed_sizes[file_entry.stat.st_size]:
						# we need to hash the file, sooner or later
						pool.submit(hash_worker, file_entry.path)
					else:
						pass  # will use hash_once policy

	@functools.cached_property
	def __temp_path(self) -> Path:
		p = self.config.temp_path
		p.mkdir(parents=True, exist_ok=True)
		return p

	def __get_or_create_blob(self, session: DbSession, src_path: Path, st: os.stat_result) -> Generator[Any, Any, Tuple[schema.Blob, os.stat_result]]:
		src_path_str = repr(src_path.as_posix())
		src_path_md5 = hashlib.md5(src_path_str.encode('utf8')).hexdigest()

		@contextlib.contextmanager
		def make_temp_file() -> Generator[Path, None, None]:
			temp_file_name = f'blob_{os.getpid()}_{threading.current_thread().ident}_{src_path_md5}.tmp'
			temp_file_path = self.__temp_path / temp_file_name
			try:
				yield temp_file_path
			finally:
				self._remove_file(temp_file_path, what='temp_file')

		def attempt_once(last_chance: bool = False) -> Generator[Any, Any, schema.Blob]:
			def log_and_raise_blob_file_changed(msg: str) -> NoReturn:
				(self.logger.warning if last_chance else self.logger.debug)(msg)
				raise _BlobFileChanged(msg)

			compress_method: CompressMethod = self.config.backup.get_compress_method_from_size(st.st_size)
			can_copy_on_write = (
					file_utils.HAS_COPY_FILE_RANGE and
					compress_method == CompressMethod.plain and
					self.__blob_store_in_cow_fs and
					st.st_dev == self.__blob_store_st.st_dev
			)

			policy: Optional[_BlobCreatePolicy] = None
			blob_hash: Optional[str] = None
			blob_content: Optional[bytes] = None
			raw_size: Optional[int] = None
			stored_size: Optional[int] = None
			pre_calc_hash = self.__pre_calc_result.hashes.pop(src_path, None)

			if last_chance:
				policy = _BlobCreatePolicy.copy_hash
			elif pre_calc_hash is not None:  # hash already calculated? just use default
				policy = _BlobCreatePolicy.default
				blob_hash = pre_calc_hash
			elif not can_copy_on_write:  # do tricks iff. no COW copy
				if st.st_size <= _READ_ALL_SIZE_THRESHOLD:
					policy = _BlobCreatePolicy.read_all
					with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_read):
						with _SourceFileNotFound.open_rb(src_path, 'rb') as f:
							blob_content = f.read(_READ_ALL_SIZE_THRESHOLD + 1)
					if len(blob_content) > _READ_ALL_SIZE_THRESHOLD:
						log_and_raise_blob_file_changed('Read too many bytes for read_all policy, stat: {}, read: {}'.format(st.st_size, len(blob_content)))
					blob_hash = hash_utils.calc_bytes_hash(blob_content)
				elif st.st_size > _HASH_ONCE_SIZE_THRESHOLD:
					if (exist := self.__blob_by_size_cache.get(st.st_size)) is None:
						# existence is unknown yet
						yield BlobBySizeFetcher.Req(st.st_size)
						can_hash_once = self.__blob_by_size_cache[st.st_size] is False
					else:
						can_hash_once = exist is False
					if can_hash_once:
						# it's certain that this blob is unique, but notes: the following code
						# cannot be interrupted (yield), or other generator could make a same blob
						policy = _BlobCreatePolicy.hash_once
			if policy is None:
				policy = _BlobCreatePolicy.default
				with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_read):
					with _SourceFileNotFound.wrap(src_path):
						blob_hash = hash_utils.calc_file_hash(src_path)

			# self.logger.info("%s %s %s", policy.name, compress_method.name, src_path)
			if blob_hash is not None:
				misc_utils.assert_true(policy != _BlobCreatePolicy.hash_once, 'unexpected policy')

				if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
					return cache
				yield BlobByHashFetcher.Req(blob_hash)
				if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
					return cache

			# notes: the following code cannot be interrupted (yield).
			# The blob is specifically generated by the generator
			# if any yield is done, ensure to check __blob_by_hash_cache again

			def check_changes(new_size: int, new_hash: Optional[str]):
				if new_size != st.st_size:
					log_and_raise_blob_file_changed('Blob size mismatch, previous: {}, current: {}'.format(st.st_size, new_size))
				if blob_hash is not None and new_hash is not None and new_hash != blob_hash:
					log_and_raise_blob_file_changed('Blob hash mismatch, previous: {}, current: {}'.format(blob_hash, new_hash))

			def bp_rba(h: str) -> Path:
				"""
				bp_rba: blob path, roll back add
				Get blob path by hash, and add the blob path to the rollbacker
				Commonly used right before creating the blob file
				"""
				bp = blob_utils.get_blob_path(h)
				self._add_remove_file_rollbacker(bp)
				return bp

			compressor = Compressor.create(compress_method)
			if policy == _BlobCreatePolicy.copy_hash:
				# copy to temp file, calc hash, then compress to blob store
				misc_utils.assert_true(blob_hash is None, 'blob_hash should not be calculated')
				with make_temp_file() as temp_file_path:
					with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_copy):
						file_utils.copy_file_fast(src_path, temp_file_path, open_r_func=_SourceFileNotFound.open_rb)
					with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_read):
						blob_hash = hash_utils.calc_file_hash(temp_file_path)

					misc_utils.assert_true(last_chance, 'only last_chance=True is allowed for the copy_hash policy')
					if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
						return cache
					yield BlobByHashFetcher.Req(blob_hash)
					if (cache := self.__blob_by_hash_cache.get(blob_hash)) is not None:
						return cache

					blob_path = bp_rba(blob_hash)
					with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_copy):
						cr = compressor.copy_compressed(temp_file_path, blob_path, calc_hash=False)
					raw_size, stored_size = cr.read_size, cr.write_size

			elif policy == _BlobCreatePolicy.hash_once:
				# read once, compress+hash to temp file, then move
				misc_utils.assert_true(blob_hash is None, 'blob_hash should not be calculated')
				with make_temp_file() as temp_file_path:
					with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_copy):
						cr = compressor.copy_compressed(src_path, temp_file_path, calc_hash=True, open_r_func=_SourceFileNotFound.open_rb)
					check_changes(cr.read_size, None)  # the size must be unchanged, to satisfy the uniqueness

					raw_size, blob_hash, stored_size = cr.read_size, cr.read_hash, cr.write_size
					blob_path = bp_rba(blob_hash)

					# reference: shutil.move, but os.replace is used
					try:
						with self.__time_costs.measure_time_cost(_TimeCostKey.kind_fs):
							os.replace(temp_file_path, blob_path)
					except OSError:
						# The temp dir is in the different file system to the blob store?
						# Whatever, use file copy as the fallback
						# the temp file will be deleted automatically
						with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_copy):
							file_utils.copy_file_fast(temp_file_path, blob_path)

			else:
				misc_utils.assert_true(blob_hash is not None, 'blob_hash is None')
				blob_path = bp_rba(blob_hash)

				if policy == _BlobCreatePolicy.read_all:
					# the file content is already in memory, just write+compress to blob store
					misc_utils.assert_true(blob_content is not None, 'blob_content is None')
					with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_write):
						with compressor.open_compressed_bypassed(blob_path) as (writer, f):
							f.write(blob_content)
					raw_size, stored_size = len(blob_content), writer.get_write_len()
				elif policy == _BlobCreatePolicy.default:
					if can_copy_on_write and compress_method == CompressMethod.plain:
						# fast copy, then calc size and hash to verify
						with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_copy):
							file_utils.copy_file_fast(src_path, blob_path, open_r_func=_SourceFileNotFound.open_rb)
						with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_read):
							sah = hash_utils.calc_file_size_and_hash(blob_path)
						raw_size = stored_size = sah.size
						check_changes(sah.size, sah.hash)
					else:
						# copy+compress+hash to blob store
						with self.__time_costs.measure_time_cost(_TimeCostKey.kind_io_copy):
							cr = compressor.copy_compressed(src_path, blob_path, calc_hash=True, open_r_func=_SourceFileNotFound.open_rb)
						raw_size, stored_size = cr.read_size, cr.write_size
						check_changes(cr.read_size, cr.read_hash)
				else:
					raise AssertionError('bad policy {!r}'.format(policy))

			misc_utils.assert_true(blob_hash is not None, f'blob_hash is None, policy {policy}')
			misc_utils.assert_true(raw_size is not None, f'raw_size is None, policy {policy}')
			misc_utils.assert_true(stored_size is not None, f'stored_size is None, policy {policy}')
			return self._create_blob(
				session,
				hash=blob_hash,
				compress=compress_method.name,
				raw_size=raw_size,
				stored_size=stored_size,
			)

		for i in range(_BLOB_FILE_CHANGED_RETRY_COUNT):
			retry_cnt = i + 1  # [1, n]
			is_last_attempt = retry_cnt == _BLOB_FILE_CHANGED_RETRY_COUNT
			if i > 0:
				self.logger.debug('Try to create blob {} (attempt {} / {})'.format(src_path_str, retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT))
			gen = attempt_once(last_chance=is_last_attempt)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:  # ok
				blob: schema.Blob = e.value
				self.__blob_by_size_cache[blob.raw_size] = True
				self.__blob_by_hash_cache[blob.hash] = blob
				return blob, st
			except _BlobFileChanged:
				(self.logger.warning if is_last_attempt else self.logger.debug)('Blob {} stat has changed, has someone modified it? {} (attempt {} / {})'.format(
					src_path_str, 'No more retry' if is_last_attempt else 'Retrying', retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT
				))
				st = src_path.lstat()
			except Exception as e:
				self.logger.error('Create blob for file {} failed (attempt {} / {}): {}'.format(src_path_str, retry_cnt, _BLOB_FILE_CHANGED_RETRY_COUNT, e))
				raise

		self.logger.error('All blob copy attempts failed since the file {} keeps changing'.format(src_path_str))
		raise VolatileBlobFile('blob file {} keeps changing'.format(src_path_str))

	def __create_file(self, session: DbSession, path: Path) -> Generator[Any, Any, schema.File]:
		if (reused_file := self.__pre_calc_result.reused_files.get(path)) is not None:
			# make a copy
			return session.create_file(
				path=sqlalchemy_utils.mapped_cast(reused_file.path),
				role=FileRole.unknown.value,
				mode=sqlalchemy_utils.mapped_cast(reused_file.mode),
				content=sqlalchemy_utils.mapped_cast(reused_file.content),
				blob_hash=sqlalchemy_utils.mapped_cast(reused_file.blob_hash),
				blob_compress=sqlalchemy_utils.mapped_cast(reused_file.blob_compress),
				blob_raw_size=sqlalchemy_utils.mapped_cast(reused_file.blob_raw_size),
				blob_stored_size=sqlalchemy_utils.mapped_cast(reused_file.blob_stored_size),
				uid=sqlalchemy_utils.mapped_cast(reused_file.uid),
				gid=sqlalchemy_utils.mapped_cast(reused_file.gid),
				mtime=sqlalchemy_utils.mapped_cast(reused_file.mtime),
			)

		if (st := self.__pre_calc_result.stats.pop(path, None)) is None:
			with _SourceFileNotFound.wrap(path), self.__time_costs.measure_time_cost(_TimeCostKey.kind_fs):
				st = path.lstat()

		blob: Optional[schema.Blob] = None
		content: Optional[bytes] = None
		if stat.S_ISREG(st.st_mode):
			gen = self.__get_or_create_blob(session, path, st)
			try:
				query = gen.send(None)
				while True:
					result = yield query
					query = gen.send(result)
			except StopIteration as e:
				blob, st = e.value
				# notes: st.st_size might be incorrect, use blob.raw_size instead
		elif stat.S_ISDIR(st.st_mode):
			pass
		elif stat.S_ISLNK(st.st_mode):
			with _SourceFileNotFound.wrap(path):
				content = os.readlink(path).encode('utf8')
		else:
			raise UnsupportedFileFormat(st.st_mode)

		return session.create_file(
			path=self.__file_path_to_db_path(path),
			role=FileRole.unknown.value,

			mode=st.st_mode,
			content=content,
			uid=st.st_uid,
			gid=st.st_gid,
			mtime=st.st_mtime_ns // 1000,

			blob=blob,
		)

	def __create_backup(self, session_context: ContextManager[DbSession], session: DbSession) -> BackupInfo:
		self.logger.info('Scanning file for backup creation at path {!r}, targets: {}'.format(
			self.__source_path.as_posix(), self.config.backup.targets,
		))
		with self.__time_costs.measure_time_cost(_TimeCostKey.stage_scan_files):
			scan_result = self.__scan_files()
		backup = session.create_backup(
			creator=str(self.creator),
			comment=self.comment,
			targets=scan_result.root_targets,
			tags=self.tags.to_dict(),
		)
		self.logger.info('Creating backup for {} at path {!r}, file cnt {}, timestamp {!r}, creator {!r}, comment {!r}, tags {!r}'.format(
			scan_result.root_targets, self.__source_path.as_posix(), len(scan_result.all_files),
			backup.timestamp, backup.creator, backup.comment, backup.tags,
		))

		self.__pre_calculate_stats(scan_result)
		if self.config.backup.reuse_stat_unchanged_file:
			with self.__time_costs.measure_time_cost(_TimeCostKey.stage_reuse_unchanged_files):
				self.__reuse_unchanged_files(session, scan_result)
			self.logger.info('Reused {} / {} stat unchanged files'.format(len(self.__pre_calc_result.reused_files), len(scan_result.all_files)))
		if self.config.get_effective_concurrency() > 1:
			with self.__time_costs.measure_time_cost(_TimeCostKey.stage_pre_calculate_hash):
				self.__pre_calculate_hash(session, scan_result)
			self.logger.info('Pre-calculate all file hash done')

		with self.__time_costs.measure_time_cost(_TimeCostKey.stage_prepare_blob_store, _TimeCostKey.kind_fs):
			blob_utils.prepare_blob_directories()
			bs_path = blob_utils.get_blob_store()
			self.__blob_store_st = bs_path.stat()
			self.__blob_store_in_cow_fs = file_utils.does_fs_support_cow(bs_path)

		@functools.lru_cache(None)
		def get_skip_missing_source_file_patterns() -> pathspec.GitIgnoreSpec:
			return pathspec.GitIgnoreSpec.from_lines(self.config.backup.creation_skip_missing_file_patterns)

		def should_skip_missing_source_file(src_file_path: Path) -> bool:
			if self.config.backup.creation_skip_missing_file:
				try:
					rel_path = src_file_path.relative_to(self.__source_path)
				except ValueError:
					self.logger.error("Path {!r} is not inside the source path {!r}".format(str(src_file_path), str(self.__source_path)))
				else:
					return get_skip_missing_source_file_patterns().match_file(rel_path)
			return False

		files = []
		with self.__time_costs.measure_time_cost(_TimeCostKey.stage_create_files):
			schedule_queue: Deque[Tuple[Generator[BqmReq, Optional[BqmRsp], schema.File], Optional[BqmRsp]]] = collections.deque()
			for file_entry in scan_result.all_files:
				schedule_queue.append((self.__create_file(session, file_entry.path), None))
			while len(schedule_queue) > 0:
				gen, value = schedule_queue.popleft()
				try:
					def callback(query_rsp: BqmRsp, g=gen):
						schedule_queue.appendleft((g, query_rsp))

					query_req = gen.send(value)
					self.__batch_query_manager.query(query_req, callback)
				except StopIteration as e:
					files.append(misc_utils.ensure_type(e.value, schema.File))
				except _SourceFileNotFound as e:
					if should_skip_missing_source_file(e.file_path):
						self.logger.warning('Backup source file {!r} not found, suppressed and skipped by config'.format(str(e.file_path)))
					else:
						raise

				self.__batch_query_manager.flush_if_needed()
				if len(schedule_queue) == 0:
					self.__batch_query_manager.flush()

		with self.__time_costs.measure_time_cost(_TimeCostKey.stage_finalize):
			self._finalize_backup_and_files(session, backup, files)
		info = BackupInfo.of(backup)

		with self.__time_costs.measure_time_cost(_TimeCostKey.stage_flush_db, _TimeCostKey.kind_db):
			session_context.__exit__(None, None, None)
		return info

	@override
	def run(self) -> BackupInfo:
		super().run()

		# TODO: prevent re-run
		self.__blob_by_size_cache.clear()
		self.__blob_by_hash_cache.clear()
		self.__time_costs.reset()
		with self.__time_costs.measure_time_cost(*_TimeCostKey):
			pass
		action_start_ts = time.time()

		try:
			session_context = DbAccess.open_session()
			with session_context as session:
				self.__batch_query_manager = BatchQueryManager(session, self.__blob_by_size_cache, self.__blob_by_hash_cache, self.__time_costs)
				info = self.__create_backup(session_context, session)
		except Exception as e:
			self._apply_blob_rollback()
			raise e
		else:
			s = self.get_new_blobs_summary()
			self.logger.info('Create backup #{} done, +{} blobs (size {} / {})'.format(
				info.id, s.count, ByteCount(s.stored_size).auto_str(), ByteCount(s.raw_size).auto_str(),
			))
			self.__log_costs(time.time() - action_start_ts)

			return info

	def __log_costs(self, actual_cost: float):
		if not (self.config.debug and self.logger.isEnabledFor(logging.DEBUG)):
			return

		def log_one_key(what: str, cost: float):
			self.logger.debug('  {}: {:.3f}s ({:.1f}%)'.format(what, cost, 100.0 * cost / actual_cost))

		self.logger.debug('========================')
		self.logger.debug('{} run costs'.format(self.__class__.__name__))
		log_one_key('ACTUAL', actual_cost)

		all_costs = self.__time_costs.get_costs()
		kind_costs = {k: v for k, v in all_costs.items() if k.name.startswith('kind_')}
		stage_costs = {k: v for k, v in all_costs.items() if k.name.startswith('stage_')}

		self.logger.debug('Kind costs')
		for k, v in kind_costs.items():
			log_one_key(k.name, v)
		log_one_key('rest', actual_cost - sum(kind_costs.values()))
		self.logger.debug('Stage costs')
		for k, v in stage_costs.items():
			log_one_key(k.name, v)
		log_one_key('rest', actual_cost - sum(stage_costs.values()))
		self.logger.debug('========================')
