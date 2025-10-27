import threading
from typing import Optional, TYPE_CHECKING

from mcdreforged.api.all import CommandSource, PluginServerInterface

from prime_backup import logger
from prime_backup.action.list_backup_action import ListBackupAction
from prime_backup.mcdr.task.backup.create_backup_task import CreateBackupTask
from prime_backup.mcdr.task.backup.restore_backup_task import RestoreBackupTask
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.types.backup_filter import BackupFilter, BackupSortOrder
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils import backup_utils

if TYPE_CHECKING:  # pragma: no cover
	from prime_backup.mcdr.task_manager import TaskManager


class CrashRecoveryManager:
	"""In-memory tracker that guards against repeated crash loops."""
	def __init__(self, task_manager: 'TaskManager'):
		self._task_manager = task_manager
		self._logger = logger.get()
		self._lock = threading.Lock()
		# consecutive abnormal exits counter, volatile only
		self._consecutive_abnormal = 0
		self._in_progress = False
		self._last_saved_counter = 0

	def record_server_stop(self, return_code: int):
		"""Track exit status to detect consecutive abnormal shutdowns."""
		with self._lock:
			if self._in_progress:
				return
			if return_code == 0:
				# success resets failure streak immediately
				self._consecutive_abnormal = 0
			else:
				self._consecutive_abnormal += 1
				self._logger.warning('Server exited abnormally with code %s (%s consecutive)', return_code, self._consecutive_abnormal)

	def on_server_start(self, server: PluginServerInterface):
		"""Intercept the boot if the crash threshold is hit."""
		with self._lock:
			if self._in_progress or self._consecutive_abnormal < 2:
				return
			self._in_progress = True
			self._last_saved_counter = self._consecutive_abnormal
			self._consecutive_abnormal = 0

		source = server.get_plugin_command_source()
		self._logger.warning('Crash recovery triggered after %s consecutive abnormal exits', self._last_saved_counter)
		task = _CrashRecoveryTask(source, self)
		self._task_manager.add_task(task)

	def on_task_finished(self, succeeded: bool):
		"""Reset bookkeeping once the recovery task finishes."""
		with self._lock:
			self._in_progress = False
			if succeeded:
				# successful recovery clears the streak entirely
				self._last_saved_counter = 0
			else:
				self._consecutive_abnormal = max(2, self._last_saved_counter)
				self._logger.error('Crash recovery failed, counter restored to %s', self._consecutive_abnormal)

	def reset(self):
		"""Forcefully clear internal state, mainly for plugin unload."""
		with self._lock:
			self._consecutive_abnormal = 0
			self._in_progress = False
			self._last_saved_counter = 0


class _CrashRecoveryTask(HeavyTask[None]):
	"""Heavy task that snapshots, rewinds, and restarts the server."""
	def __init__(self, source: CommandSource, manager: CrashRecoveryManager):
		super().__init__(source)
		self._manager = manager

	@property
	def id(self) -> str:
		return 'crash_recovery'

	def get_abort_permission(self) -> int:
		return 0

	def run(self) -> None:
		succeeded = False
		try:
			self.broadcast(self.tr('start'))
			self._ensure_server_stopped()
			backup_id = self._create_snapshot()
			if backup_id is None:
				self.broadcast(self.tr('backup_failed'))
				return
			previous_backup = self._select_previous_backup(backup_id)
			if previous_backup is None:
				self.broadcast(self.tr('no_previous'))
				return
			self.broadcast(self.tr('restore_start', TextComponents.backup_brief(previous_backup)))
			self._restore(previous_backup.id)
			self._restart_server()
			self.broadcast(self.tr('completed', TextComponents.backup_id(previous_backup.id)))
			succeeded = True
		finally:
			self._manager.on_task_finished(succeeded)

	def _create_snapshot(self) -> Optional[int]:
		"""Create the crash snapshot backup with auto markers."""
		comment = backup_utils.create_translated_backup_comment('crash_auto_recovery')
		operator = Operator.pb(PrimeBackupOperatorNames.scheduled_backup)
		task = CreateBackupTask(self.source, comment, operator=operator)
		return self.run_subtask(task)

	def _select_previous_backup(self, current_backup_id: int) -> Optional[BackupInfo]:
		"""Pick the most recent non-temporary backup preceding the snapshot."""
		flt = BackupFilter()
		flt.id_end = current_backup_id - 1
		flt.sort_order = BackupSortOrder.id_r
		flt.requires_non_temporary_backup()
		flt.requires_non_hidden_backup()
		backups = ListBackupAction(backup_filter=flt, limit=1).run()
		return backups[0] if backups else None

	def _restore(self, backup_id: int):
		"""Replay the chosen backup without interactive confirmation."""
		task = RestoreBackupTask(self.source, backup_id, needs_confirm=False, fail_soft=False)
		self.run_subtask(task)

	def _ensure_server_stopped(self):
		"""Stop the server before touching disk state."""
		if not self.server.is_server_running():
			return
		self.broadcast(self.tr('stopping'))
		# asynchronously started server must be force-stopped here
		self.server.stop()
		self.server.wait_until_stop()

	def _restart_server(self):
		"""Bring the server back after rollback."""
		self.broadcast(self.tr('restarting'))
		self.server.start()
