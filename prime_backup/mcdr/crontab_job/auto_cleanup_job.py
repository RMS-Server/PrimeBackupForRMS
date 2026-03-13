import time
from typing import TYPE_CHECKING, List, Optional

from apscheduler.schedulers.base import BaseScheduler
from typing_extensions import override

from prime_backup.config.config_common import CrontabJobSetting
from prime_backup.config.scheduled_backup_config import ScheduledBackupConfig
from prime_backup.mcdr import mcdr_globals
from prime_backup.mcdr.crontab_job import CrontabJobId
from prime_backup.mcdr.crontab_job.basic_job import BasicCrontabJob
from prime_backup.mcdr.online_player_counter import OnlinePlayerCounter
from prime_backup.types.backup_filter import BackupFilter
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.operator import Operator, PrimeBackupOperatorNames
from prime_backup.utils.mcdr_utils import broadcast_message

if TYPE_CHECKING:
	from prime_backup.mcdr.task_manager import TaskManager


class AutoCleanupJob(BasicCrontabJob):
	def __init__(self, scheduler: BaseScheduler, task_manager: 'TaskManager'):
		super().__init__(scheduler, task_manager)
		self.config: ScheduledBackupConfig = self._root_config.scheduled_backup
		self._cleanup_scheduled_time: Optional[int] = None
		self._cleanup_backup_ids: List[int] = []

	@property
	@override
	def id(self) -> CrontabJobId:
		return CrontabJobId.auto_cleanup

	@property
	@override
	def job_config(self) -> CrontabJobSetting:
		from prime_backup.types.units import Duration
		class AutoCleanupJobSetting(CrontabJobSetting):
			enabled: bool = self.config.auto_cleanup_enabled
			interval: Optional[Duration] = self.config.interval if self.config.auto_cleanup_enabled else None
			crontab: Optional[str] = self.config.crontab
			jitter: Duration = self.config.jitter
		return AutoCleanupJobSetting()

	@override
	def run(self):
		if not self.config.auto_cleanup_enabled:
			return

		if not mcdr_globals.server.is_server_running():
			return

		current_time_us = int(time.time() * 1_000_000)
		expire_time_us = int(self.config.auto_cleanup_expire_time.value * 1_000_000)
		warning_time_us = int(self.config.auto_cleanup_warning_time.value * 1_000_000)

		if self._cleanup_scheduled_time is not None:
			if current_time_us >= self._cleanup_scheduled_time:
				self._execute_cleanup()
			return

		expired_backups = self._find_expired_backups(current_time_us, expire_time_us)
		if not expired_backups:
			return

		snapshot = OnlinePlayerCounter.get().get_player_record_snapshot()
		if snapshot is None or not snapshot.has_valid_online:
			self.logger.info(f'Found {len(expired_backups)} expired auto backups, but no valid players online, skipping warning')
			return

		self.logger.info(f'Found {len(expired_backups)} expired auto backups with valid players online ({snapshot.summary})')
		self._cleanup_backup_ids = [backup.id for backup in expired_backups]
		self._cleanup_scheduled_time = current_time_us + warning_time_us

		self._broadcast_cleanup_warning(expired_backups)

	def _find_expired_backups(self, current_time_us: int, expire_time_us: int) -> List[BackupInfo]:
		from prime_backup.action.list_backup_action import ListBackupAction

		backup_filter = BackupFilter()
		backup_filter.creator = Operator.pb(PrimeBackupOperatorNames.scheduled_backup)

		list_action = ListBackupAction(backup_filter=backup_filter)
		backups = list_action.run()

		expired_backups = []
		for backup in backups:
			backup_age_us = current_time_us - backup.timestamp_us
			if backup_age_us > expire_time_us:
				expired_backups.append(backup)

		return expired_backups

	@staticmethod
	def _age_str(backup: BackupInfo) -> str:
		from prime_backup.types.units import Duration
		age_us = int(time.time() * 1_000_000) - backup.timestamp_us
		return str(Duration(age_us / 1_000_000))

	def _broadcast_cleanup_warning(self, expired_backups: List[BackupInfo]):
		count = len(expired_backups)
		prefix = self._root_config.command.prefix
		if count == 1:
			backup = expired_backups[0]
			time_info = f"{backup.date_str}(-{self._age_str(backup)})"
			msg = self.tr('warning_single', time_info, prefix)
			broadcast_message(msg)
		else:
			oldest = min(expired_backups, key=lambda b: b.timestamp_us)
			newest = max(expired_backups, key=lambda b: b.timestamp_us)
			time_info = f"{oldest.date_str}到{newest.date_str}"
			msg = self.tr('warning_multiple', time_info, count, prefix)
			broadcast_message(msg)

	def _execute_cleanup(self):
		backup_ids = self._cleanup_backup_ids
		if not backup_ids:
			self.logger.warning('Auto cleanup executed but no backup IDs found')
			self._cleanup_scheduled_time = None
			return

		self.logger.info(f'Executing auto cleanup of {len(backup_ids)} expired backups')

		from prime_backup.mcdr.task.backup.delete_backup_task import DeleteBackupTask

		task = DeleteBackupTask(
			self.get_command_source(),
			backup_ids,
			needs_confirm=False
		)

		def cleanup_callback(backup_id: Optional[int], err: Optional[Exception]):
			self._cleanup_scheduled_time = None
			self._cleanup_backup_ids = []
			if err is None:
				broadcast_message(self.tr('cleanup_completed', len(backup_ids)))
			else:
				self.logger.error(f'Auto cleanup failed: {err}')
				broadcast_message(self.tr('cleanup_failed'))

		self.task_manager.add_task(task, cleanup_callback)

	def cancel_scheduled_cleanup(self) -> bool:
		if self._cleanup_scheduled_time is not None:
			count = len(self._cleanup_backup_ids)
			self._cleanup_scheduled_time = None
			self._cleanup_backup_ids = []
			broadcast_message(self.tr('cleanup_cancelled', count))
			return True
		return False

	def reset_cleanup_state(self) -> None:
		"""Silently clear scheduled cleanup state, used when manual run has already handled the deletion."""
		self._cleanup_scheduled_time = None
		self._cleanup_backup_ids = []

	def get_cleanup_status(self) -> Optional[dict]:
		scheduled_time = self._cleanup_scheduled_time
		if scheduled_time is None:
			return None

		backup_ids = self._cleanup_backup_ids
		current_time_us = int(time.time() * 1_000_000)
		remaining_time_us = scheduled_time - current_time_us

		return {
			'backup_count': len(backup_ids),
			'backup_ids': backup_ids,
			'scheduled_time_us': scheduled_time,
			'remaining_time_us': max(0, remaining_time_us),
		}

	@property
	def auto_cleanup_enabled(self) -> bool:
		return self.config.auto_cleanup_enabled

	def find_expired_backups_now(self) -> List[BackupInfo]:
		"""Find expired backups using the configured expire time, regardless of auto_cleanup_enabled."""
		current_time_us = int(time.time() * 1_000_000)
		expire_time_us = int(self.config.auto_cleanup_expire_time.value * 1_000_000)
		return self._find_expired_backups(current_time_us, expire_time_us)

	def manual_check(self) -> List[BackupInfo]:
		if not self.config.auto_cleanup_enabled:
			return []

		current_time_us = int(time.time() * 1_000_000)
		expire_time_us = int(self.config.auto_cleanup_expire_time.value * 1_000_000)

		return self._find_expired_backups(current_time_us, expire_time_us)