from typing import Optional, List

from mcdreforged.api.all import *
from typing_extensions import override

from prime_backup.mcdr.crontab_job import CrontabJobId
from prime_backup.mcdr.crontab_manager import CrontabManager
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents
from prime_backup.utils.mcdr_utils import TranslationContext


class AutoCleanCancelTask(LightTask[bool]):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager):
		super().__init__(source)
		self.crontab_manager = crontab_manager

	@property
	@override
	def id(self) -> str:
		return 'autoclean_cancel'

	@override
	def get_abort_permission(self) -> int:
		return 0

	@override
	def run(self) -> bool:
		auto_cleanup_job = self.crontab_manager.get_job(CrontabJobId.auto_cleanup)
		
		cancelled = auto_cleanup_job.cancel_scheduled_cleanup()
		if cancelled:
			self.reply(self.tr('success'))
		else:
			self.reply(self.tr('no_scheduled_cleanup'))
		
		return cancelled


class AutoCleanStatusTask(LightTask[Optional[dict]]):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager):
		super().__init__(source)
		self.crontab_manager = crontab_manager

	@property
	@override
	def id(self) -> str:
		return 'autoclean_status'

	@override
	def get_abort_permission(self) -> int:
		return 0

	@override
	def run(self) -> Optional[dict]:
		auto_cleanup_job = self.crontab_manager.get_job(CrontabJobId.auto_cleanup)
		
		status = auto_cleanup_job.get_cleanup_status()
		if status is None:
			self.reply(self.tr('no_scheduled_cleanup'))
			return None
		
		backup_count = status['backup_count']
		remaining_time_us = status['remaining_time_us']
		remaining_time_text = TextComponents.duration(remaining_time_us / 1_000_000)
		
		self.reply(self.tr('status_info', backup_count, remaining_time_text))
		return status


class AutoCleanListTask(LightTask[List[int]]):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager):
		super().__init__(source)
		self.crontab_manager = crontab_manager

	@property
	@override
	def id(self) -> str:
		return 'autoclean_list'

	@override
	def get_abort_permission(self) -> int:
		return 0

	@override
	def run(self) -> List[int]:
		auto_cleanup_job = self.crontab_manager.get_job(CrontabJobId.auto_cleanup)
		
		status = auto_cleanup_job.get_cleanup_status()
		if status is None:
			self.reply(self.tr('no_scheduled_cleanup'))
			return []
		
		backup_ids = status['backup_ids']
		self.reply(self.tr('list_header', len(backup_ids)))
		
		from prime_backup.action.get_backup_action import GetBackupAction
		for backup_id in backup_ids:
			try:
				get_action = GetBackupAction(backup_id)
				backup_info = get_action.run()
				self.reply(TextComponents.backup_brief(backup_info))
			except Exception as e:
				self.reply(self.tr('get_backup_failed', backup_id, str(e)).set_color(RColor.red))
		
		return backup_ids


class AutoCleanCheckTask(LightTask[List[int]]):
	def __init__(self, source: CommandSource, crontab_manager: CrontabManager):
		super().__init__(source)
		self.crontab_manager = crontab_manager

	@property
	@override
	def id(self) -> str:
		return 'autoclean_check'

	@override
	def get_abort_permission(self) -> int:
		return 0

	@override
	def run(self) -> List[int]:
		auto_cleanup_job = self.crontab_manager.get_job(CrontabJobId.auto_cleanup)
		
		expired_backups = auto_cleanup_job.manual_check()
		
		if not expired_backups:
			self.reply(self.tr('no_expired_backups'))
			return []
		
		self.reply(self.tr('found_expired_backups', len(expired_backups)))
		
		from prime_backup.mcdr.text_components import TextComponents
		for backup in expired_backups:
			self.reply(TextComponents.backup_brief(backup))
		
		return [backup.id for backup in expired_backups]