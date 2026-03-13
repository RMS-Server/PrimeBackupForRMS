from mcdreforged.api.all import CommandSource
from typing_extensions import override

from prime_backup.action.repack_mca_action import RepackMcaAction
from prime_backup.mcdr.task.basic_task import HeavyTask
from prime_backup.types.units import ByteCount


class RepackMcaTask(HeavyTask[None]):
	def __init__(self, source: CommandSource):
		super().__init__(source)

	@property
	@override
	def id(self) -> str:
		return 'db_repack_mca'

	@override
	def run(self):
		self.reply_tr('confirm_prompt')
		if not self.wait_confirm(self.tr('confirm_target')):
			return

		self.reply_tr('start')
		size_diff = self.run_action(RepackMcaAction())

		if size_diff.before == 0 and size_diff.after == 0:
			self.reply_tr('nothing_to_do')
		else:
			self.reply_tr(
				'done',
				ByteCount(size_diff.before).auto_str(),
				ByteCount(size_diff.after).auto_str(),
				ByteCount(size_diff.before - size_diff.after).auto_str(),
			)
