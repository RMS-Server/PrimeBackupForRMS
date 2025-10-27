from typing import Any

from mcdreforged.api.all import CommandSource, RTextBase, RText, RTextList, RColor, RAction
from typing_extensions import override

from prime_backup.action.get_backup_action import GetBackupAction
from prime_backup.mcdr.task.basic_task import LightTask
from prime_backup.mcdr.text_components import TextComponents, TextColors
from prime_backup.types.backup_info import BackupInfo
from prime_backup.types.backup_tags import BackupTagName, BackupTags
from prime_backup.utils.mcdr_utils import mkcmd


class ShowBackupSingleTagTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id: int, tag_name: BackupTagName):
		super().__init__(source)
		self.backup_id = backup_id
		self.tag_name = tag_name

	@property
	@override
	def id(self) -> str:
		return 'backup_show_tag_single'

	@override
	def run(self) -> None:
		backup = GetBackupAction(self.backup_id).run()
		value = backup.tags.get(self.tag_name)
		if value != BackupTags.NONE:
			self.reply_tr(
				'value',
				TextComponents.backup_id(backup.id, backup_data=backup),
				TextComponents.tag_name(self.tag_name),
				TextComponents.auto(value),
			)
		else:
			self.reply_tr(
				'not_exists',
				TextComponents.backup_id(backup.id, backup_data=backup),
				TextComponents.tag_name(self.tag_name),
			)


class ShowBackupTagTask(LightTask[None]):
	def __init__(self, source: CommandSource, backup_id: int):
		super().__init__(source)
		self.backup_id = backup_id

	@property
	@override
	def id(self) -> str:
		return 'backup_show_tag'

	def __show_tag(self, backup: BackupInfo, key: str, value: Any):
		try:
			tag_name = BackupTagName[key]
		except KeyError:
			t_value_type = self.tr('value_type', type(value).__name__)
			t_key = RText(key).h(t_value_type)
		else:
			t_value_type = self.tr('value_type', tag_name.value.type.__name__)
			t_key = TextComponents.tag_name(tag_name).h(RTextList(tag_name.value.text.copy().set_color(TextColors.backup_tag), '\n', t_value_type))

		if value is not BackupTags.NONE:
			t_value = TextComponents.auto(value)
			buttons = [
				RText('[_]', RColor.yellow).h(self.tr('edit', t_key)).c(RAction.suggest_command, mkcmd(f'tag {backup.id} {key} set ')),
				RText('[x]', RColor.red).h(self.tr('clear', t_key)).c(RAction.suggest_command, mkcmd(f'tag {backup.id} {key} clear')),
			]
		else:
			t_value = self.tr('not_exists').set_color(RColor.gray)
			buttons = [
				RText('[+]', RColor.dark_green).h(self.tr('create', t_key)).c(RAction.suggest_command, mkcmd(f'tag {backup.id} {key} set ')),
			]

		if self.source.is_console:
			self.reply(RTextBase.format('{}: {}', t_key, t_value))
		else:
			self.reply(RTextBase.format('{} {}: {}', RTextBase.join(' ', buttons), t_key, t_value))

	@override
	def run(self) -> None:
		backup = GetBackupAction(self.backup_id).run()
		self.reply(TextComponents.title(self.tr('title', TextComponents.backup_id(backup.id, backup_data=backup))))
		self.reply_tr('amount', TextComponents.number(len(backup.tags)))

		recognized_names = set()
		for tag_name in BackupTagName:
			recognized_names.add(tag_name.name)
			self.__show_tag(backup, tag_name.name, backup.tags.get(tag_name))

		for key, value in backup.tags.items():
			if key not in recognized_names:
				self.__show_tag(backup, key, value)
