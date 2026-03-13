import argparse
import dataclasses
import time
from pathlib import Path

from typing_extensions import override

from prime_backup.action.repack_mca_action import RepackMcaAction
from prime_backup.cli.cmd import CliCommandHandlerBase, CommonCommandArgs, CliCommandAdapterBase
from prime_backup.types.units import ByteCount


@dataclasses.dataclass(frozen=True)
class RepackMcaCommandArgs(CommonCommandArgs):
	dry_run: bool
	yes: bool


class RepackMcaCommandHandler(CliCommandHandlerBase):
	def __init__(self, args: RepackMcaCommandArgs):
		super().__init__()
		self.args = args

	def handle(self):
		self.init_environment(self.args.db_path)

		if not self.args.yes and not self.args.dry_run:
			print('This operation will convert all standalone .mca backup blobs into chunk-level deduplicated format.')
			print('Make sure no other PrimeBackup process is running against this storage.')
			try:
				answer = input('Continue? [y/N] ')
			except (EOFError, KeyboardInterrupt):
				print()
				return
			if answer.lower() not in ('y', 'yes'):
				self.logger.info('Aborted')
				return

		action = RepackMcaAction(dry_run=self.args.dry_run)
		t = time.time()
		size_diff = action.run()
		elapsed = time.time() - t

		if not self.args.dry_run and size_diff.before != 0:
			self.logger.info(
				'Repack completed in {:.1f}s. Storage: {} -> {} (saved {})'.format(
					elapsed,
					ByteCount(size_diff.before).auto_str(),
					ByteCount(size_diff.after).auto_str(),
					ByteCount(size_diff.before - size_diff.after).auto_str(),
				)
			)


class RepackMcaCommandAdapter(CliCommandAdapterBase):
	@property
	@override
	def command(self) -> str:
		return 'repack_mca'

	@property
	@override
	def description(self) -> str:
		return 'Repack existing standalone .mca backups into chunk-level deduplicated format'

	@override
	def build_parser(self, parser: argparse.ArgumentParser):
		parser.add_argument('--dry-run', action='store_true', default=False, help='Only scan and report, do not modify anything')
		parser.add_argument('-y', '--yes', action='store_true', default=False, help='Skip confirmation prompt')

	@override
	def run(self, args: argparse.Namespace):
		handler = RepackMcaCommandHandler(RepackMcaCommandArgs(
			db_path=Path(args.db),
			dry_run=args.dry_run,
			yes=args.yes,
		))
		handler.handle()
