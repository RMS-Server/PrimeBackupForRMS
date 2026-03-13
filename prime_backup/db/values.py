import enum
from typing import Dict, Any, List

BackupTagDict = Dict[str, Any]


class FileRole(enum.IntEnum):
	unknown = 0
	standalone = 1
	delta_override = 2
	delta_add = 3
	delta_remove = 4
	mca_assembled = 5  # .mca stored as chunk blobs referenced by a manifest blob

	@classmethod
	def standalone_roles(cls) -> List['FileRole']:
		return [cls.standalone]

	@classmethod
	def standalone_role_ints(cls) -> List[int]:
		return [role.value for role in cls.standalone_roles()]

	@classmethod
	def delta_roles(cls) -> List['FileRole']:
		return [
			cls.delta_override,
			cls.delta_add,
			cls.delta_remove,
		]

	@classmethod
	def delta_role_ints(cls) -> List[int]:
		return [role.value for role in cls.delta_roles()]

	@classmethod
	def base_present_role_ints(cls) -> List[int]:
		"""Roles indicating a file is present in a base fileset"""
		return [cls.standalone.value, cls.mca_assembled.value]

	@classmethod
	def delta_present_role_ints(cls) -> List[int]:
		"""Roles indicating a file is present (not removed) in a delta fileset"""
		return [cls.delta_add.value, cls.delta_override.value, cls.mca_assembled.value]
