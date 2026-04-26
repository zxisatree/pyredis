from enum import Enum
from pathlib import Path

from .logs import logger


class AppendFsyncOption(Enum):
    ALWAYS = "always"
    EVERYSEC = "everysec"


class AofHandler:
    def __init__(
        self,
        rdbdir: str,
        append_only: bool,
        append_dirname: str,
        append_filename: str,
        append_fsync: AppendFsyncOption,
    ) -> None:
        self.rdbdir = rdbdir
        self.rdbdir_path = Path(self.rdbdir).resolve().absolute()
        self.append_only = append_only
        self.append_dirname = append_dirname
        self.append_filename = append_filename
        self.append_fsync = append_fsync

        if self.append_only:
            self.aof_dir_path = self.rdbdir_path / self.append_dirname
            self.manifest_file_path = self.aof_dir_path / (
                self.append_filename + ".manifest"
            )
            if not self.aof_dir_path.exists():
                self.aof_dir_path.mkdir(parents=True, exist_ok=True)
                self.seq_num = 1
                self.aof_type = "i"
                self.aof_file_path = self.aof_dir_path / (
                    self.append_filename + f".{self.seq_num}.incr.aof"
                )
                with self.manifest_file_path.open("w") as f:
                    f.write(
                        f"file {self.aof_file_path.name} seq {self.seq_num} type {self.aof_type}"
                    )
                self.aof_file_path.touch(exist_ok=True)
            else:
                with self.manifest_file_path.open("r") as f:
                    (
                        file_literal,
                        file_path,
                        seq_literal,
                        seq_num,
                        type_literal,
                        aof_type,
                    ) = (
                        f.read().strip().split()
                    )
                assert file_literal == "file"
                assert seq_literal == "seq"
                assert type_literal == "type"
                self.aof_file_path = self.aof_dir_path / file_path
                try:
                    self.seq_num = int(seq_num)
                except ValueError:
                    logger.warning(
                        "Could not parse sequence number from AOF manifest file. Defaulting to 1"
                    )
                    self.seq_num = 1
                self.aof_type = aof_type

    def __enter__(self):
        if self.append_only:
            self.aof_file = self.aof_file_path.open("ab+")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.append_only:
            self.aof_file.close()
        return False

    def read(self) -> bytes:
        if self.append_only:
            # file is opened in ab+ mode
            self.aof_file.seek(0)
            return self.aof_file.read()
        return b""

    def write(self, data: bytes):
        if self.append_only:
            self.aof_file.write(data)
            if self.append_fsync == AppendFsyncOption.ALWAYS:
                self.aof_file.flush()
