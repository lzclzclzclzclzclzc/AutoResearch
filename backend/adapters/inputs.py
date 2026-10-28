"""Read-only, explicitly selected human inputs captured as immutable evidence."""
from __future__ import annotations

import hashlib
import codecs
import json
import mimetypes
import os
import shutil
import uuid
from pathlib import Path, PureWindowsPath


class InputAccessError(PermissionError):
    public_message = "资料路径未获授权、包含受保护文件，或文件在读取时发生变化"


class ResearchInputs:
    TRANSPORT_LIMIT = 64 * 1024
    PAGE_SIZE = 100
    PROTECTED_PARTS = {".git", ".ssh", ".aws", ".azure", ".config", ".codex", ".agents", "credentials", "secrets", "node_modules", ".venv"}
    PROTECTED_SUFFIXES = {".pem", ".key", ".pfx", ".p12", ".dpapi", ".jks", ".kdbx"}

    def __init__(self, store, research_id, context, progress):
        self.store, self.research_id, self.context, self.progress = store, research_id, context, progress
        value = store.project(research_id).get("workspace_path")
        if not value:
            error = InputAccessError("No human-selected input workspace")
            error.public_message = "本研究未配置用户资料目录，请先在研究设置中指定目录"
            raise error
        selected = Path(value).expanduser()
        if not selected.is_absolute():
            raise InputAccessError("The human input directory must be absolute")
        try:
            self.root = selected.resolve(strict=True)
        except OSError as error:
            raise InputAccessError("Human input directory is unavailable") from error
        self.managed = store.data_dir.resolve()
        if not self.root.is_dir() or self.root.is_relative_to(self.managed) or self._protected(self.root.parts):
            raise InputAccessError("Protected input directory")

    @classmethod
    def _protected(cls, parts):
        for part in parts:
            name = part.lower()
            if name in cls.PROTECTED_PARTS or name.startswith((".env", ".credentials")):
                return True
            if Path(name).suffix in cls.PROTECTED_SUFFIXES:
                return True
            if any(token in name for token in ("credential", "api_key", "api-key", "apikey", "private_key", "private-key", "access_token", "access-token")):
                return True
            if name in {"id_rsa", "id_ed25519", "id_dsa", "id_ecdsa", ".netrc", ".npmrc", ".pypirc", "authorized_keys", "password", "passwords", "token", "tokens", "secrets.json", "secrets.toml", "secrets.yaml", "secrets.yml"}:
                return True
        return False

    def _check(self, path):
        if not path.is_relative_to(self.root) or path.is_relative_to(self.managed) or self._protected(path.relative_to(self.root).parts):
            raise InputAccessError("Path escapes the selected workspace or is protected")
        return path

    def resolve(self, value=""):
        if not isinstance(value, str) or "\x00" in value or ":" in value:
            raise InputAccessError("Invalid relative input path")
        windows = PureWindowsPath(value)
        if windows.is_absolute() or windows.drive or any(part == ".." for part in windows.parts) or self._protected(windows.parts):
            raise InputAccessError("Input paths must stay relative to the selected directory")
        try:
            return self._check((self.root / Path(*windows.parts)).resolve(strict=True))
        except OSError as error:
            raise InputAccessError("Input does not exist") from error

    @staticmethod
    def _offset(value):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("Offset must be a nonnegative integer")
        return value

    def list(self, path="", offset=0):
        offset, directory = self._offset(offset), self.resolve(path)
        if not directory.is_dir():
            raise ValueError("The selected input is not a directory")
        entries = []
        for entry in sorted(directory.iterdir(), key=lambda item: (item.name.casefold(), item.name)):
            try:
                resolved = self._check(entry.resolve(strict=True))
                if self._protected(entry.relative_to(self.root).parts):
                    continue
                info = resolved.stat()
                if not (resolved.is_file() or resolved.is_dir()):
                    continue
                entries.append({"name": entry.name, "relative_path": entry.relative_to(self.root).as_posix(),
                    "kind": "directory" if resolved.is_dir() else "file", "size_bytes": info.st_size if resolved.is_file() else None})
            except (InputAccessError, OSError):
                continue
        page = entries[offset:offset + self.PAGE_SIZE]
        result = {"path": directory.relative_to(self.root).as_posix(), "offset": offset, "entries": page,
            "next_offset": offset + len(page) if offset + len(page) < len(entries) else None}
        # Long filenames also obey the transport bound; a caller advances by entries.
        while len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > self.TRANSPORT_LIMIT:
            if not page:
                raise InputAccessError("Input path metadata exceeds the transport limit")
            page.pop()
            result["next_offset"] = offset + len(page)
        return result

    def _opened_path(self, handle):
        # Validate the opened object, closing a symlink/reparse swap between resolve/open.
        if os.name == "nt":
            import ctypes
            import msvcrt
            from ctypes import wintypes
            get_path = ctypes.windll.kernel32.GetFinalPathNameByHandleW
            get_path.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
            get_path.restype = wintypes.DWORD
            buffer = ctypes.create_unicode_buffer(32768)
            size = get_path(msvcrt.get_osfhandle(handle.fileno()), buffer, len(buffer), 0)
            if not size or size >= len(buffer):
                raise InputAccessError("Cannot verify opened input identity")
            value = buffer.value
            if value.startswith("\\\\?\\UNC\\"):
                value = "\\\\" + value[8:]
            elif value.startswith("\\\\?\\"):
                value = value[4:]
            return self._check(Path(value).resolve(strict=True))
        try:
            return self._check(Path(os.readlink(f"/proc/self/fd/{handle.fileno()}")).resolve(strict=True))
        except OSError as error:
            raise InputAccessError("Cannot verify opened input identity") from error

    def read(self, relative_path, offset=0):
        offset, source = self._offset(offset), self.resolve(relative_path)
        if not source.is_file():
            raise ValueError("Only regular input files can be read")
        capture_dir = self.managed / "projects" / self.research_id / "input-captures" / uuid.uuid4().hex
        capture_dir.mkdir(parents=True, exist_ok=False)
        capture = capture_dir / source.name
        try:
            with source.open("rb") as original, capture.open("xb") as copied:
                self._opened_path(original)
                before = os.fstat(original.fileno())
                sha = hashlib.sha256()
                while chunk := original.read(1024 * 1024):
                    if not self.progress():
                        raise InputAccessError("Research is stopped")
                    if b"\x00" in chunk:
                        raise InputAccessError("Binary input reading is unsupported")
                    copied.write(chunk)
                    sha.update(chunk)
                after = os.fstat(original.fileno())
                self._opened_path(original)
                if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    raise InputAccessError("Input changed while it was captured")
            if not self.progress():
                raise InputAccessError("Research is stopped")
            source_hash = sha.hexdigest()
            relative = source.relative_to(self.root).as_posix()
            artifact = self.store.register_artifact(self.research_id, capture, mimetypes.guess_type(source.name)[0] or "text/plain",
                line_id=self.context.get("line_id"), source_kind="human_input", source_relative_path=relative, source_sha256=source_hash)
            _, immutable = self.store.artifact(artifact["id"])
            with immutable.open("rb") as handle:
                handle.seek(offset)
                raw = handle.read(self.TRANSPORT_LIMIT)
            result = {"artifact_id": artifact["id"], "source_relative_path": relative, "source_sha256": source_hash,
                "sha256": artifact["sha256"], "offset": offset, "total_bytes": artifact["size_bytes"]}
            # Bound serialized UTF-8 response, including escaped controls/metadata.
            count = len(raw)
            while True:
                decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                text = decoder.decode(raw[:count], final=offset + count >= artifact["size_bytes"])
                count -= len(decoder.getstate()[0])
                result.update(text=text, next_offset=offset + count,
                    eof=offset + count >= artifact["size_bytes"])
                if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) <= self.TRANSPORT_LIMIT:
                    break
                if count == 0:
                    raise InputAccessError("Input path metadata exceeds the transport limit")
                count = max(0, count - max(1, count // 8))
            self.store.emit(self.research_id, "research.input_read", {"artifact_id": artifact["id"], "source_relative_path": relative,
                "source_sha256": source_hash, "offset": offset, "read_bytes": count}, agent_id=self.context["agent_id"],
                task_id=self.context["task_id"])
            return result
        finally:
            if not capture_dir.resolve().is_relative_to(self.managed):
                raise InputAccessError("Input capture cleanup escaped managed storage")
            shutil.rmtree(capture_dir)
