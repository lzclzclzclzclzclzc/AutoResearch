"""Recognize retryable SQLite contention without hiding other database faults."""
import sqlite3


def is_storage_busy(error):
    if getattr(error, "code", None) == "storage_busy":
        return True
    if isinstance(error, sqlite3.OperationalError):
        code = getattr(error, "sqlite_errorcode", None)
        if code is not None:
            return code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
        return str(error).lower() in {"database is locked", "database table is locked", "database is busy"}
    return False
