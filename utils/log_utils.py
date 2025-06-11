import logging
import pendulum
import inspect


class LogBlock:
    def __init__(self, logger_name: str = __name__, max_depth: int = 2):
        self.logger = logging.getLogger(logger_name)
        self.max_depth = max_depth

    def _get_caller_info(self):
        stack = inspect.stack()
        trace = []

        for frame_info in stack[1:self.max_depth + 1]:
            filename = frame_info.filename.split("/")[-1]
            function = frame_info.function

            if "log_utils.py" not in filename:
                trace.append(f"{filename}::{function}")

        if trace:
            return " -> ".join(reversed(trace))
        return "unknown"

    def _format_log(self, key: str = None, message: str = None, timezone: str = "America/Los_Angeles") -> str:
        utc_time = pendulum.now("UTC").to_datetime_string()
        caller = self._get_caller_info()

        log_block = f"\n[{key}]" if key else ""
        log_block += f"\ncaller: {caller}"

        if timezone:
            local_time = pendulum.now(timezone).to_datetime_string()
            log_block += f"\nlog time in {timezone}: {local_time}"

        log_block += f"\nlog time in UTC: {utc_time}"
        log_block += f"\nmessage:\n{message}"

        return log_block.strip()

    def info(self, key: str = None, message: str = None, timezone: str = "America/Los_Angeles"):
        self.logger.info(self._format_log(key, message, timezone))

    def warning(self, key: str = None, message: str = None, timezone: str = "America/Los_Angeles"):
        self.logger.warning(self._format_log(key, message, timezone))

    def error(self, key: str = None, message: str = None, timezone: str = "America/Los_Angeles"):
        self.logger.error(self._format_log(key, message, timezone))

    def debug(self, key: str = None, message: str = None, timezone: str = "America/Los_Angeles"):
        self.logger.debug(self._format_log(key, message, timezone))
