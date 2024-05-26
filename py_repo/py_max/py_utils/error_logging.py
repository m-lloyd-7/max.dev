import logging


class ErrorLogger:
    def __init__(self, log_name: str, log_file: str):
        # Creating main inputs
        self._log_name: str = log_name

        # Creating the logger
        self.logger: logging.Logger = logging.getLogger(self._log_name)
        self.logger.setLevel(logging.DEBUG)
        self._formatting = logging.Formatter(
            "[%(asctime)s:%(funcName)s][%(levelname)s]:%(message)s"
        )

        # Extra so we can print out the error statements into the terminal
        self._terminal_handler = logging.StreamHandler()
        self._terminal_handler.setFormatter(self._formatting)

        # Adding the handling for log file and terminal window
        self.logger.addHandler(self._terminal_handler)

    # Defining a series of class functions that record the error
    def LogInfo(self, info_message):
        return self.logger.info(info_message)

    def LogDebug(self, debug_message):
        return self.logger.debug(debug_message)

    def LogWarning(self, warning_message):
        return self.logger.warning(warning_message)

    def LogError(self, error_message):
        return self.logger.error(error_message)

    def LogCritical(self, critical_message):
        return self.logger.critical(critical_message)
