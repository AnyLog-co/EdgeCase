import io
import unittest

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


class SilentStream(io.StringIO):
    """A stream that discards all writes (used to silence unittest default output)."""
    def write(self, *args, **kwargs):
        pass
    def writeln(self, *args, **kwargs):
        pass


class ColorizedResult(unittest.TextTestResult):

    def _short(self, test):
        return test._testMethodName

    def addSuccess(self, test):
        super().addSuccess(test)
        print(f"{GREEN}✔ SUCCESS:{RESET} {self._short(test)}")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        print(f"{RED}✘ FAILURE:{RESET} {self._short(test)}")

    def addError(self, test, err):
        super().addError(test, err)
        print(f"{RED}✘ ERROR:{RESET} {self._short(test)}")


class SilentRunner(unittest.TextTestRunner):
    """A runner that hides all standard unittest output and prints only our colorized logs."""
    def __init__(self, *args, **kwargs):
        kwargs["stream"] = SilentStream()  # silence unittest output
        super().__init__(*args, **kwargs)
        self.resultclass = ColorizedResult
