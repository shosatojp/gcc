import subprocess

PROGRESS = 0
DEBUG = 1
INFO = 2
WARN = 3
ERROR = 4
FATAL = 5

# リアルタイムで通知したいので同期的に


NETWORK = '>'
FILEIO = '.'


class Reporter():
    def __init__(self, loglevel, handler=None, handlelevel=3):
        self.loglevel = loglevel
        self.handler = handler
        self.handlelevel = handlelevel

    def report(self, level: int, message: str, end='\n', type=' ', ** args):
        if level == PROGRESS or level >= self.loglevel:
            print(type*level, message, end=end)

        if level >= self.handlelevel and self.handler:
            subprocess.run([self.handler, str(level), message])
