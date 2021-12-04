import re
from typing import List


class Parser:
    def __init__(self):
        self.commands = {
            'begin', 'end', 'W', 'R',
            'dump', 'beginRO', 'fail', 'recover'
        }

    def parse(self, line: str) -> List[str]:
        line = line.strip()
        if not line.startswith('\\'):
            res = re.findall(r'\w+', line)
            cmd = res[0]
            if cmd not in self.commands:
                raise "Error, unknown command {}, the allowed command should be " \
                      "within [begin, end, W, R, dump, beginRO, fail, recover]," \
                      "please check.".format(cmd)
            return res


# Test code
# if __name__ == '__main__':
#     parser = Parser()
#     while (command := input('> ')) != 'exit':
#         print(parser.parse(command))
