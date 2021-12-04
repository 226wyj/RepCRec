import re
from typing import List


class Parser:
    def __init__(self):
        self.commands = {
            'begin', 'end', 'W', 'R',
            'dump', 'beginRO', 'fail', 'recover'
        }

    def parse(self, line: str) -> List[str]:
        # print("Current line: ")
        # print(line)
        line = line.strip()
        is_valid = (line.split('//')[0].strip() != '')
        if is_valid:
            res = re.findall(r'\w+', line)
            print(res)
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
