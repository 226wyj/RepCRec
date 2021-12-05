import re

from errors import ParseError


class Parser:
    def __init__(self):
        self.commands = {
            'begin', 'end', 'W', 'R',
            'dump', 'beginRO', 'fail', 'recover'
        }
        self.is_hint = False

    def parse(self, line: str):
        if self.is_hint:
            return
        line = line.split('//')[0].strip()
        if line != '':
            if line.startswith('==='):
                self.is_hint = True
                return
            res = re.findall(r'\w+', line)
            cmd = res[0]
            if cmd not in self.commands:
                raise ParseError(
                    "Unknown command {}, the valid commands are {}, please check".format(cmd, self.commands))
            return res

# Test if parser works.
# if __name__ == '__main__':
#     parser = Parser()
#     while (command := input('> ')) != 'exit':
#         print(parser.parse(command))
