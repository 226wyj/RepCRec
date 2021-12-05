import re

from errors import ParseError


class Parser:
    def __init__(self):
        self.commands = {
            'begin', 'end', 'W', 'R',
            'dump', 'beginRO', 'fail', 'recover'
        }
        self.is_output_message = False

    def parse(self, line: str):
        if self.is_output_message:
            return
        line = line.strip()
        is_valid = (line.split('//')[0].strip() != '')
        if is_valid:
            if line.startswith('==='):
                self.is_output_message = True
                return
            print(line, end='\t\t==>\t\t')
            res = re.findall(r'\w+', line)
            cmd = res[0]
            if cmd not in self.commands:
                raise ParseError(
                    "Unknown command {}, the allowed command should be " \
                    "within [begin, end, W, R, dump, beginRO, fail, recover]," \
                    "please check.".format(cmd))
            return res

# Test code
# if __name__ == '__main__':
#     parser = Parser()
#     while (command := input('> ')) != 'exit':
#         print(parser.parse(command))
