import argparse

from transaction.manager import TransactionManager


def main(arguments):
    manager = TransactionManager()
    if arguments.file and arguments.std:
        print("You can choose only one input method at one time."
              "The usage should be python main.py --file/--std")
    elif arguments.file:
        while True:
            print("Please input file path:")
            input_file = input('> ')
            try:
                print("Getting inputs from {}".format(input_file))
                with open(input_file, 'r') as f:
                    for line in f:
                        manager.process(line)
            except IOError:
                print("Error, can not open " + input_file)
            is_continue = input('Continue[y/n]?')
            if is_continue.lower() == 'n':
                break
    else:
        print("Standard input, use 'exit' to exit.")
        while True:
            cmd = input('> ')
            if cmd != 'exit':
                manager.process(cmd)
            else:
                break
    print('Bye')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Choose whether to get input from the keyboard or the file')

    parser.add_argument('--file', action='store_true', help='whether to get input from file')
    parser.add_argument('--std', action='store_true', help='whether to get input from standard input')

    args = parser.parse_args()
    main(args)
