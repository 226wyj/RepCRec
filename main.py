import argparse
import os

from transaction.manager import TransactionManager


def main(arguments):
    """Main function, used for getting inputs.

    @author Yuejiang Wu
    """
    if arguments.file and not (arguments.std or arguments.dir):
        while True:
            manager = TransactionManager()
            print("Please input file path:")
            input_file = input('> ')
            try:
                print("Getting inputs from {}".format(input_file))
                with open(input_file, 'r') as f:
                    for line in f:
                        manager.process(line)
                is_continue = input('Continue[y/n]?')
                while is_continue.lower() != 'y' and is_continue.lower() != 'n':
                    is_continue = input('Continue[y/n]?')
                if is_continue.lower() == 'n':
                    break
            except IOError:
                print("Error, can not open " + input_file)
    elif arguments.std and not (arguments.file or arguments.dir):
        manager = TransactionManager()
        print("Standard input, use 'exit' to exit.")
        while True:
            cmd = input('> ')
            if cmd != 'exit':
                manager.process(cmd)
            else:
                break
    elif arguments.dir and not (arguments.std or arguments.file):
        print("Please input the root directory: ")
        root_dir = input('> ')
        files = [os.path.join(root_dir, file_name) for file_name in os.listdir(root_dir)]
        for file in files:
            manager = TransactionManager()
            try:
                print("Getting inputs from {}".format(file))
                with open(file, 'r') as f:
                    for line in f:
                        manager.process(line)
                print()
            except IOError:
                print("Error, can not open " + file)
    else:
        print("You should choose one and only one input method at one time."
              "The usage should be python main.py [--file] | [--std] | [--dir]")
    print('Bye')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Choose whether to get input from the keyboard or the file')

    parser.add_argument('--file', action='store_true', help='whether to get input from file')
    parser.add_argument('--std', action='store_true', help='whether to get input from standard input')
    parser.add_argument('--dir', action='store_true', help='whether to get input from a directory.')

    args = parser.parse_args()
    main(args)
