from cmd import Cmd
from scanner import Scanner
from atr import *
from data_client import *

class SockCli(Cmd):
    def do_exit(self, inp):
        scanner.stop_update_loop()
        return True

    def do_rising(self, inp):
        print(scanner.get_rising_stocks())

    def do_falling(self, inp):
        print(scanner.get_falling_stocks())

    def do_atr(self, inp):
        with scanner.data_mutex:
            try:
                args = inp.split(' ')
                if len(args) < 2:
                    print("Incomplete arguments. Put a ticker and a timeframe for ATR")
                    return
                print(get_current_atr(args[0], args[1], MixedDataClient()))
            except Exception as e:
                print('Something went wrong: ' + str(e))


scanner = Scanner(MixedDataClient())
scanner.batch_size = 3
SockCli().cmdloop()