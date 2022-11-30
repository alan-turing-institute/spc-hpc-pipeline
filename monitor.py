from simple_term_menu import TerminalMenu


RUNNING = [True]
def quit():
    RUNNING[0] = False
        

def main():

    def f():
        pass

    options = ["Check job(s)", "Check task(s)", "Clean up completed jobs", "Exit"]
    option_funcs = {options[0]:f, options[1]:f, options[2]:f, options[3]:quit}
    terminal_menu = TerminalMenu(options)

    while(RUNNING[0]):
        menu_idx = terminal_menu.show()
        option_funcs[options[menu_idx]]()

if __name__ == "__main__":
    main()