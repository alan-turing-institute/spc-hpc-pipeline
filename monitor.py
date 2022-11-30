from simple_term_menu import TerminalMenu


RUNNING = [True]
JOBS = []
TASKS = []
BATCH_CONN = []

def quit():
    RUNNING[0] = False
        
def report_jobs():
    pass
def report_tasks():
    pass
def clean():
    pass

def main():

    options = ["Check job(s)", "Check task(s)", "Clean up completed jobs", "Exit"]
    option_funcs = {options[0]:report_jobs, options[1]:report_tasks, options[2]:clean, options[3]:quit}
    terminal_menu = TerminalMenu(options)

    while(RUNNING[0]):
        menu_idx = terminal_menu.show()
        option_funcs[options[menu_idx]]()

if __name__ == "__main__":
    main()