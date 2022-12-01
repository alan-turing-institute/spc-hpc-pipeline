from time import sleep
from simple_term_menu import TerminalMenu
import connection
import datetime

RUNNING = [True]
JOBS = []
TASKS = []
BATCH_CONN = []


def get_conn():
    if len(BATCH_CONN):
        return BATCH_CONN[0]
    print("Connection doesn't exist, making a new one...")
    BATCH_CONN.append(connection.getBatchServiceClient())
    return BATCH_CONN[0]
    
def clear_vars():
    for l in [JOBS, TASKS, BATCH_CONN]:
        if len(l):
            l.pop()
    print("Cleared local data...")
    sleep(2)

def quit():
    RUNNING[0] = False
        
def job_to_str(job):
    return f"---\nJob: {job.id}\nState: {job.state}\nCreated on: {job.creation_time.strftime('%H:%M:%S on %d/%m/%Y')}\nModified on: {job.last_modified.strftime('%H:%M:%S on %d/%m/%Y')}\nCurrent Run time: {str(datetime.datetime.now(datetime.timezone.utc) - job.creation_time)}\n---"

def task_to_str(task):
    return f"---\nJob: {task.id}\nState: {task.state}\nCreated on: {task.creation_time.strftime('%H:%M:%S on %d/%m/%Y')}\nModified on: {task.last_modified.strftime('%H:%M:%S on %d/%m/%Y')}\nCurrent Run time: {str(datetime.datetime.now(datetime.timezone.utc) - task.creation_time)}\n---"


def report_jobs():
    conn = get_conn()
    if not len(JOBS):
        JOBS.append(connection.get_all_jobs(conn))
    for j in JOBS[0]:
        print(job_to_str(j))
    _=input("Press Enter to return...")

    
def report_tasks():
    conn = get_conn()
    if not len(JOBS):
        JOBS.append(connection.get_all_jobs(conn))
    for j in JOBS[0]:
        print(f"--- JOB: {j.id} ---")
        TASKS.append( connection.get_all_jobs() )
        print(job_to_str(j))
    _=input("Press Enter to return...")


def clean():
    pass

def main():

    options = ["Check job(s)", "Check task(s)", "Clean up completed jobs", "Reset local data", "Exit"]
    option_funcs = {options[0]:report_jobs, options[1]:report_tasks, options[2]:clean, options[3]:clear_vars, options[-1]:quit}
    terminal_menu = TerminalMenu(options, clear_screen=True)

    while(RUNNING[0]):
        menu_idx = terminal_menu.show()
        option_funcs[options[menu_idx]]()

if __name__ == "__main__":
    main()