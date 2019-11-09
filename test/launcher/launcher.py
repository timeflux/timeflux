import sys
import glob
import re
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from subprocess import Popen, PIPE, STDOUT
from threading  import Thread, enumerate
from queue import Queue, Empty

running = False

q = Queue()
ansi_escape = re.compile(r'''
    \x1B    # ESC
    [@-_]   # 7-bit C1 Fe
    [0-?]*  # Parameter bytes
    [ -/]*  # Intermediate bytes
    [@-~]   # Final byte
''', re.VERBOSE)

def on_start(event):
    global running
    button_start['state'] = tk.DISABLED
    command = ['python', '-m', 'timeflux.helpers.handler', 'launch', 'timeflux', '-d', dropdown.get()]
    close_fds = 'posix' in sys.builtin_module_names
    p = Popen(command, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=close_fds, text=True)
    t = Thread(target=read_stream, args=(p.stdout, q))
    t.start()
    running = True
    output.delete('1.0', tk.END)
    button_stop['state'] = tk.NORMAL

def on_stop(event):
    button_stop['state'] = tk.DISABLED
    Popen(['python', '-m', 'timeflux.helpers.handler', 'terminate'])

def on_closing():
    if running:
        messagebox.showwarning('Warning', 'A Timeflux instance is running. Please close it before quitting.')
    else:
        window.destroy()

def read_stream(out, queue):
    global running
    for line in iter(out.readline, b''):
        queue.put(line)
        if ansi_escape.sub('', line).endswith('Terminated\n'):
            break;
    out.close()
    running = False
    button_start['state'] = tk.NORMAL

def read_queue():
    try:
        line = q.get_nowait()
        print(line, end = '')
        output.insert(tk.END, ansi_escape.sub('', line))
    except:
        pass
    window.after(100, read_queue)


window = tk.Tk()
window.geometry('800x400')
window.title('Launcher')

frame_top = tk.Frame(bd=1, relief=tk.RIDGE)
frame_bottom = tk.Frame(bd=1, relief=tk.RIDGE)

dropdown = ttk.Combobox(frame_top, values=glob.glob('*.yaml'))
dropdown.current(0)
button_start = tk.Button(frame_top, text='Start', fg='green')
button_stop = tk.Button(frame_top, text='Stop', fg='red', state=tk.DISABLED)
output = scrolledtext.ScrolledText(frame_bottom)

frame_top.pack(fill=tk.BOTH, padx=5, pady=5)
frame_bottom.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
dropdown.pack(padx=5, pady=5, fill=tk.X, side=tk.LEFT)
button_start.pack(padx=5, pady=5, side=tk.LEFT)
button_stop.pack(padx=5, pady=5, side=tk.LEFT)
output.pack(fill=tk.BOTH, expand=True)

button_start.bind('<Button-1>', on_start)
button_stop.bind('<Button-1>', on_stop)
window.protocol('WM_DELETE_WINDOW', on_closing)

read_queue()

window.mainloop()