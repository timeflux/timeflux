import sys
import glob
import re
import signal
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from subprocess import Popen, PIPE, STDOUT
from threading  import Thread, enumerate
from queue import Queue, Empty

class Launcher():

    def __init__(self):
        self.running = False
        self.queue = Queue()
        self.ansi_escape = re.compile(r'''
            \x1B    # ESC
            [@-_]   # 7-bit C1 Fe
            [0-?]*  # Parameter bytes
            [ -/]*  # Intermediate bytes
            [@-~]   # Final byte
        ''', re.VERBOSE)
        if sys.platform == 'win32':
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        self._init_gui()

    def start(self):
        self._check_status()
        self._read_queue()
        self.window.mainloop()

    def _init_gui(self):
        self.window = tk.Tk()
        self.window.geometry('800x400')
        self.window.title('Launcher')
        frame_top = tk.Frame(bd=1, relief=tk.RIDGE)
        frame_bottom = tk.Frame(bd=1, relief=tk.RIDGE)
        self.dropdown = ttk.Combobox(frame_top, values=glob.glob('*.yaml'))
        self.dropdown.current(0)
        self.button_start = tk.Button(frame_top, text='Start', fg='green')
        self.button_stop = tk.Button(frame_top, text='Stop', fg='red', state=tk.DISABLED)
        self.output = scrolledtext.ScrolledText(frame_bottom)
        frame_top.pack(fill=tk.BOTH, padx=5, pady=5)
        frame_bottom.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.dropdown.pack(padx=5, pady=5, fill=tk.X, side=tk.LEFT)
        self.button_start.pack(padx=5, pady=5, side=tk.LEFT)
        self.button_stop.pack(padx=5, pady=5, side=tk.LEFT)
        self.output.pack(fill=tk.BOTH, expand=True)
        self.button_start.bind('<Button-1>', self._on_start)
        self.button_stop.bind('<Button-1>', self._on_stop)
        self.window.protocol('WM_DELETE_WINDOW', self._on_closing)

    def _on_start(self, event):
        if self.button_start['state'] == tk.DISABLED: return
        command = ['python', '-m', 'timeflux.helpers.handler', 'launch', 'timeflux', '-d', self.dropdown.get()]
        p = Popen(command, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=True, text=True)
        t = Thread(target=self._read_stream, args=(p.stdout, self.queue))
        t.start()
        self.running = True
        self.output.delete('1.0', tk.END)
        self.button_start['state'] = tk.DISABLED
        self.button_stop['state'] = tk.NORMAL

    def _on_stop(self, event):
        if self.button_stop['state'] == tk.DISABLED: return
        command = ['python', '-m', 'timeflux.helpers.handler', 'terminate']
        Popen(command)

    def _on_closing(self):
        if self.running:
            messagebox.showwarning('Warning', 'A Timeflux instance is running. Please stop it before quitting.')
        else:
            self.window.destroy()

    def _read_stream(self, out, queue):
        for line in iter(out.readline, b''):
            queue.put(line)
            if self.ansi_escape.sub('', line).endswith('Terminated\n'):
                break;
        out.close()
        self.running = False

    def _read_queue(self):
        try:
            line = self.queue.get_nowait()
            print(line, end = '')
            self.output.insert(tk.END, self.ansi_escape.sub('', line))
        except:
            pass
        self.window.after(100, self._read_queue)

    def _check_status(self):
        if self.running == False:
            self.button_stop['state'] = tk.DISABLED
            self.button_start['state'] = tk.NORMAL
        self.window.after(100, self._check_status)


if __name__ == '__main__':
    Launcher().start()
