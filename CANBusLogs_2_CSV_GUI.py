import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import subprocess
import os
import sys
import tempfile
import shutil

class CANBusLogs2CSVGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("CANBusLogs_2_CSV GUI")
        self.geometry("600x450")
        self.resizable(False, True)

        # Variables
        self.log_file = tk.StringVar()
        self.dbc_files = []
        self.output_file = tk.StringVar(value="output.csv")
        self.delimiter = tk.StringVar(value=";")
        self.name_mode = tk.StringVar(value="message.signal")
        self.message_counter = tk.BooleanVar(value=True)
        self.message_pulser = tk.BooleanVar(value=True)

        self.create_widgets()

    def create_widgets(self):
        padx = 10
        pady = 6

        # Log file selection
        log_frame = ttk.LabelFrame(self, text="Log file (CAN log or MDF4 *.mf4)")
        log_frame.pack(fill="x", padx=padx, pady=pady)
        log_entry = ttk.Entry(log_frame, textvariable=self.log_file, width=60)
        log_entry.pack(side="left", padx=5, pady=5)
        log_btn = ttk.Button(log_frame, text="Browse...", command=self.browse_log)
        log_btn.pack(side="left", padx=5)

        # DBC files selection
        dbc_frame = ttk.LabelFrame(self, text="DBC files (multiple allowed)")
        dbc_frame.pack(fill="x", padx=padx, pady=pady)
        self.dbc_listbox = tk.Listbox(dbc_frame, height=4, width=55, selectmode=tk.BROWSE)
        self.dbc_listbox.pack(side="left", padx=5, pady=5)
        dbc_btn_frame = tk.Frame(dbc_frame)
        dbc_btn_frame.pack(side="left")
        ttk.Button(dbc_btn_frame, text="Add...", command=self.add_dbc).pack(fill="x", pady=1)
        ttk.Button(dbc_btn_frame, text="Remove", command=self.remove_dbc).pack(fill="x", pady=1)

        # Output file selection
        output_frame = ttk.LabelFrame(self, text="Output CSV file")
        output_frame.pack(fill="x", padx=padx, pady=pady)
        output_entry = ttk.Entry(output_frame, textvariable=self.output_file, width=60)
        output_entry.pack(side="left", padx=5, pady=5)
        output_btn = ttk.Button(output_frame, text="Browse...", command=self.browse_output)
        output_btn.pack(side="left", padx=5)

        # CSV delimiter
        delim_frame = ttk.Frame(self)
        delim_frame.pack(fill="x", padx=padx, pady=pady)
        ttk.Label(delim_frame, text="CSV delimiter:").pack(side="left")
        delim_entry = ttk.Entry(delim_frame, textvariable=self.delimiter, width=3)
        delim_entry.pack(side="left", padx=5)

        # Signal name mode
        name_mode_frame = ttk.Frame(self)
        name_mode_frame.pack(fill="x", padx=padx, pady=pady)
        ttk.Label(name_mode_frame, text="Signal name mode:").pack(side="left")
        ttk.Radiobutton(name_mode_frame, text="signal", variable=self.name_mode, value="signal").pack(side="left", padx=2)
        ttk.Radiobutton(name_mode_frame, text="message.signal", variable=self.name_mode, value="message.signal").pack(side="left", padx=2)

        # Message counter and pulser
        opts_frame = ttk.Frame(self)
        opts_frame.pack(fill="x", padx=padx, pady=pady)
        ttk.Checkbutton(opts_frame, text="Add message counter signal (--message_counter)", variable=self.message_counter).pack(side="left", padx=2)
        ttk.Checkbutton(opts_frame, text="Add message pulser signal (--message_pulser)", variable=self.message_pulser).pack(side="left", padx=12)

        # Run button
        run_frame = ttk.Frame(self)
        run_frame.pack(fill="x", padx=padx, pady=15)
        run_btn = ttk.Button(run_frame, text="Convert!", command=self.run_conversion)
        run_btn.pack()

        # Output box
        outbox_frame = ttk.LabelFrame(self, text="Output")
        outbox_frame.pack(fill="both", expand=True, padx=padx, pady=pady)
        self.output_text = tk.Text(outbox_frame, height=8, state='disabled')
        self.output_text.pack(fill="both", expand=True, padx=5, pady=5)

    def browse_log(self):
        fname = filedialog.askopenfilename(
            title="Select CAN log or MDF4 file",
            filetypes=[("CAN log / MDF4 / TRC files", "*.log *.txt *.asc *.trc *.mf4"), ("All files", "*.*")]
        )
        if fname:
            self.log_file.set(fname)

    def add_dbc(self):
        files = filedialog.askopenfilenames(title="Select DBC file(s)", filetypes=[("DBC files", "*.dbc"), ("All files", "*.*")])
        for f in files:
            if f not in self.dbc_files:
                self.dbc_files.append(f)
                self.dbc_listbox.insert(tk.END, f)

    def remove_dbc(self):
        sel = self.dbc_listbox.curselection()
        if sel:
            idx = sel[0]
            self.dbc_files.pop(idx)
            self.dbc_listbox.delete(idx)

    def browse_output(self):
        fname = filedialog.asksaveasfilename(title="Save CSV as...", defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if fname:
            self.output_file.set(fname)

    def run_conversion(self):
        # Validate
        log_path = self.log_file.get()
        if not log_path or not os.path.exists(log_path):
            messagebox.showerror("Missing log file", "Please select a valid CAN log file or MDF4 file.")
            return
        if not self.dbc_files or any(not os.path.exists(f) for f in self.dbc_files):
            messagebox.showerror("Missing DBC file", "Please add valid DBC file(s).")
            return

        # Determine if MDF4 and needs conversion
        is_mdf4 = log_path.lower().endswith(".mf4")
        temp_trc = None

        self.output_text.configure(state="normal")
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, "Preparing conversion...\n\n")
        self.output_text.configure(state="disabled")
        self.update_idletasks()

        input_for_converter = log_path
        try:
            if is_mdf4:
                self.output_text.configure(state="normal")
                self.output_text.insert(tk.END, f"Detected MDF4 file: {os.path.basename(log_path)}\n")
                self.output_text.insert(tk.END, "Converting MDF4 to .trc using mdf2peak.exe...\n")
                self.output_text.configure(state="disabled")
                self.update_idletasks()

                # Find mdf2peak.exe
                mdf2peak_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_aux", "mdf2peak.exe")
                if not os.path.exists(mdf2peak_path):
                    messagebox.showerror("mdf2peak.exe not found", f"File not found: {mdf2peak_path}")
                    return

                # Prepare temp .trc file
                temp_dir = os.path.dirname(log_path)
                temp_trc = os.path.join(temp_dir, os.path.basename(os.path.splitext(log_path)[0] + "_CAN.trc")).replace("\\", "/")

                mdf2peak_args = [mdf2peak_path, log_path, "-f", "version2"]
                proc = subprocess.Popen(mdf2peak_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                for line in proc.stdout:
                    self.output_text.configure(state="normal")
                    self.output_text.insert(tk.END, "[mdf2peak] " + line)
                    self.output_text.see(tk.END)
                    self.output_text.configure(state="disabled")
                    self.update_idletasks()
                proc.wait()
                if proc.returncode != 0 or not os.path.exists(temp_trc):
                    messagebox.showerror("mdf2peak.exe error", "Failed to convert MDF4 to TRC. Check output for details.")
                    return
                input_for_converter = temp_trc
                self.output_text.configure(state="normal")
                self.output_text.insert(tk.END, f"TRC file generated: {temp_trc}\n\n")
                self.output_text.configure(state="disabled")
                self.update_idletasks()

            # Arguments for converter
            args = [
                sys.executable if getattr(sys, "frozen", False) == False else sys.executable,
                os.path.abspath("CANBusLogs_2_CSV.py"),
                input_for_converter,
                *self.dbc_files,
                "-o", self.output_file.get(),
                "-d", self.delimiter.get(),
                "-n", self.name_mode.get()
            ]
            if self.message_counter.get():
                args.append("--message_counter")
            if self.message_pulser.get():
                args.append("--message_pulser")

            self.output_text.configure(state="normal")
            self.output_text.insert(tk.END, "Running CANBusLogs_2_CSV.py converter...\n\n")
            self.output_text.configure(state="disabled")
            self.update_idletasks()

            # Run CANBusLogs_2_CSV.py as subprocess and capture output
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in proc.stdout:
                self.output_text.configure(state="normal")
                self.output_text.insert(tk.END, line)
                self.output_text.see(tk.END)
                self.output_text.configure(state="disabled")
                self.update_idletasks()
            proc.wait()
            if proc.returncode == 0:
                messagebox.showinfo("Done", f"CSV file created:\n{self.output_file.get()}")
            else:
                messagebox.showerror("Error", "Conversion failed. Check output for details.")
        except Exception as e:
            messagebox.showerror("Error running script", str(e))
        finally:
            # Clean up temp TRC if created
            if temp_trc and os.path.exists(temp_trc):
                try:
                    os.remove(temp_trc)
                except Exception:
                    pass

if __name__ == "__main__":
    app = CANBusLogs2CSVGUI()
    app.mainloop()