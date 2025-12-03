import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
from dateutil.parser import parse as date_parse
from dateutil.parser._parser import ParserError
from temporal_db.data_manager import TemporalDataManager, LoincManager
from temporal_db.query_engine import TemporalQueryEngine
import os
import pandas as pd

class App(tk.Tk):
    def __init__(self, engine):
        super().__init__()
        self.engine = engine
        self.title("Bi-Temporal DBMS - 2025")
        self.geometry("950x650")

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(pady=10, padx=10, fill="both", expand=True)

        self.q1_frame = ttk.Frame(self.notebook)
        self.q2_frame = ttk.Frame(self.notebook)
        self.q3_frame = ttk.Frame(self.notebook)
        self.q4_frame = ttk.Frame(self.notebook)

        self.notebook.add(self.q1_frame, text="Query 1: Point-in-Time")
        self.notebook.add(self.q2_frame, text="Query 2: History")
        self.notebook.add(self.q3_frame, text="Query 3 (Coming Soon)")
        self.notebook.add(self.q4_frame, text="Query 4 (Coming Soon)")

        self.setup_query1_tab()
        self.setup_query2_tab()

    def setup_query1_tab(self):
        frame = self.q1_frame
        input_frame = ttk.LabelFrame(frame, text="Query Parameters")
        input_frame.pack(fill="x", padx=10, pady=10)
        
        ttk.Label(input_frame, text="First Name:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.q1_first_name = ttk.Entry(input_frame, width=30)
        self.q1_first_name.grid(row=0, column=1, columnspan=2, padx=5, pady=5, sticky="w")
        ttk.Label(input_frame, text="Last Name:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.q1_last_name = ttk.Entry(input_frame, width=30)
        self.q1_last_name.grid(row=1, column=1, columnspan=2, padx=5, pady=5, sticky="w")
        ttk.Label(input_frame, text="LOINC Code:").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.q1_loinc = ttk.Entry(input_frame, width=30)
        self.q1_loinc.grid(row=2, column=1, columnspan=2, padx=5, pady=5, sticky="w")
        ttk.Label(input_frame, text="Valid Time:").grid(row=3, column=0, padx=5, pady=5, sticky="w")
        self.q1_valid_date = DateEntry(input_frame, width=12, date_pattern='y-mm-dd')
        self.q1_valid_date.grid(row=3, column=1, padx=5, pady=5, sticky="w")
        self.q1_valid_time = ttk.Entry(input_frame, width=10)
        self.q1_valid_time.grid(row=3, column=2, padx=5, pady=5, sticky="w")
        self.q1_valid_time.insert(0, "HH:MM")
        ttk.Label(input_frame, text="Transaction Time (Optional):").grid(row=4, column=0, padx=5, pady=5, sticky="w")
        self.q1_trans_date = DateEntry(input_frame, width=12, date_pattern='y-mm-dd')
        self.q1_trans_date.grid(row=4, column=1, padx=5, pady=5, sticky="w")
        self.q1_trans_time = ttk.Entry(input_frame, width=10)
        self.q1_trans_time.grid(row=4, column=2, padx=5, pady=5, sticky="w")
        self.q1_trans_time.insert(0, "HH:MM")
        self.q1_trans_date.set_date(None)
        self.q1_trans_time.delete(0, tk.END)
        run_button = ttk.Button(frame, text="Run Point-in-Time Query", command=self.run_query1)
        run_button.pack(pady=10)
        for widget in input_frame.winfo_children():
            if isinstance(widget, (ttk.Entry, DateEntry)):
                widget.bind("<Return>", self.run_query1)
        result_frame = ttk.LabelFrame(frame, text="Query Result")
        result_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.q1_result_tree = ttk.Treeview(result_frame, columns=("Attribute", "Value"), show="headings")
        self.q1_result_tree.heading("Attribute", text="Attribute")
        self.q1_result_tree.heading("Value", text="Value")
        self.q1_result_tree.column("Attribute", width=200)
        self.q1_result_tree.column("Value", width=500)
        self.q1_result_tree.pack(fill="both", expand=True, padx=5, pady=5)

    def setup_query2_tab(self):
        frame = self.q2_frame
        input_frame = ttk.LabelFrame(frame, text="Query Parameters")
        input_frame.pack(fill="x", padx=10, pady=10)

        ttk.Label(input_frame, text="First Name:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.q2_first_name = ttk.Entry(input_frame, width=40)
        self.q2_first_name.grid(row=0, column=1, columnspan=3, padx=5, pady=5, sticky="w")

        ttk.Label(input_frame, text="Last Name:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.q2_last_name = ttk.Entry(input_frame, width=40)
        self.q2_last_name.grid(row=1, column=1, columnspan=3, padx=5, pady=5, sticky="w")

        ttk.Label(input_frame, text="LOINC Code (Optional):").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.q2_loinc = ttk.Entry(input_frame, width=40)
        self.q2_loinc.grid(row=2, column=1, columnspan=3, padx=5, pady=5, sticky="w")

        ttk.Label(input_frame, text="Concept Name (Optional):").grid(row=3, column=0, padx=5, pady=5, sticky="w")
        self.q2_concept = ttk.Entry(input_frame, width=40)
        self.q2_concept.grid(row=3, column=1, columnspan=3, padx=5, pady=5, sticky="w")

        ttk.Label(input_frame, text="Valid Time Range (Optional):").grid(row=4, column=0, padx=5, pady=5, sticky="w")
        self.q2_valid_start_date = DateEntry(input_frame, width=12, date_pattern='y-mm-dd')
        self.q2_valid_start_date.grid(row=4, column=1, padx=5, pady=5, sticky="w")
        self.q2_valid_start_time = ttk.Entry(input_frame, width=10)
        self.q2_valid_start_time.grid(row=4, column=2, padx=5, pady=5, sticky="w")
        self.q2_valid_start_date.set_date(None)
        self.q2_valid_start_date.delete(0, tk.END)
        self.q2_valid_start_time.insert(0, "HH:MM")
        
        ttk.Label(input_frame, text="to").grid(row=4, column=3)

        self.q2_valid_end_date = DateEntry(input_frame, width=12, date_pattern='y-mm-dd')
        self.q2_valid_end_date.grid(row=4, column=4, padx=5, pady=5, sticky="w")
        self.q2_valid_end_time = ttk.Entry(input_frame, width=10)
        self.q2_valid_end_time.grid(row=4, column=5, padx=5, pady=5, sticky="w")
        self.q2_valid_end_date.set_date(None)
        self.q2_valid_end_date.delete(0, tk.END)
        self.q2_valid_end_time.insert(0, "HH:MM")

        ttk.Label(input_frame, text="Point of View (Optional):").grid(row=5, column=0, padx=5, pady=5, sticky="w")
        self.q2_trans_date = DateEntry(input_frame, width=12, date_pattern='y-mm-dd')
        self.q2_trans_date.grid(row=5, column=1, padx=5, pady=5, sticky="w")
        self.q2_trans_time = ttk.Entry(input_frame, width=10)
        self.q2_trans_time.grid(row=5, column=2, padx=5, pady=5, sticky="w")
        self.q2_trans_date.set_date(None)
        self.q2_trans_date.delete(0, tk.END)
        self.q2_trans_time.insert(0, "HH:MM") # Added placeholder

        run_button = ttk.Button(frame, text="Run History Query", command=self.run_query2)
        run_button.pack(pady=10)

        for widget in input_frame.winfo_children():
            if isinstance(widget, (ttk.Entry, DateEntry)):
                widget.bind("<Return>", self.run_query2)

        result_frame = ttk.LabelFrame(frame, text="Query Result")
        result_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.q2_count_label = ttk.Label(result_frame, text="Records found: 0")
        self.q2_count_label.pack(anchor="w", padx=5)

        cols = ('first_name', 'last_name', 'concept_name', 'loinc_code', 'value', 'valid_start_time', 'valid_stop_time', 'transaction_time')
        self.q2_result_tree = ttk.Treeview(result_frame, columns=cols, show="headings")
        for col in cols:
            self.q2_result_tree.heading(col, text=col.replace('_', ' ').title())
            self.q2_result_tree.column(col, width=110)
        self.q2_result_tree.pack(fill="both", expand=True, padx=5, pady=5)

    def _parse_datetime(self, date_widget, time_widget):
        raw_date = date_widget.get().strip()
        if not raw_date: return None
        try:
            parsed_date = date_parse(raw_date).strftime('%Y-%m-%d')
            time_str = time_widget.get().strip()
            
            if time_str and time_str.lower() != "hh:mm":
                # Smart time parsing logic
                if time_str.isdigit() and len(time_str) in [3, 4]:
                    if len(time_str) == 3: # HMM format
                        h, mm = time_str[0], time_str[1:]
                    else: # HHMM format
                        h, mm = time_str[:2], time_str[2:]
                    time_str = f"{h}:{mm}"
                
                parsed_time = date_parse(time_str).strftime('%H:%M')
                return f"{parsed_date} {parsed_time}"
            return parsed_date
        except (ValueError, ParserError):
            return "invalid_date"

    def run_query1(self, event=None):
        for i in self.q1_result_tree.get_children(): self.q1_result_tree.delete(i)
        try:
            valid_datetime_str = self._parse_datetime(self.q1_valid_date, self.q1_valid_time)
            if valid_datetime_str == "invalid_date": raise ValueError("Invalid format for Valid Time.")
            trans_datetime_str = self._parse_datetime(self.q1_trans_date, self.q1_trans_time)
            if trans_datetime_str == "invalid_date": raise ValueError("Invalid format for Transaction Time.")

            params = {"first_name": self.q1_first_name.get(), "last_name": self.q1_last_name.get(), "loinc_code": self.q1_loinc.get(), "valid_time": valid_datetime_str, "transaction_time": trans_datetime_str}
            if not all([params["first_name"], params["last_name"], params["loinc_code"], params["valid_time"]]):
                raise ValueError("First Name, Last Name, LOINC, and Valid Time are required.")
            result = self.engine.point_in_time_query(**params)
            if 'error' in result: self.q1_result_tree.insert("", "end", values=("Error", result['error']))
            else:
                display_order = ['first_name', 'last_name', 'concept_name', 'loinc_code', 'value', 'unit', 'valid_start_time', 'valid_stop_time', 'transaction_time']
                displayed_keys = set()
                for key in display_order:
                    if key in result:
                        value = result[key]
                        if isinstance(value, pd.Timestamp): value = value.strftime('%Y-%m-%d %H:%M:%S')
                        self.q1_result_tree.insert("", "end", values=(key, value))
                        displayed_keys.add(key)
                for key, value in result.items():
                    if key not in displayed_keys:
                        if isinstance(value, pd.Timestamp): value = value.strftime('%Y-%m-%d %H:%M:%S')
                        self.q1_result_tree.insert("", "end", values=(key, value))
        except (ValueError, ParserError) as e:
            self.q1_result_tree.insert("", "end", values=("Input Error", str(e)))

    def run_query2(self, event=None):
        for i in self.q2_result_tree.get_children(): self.q2_result_tree.delete(i)
        self.q2_count_label.config(text="Records found: 0")

        try:
            valid_start = self._parse_datetime(self.q2_valid_start_date, self.q2_valid_start_time)
            if valid_start == "invalid_date": raise ValueError("Invalid format for Valid Start Time.")
            valid_end = self._parse_datetime(self.q2_valid_end_date, self.q2_valid_end_time)
            if valid_end == "invalid_date": raise ValueError("Invalid format for Valid End Time.")
            transaction_time = self._parse_datetime(self.q2_trans_date, self.q2_trans_time)
            if transaction_time == "invalid_date": raise ValueError("Invalid format for Point of View Time.")

            params = {
                "first_name": self.q2_first_name.get(),
                "last_name": self.q2_last_name.get(),
                "loinc_code": self.q2_loinc.get() or None,
                "concept_name": self.q2_concept.get() or None,
                "valid_start": valid_start,
                "valid_end": valid_end,
                "transaction_time": transaction_time
            }

            if not all([params["first_name"], params["last_name"]]):
                raise ValueError("First Name and Last Name are required.")

            result = self.engine.history_query(**params)
            
            self.q2_count_label.config(text=f"Records found: {result.get('count', 0)}")

            if 'error' in result:
                self.q2_result_tree.insert("", "end", values=(result['error'], "", "", "", "", "", "", ""))
            elif 'data' in result:
                for record in result['data']:
                    row = [record.get(col, '') for col in self.q2_result_tree['columns']]
                    for i, val in enumerate(row):
                        if isinstance(val, pd.Timestamp):
                            row[i] = val.strftime('%Y-%m-%d %H:%M:%S')
                    self.q2_result_tree.insert("", "end", values=row)

        except (ValueError, ParserError) as e:
            self.q2_result_tree.insert("", "end", values=(str(e), "", "", "", "", "", "", ""))


def main():
    print("Initializing Bi-Temporal DBMS...")
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(project_root, 'project_db_2025.xlsx')
    loinc_file = os.path.join(project_root, 'LOINCTONAME.csv')

    try:
        from dateutil.parser import parse
    except ImportError:
        root = tk.Tk()
        root.title("Dependency Error")
        ttk.Label(root, text="Required library 'python-dateutil' is not installed.\nPlease run 'pip install python-dateutil'").pack(padx=20, pady=20)
        root.mainloop()
        return

    try:
        loinc_manager = LoincManager(file_path=loinc_file)
        data_manager = TemporalDataManager(file_path=data_file, loinc_manager=loinc_manager)
    except Exception as e:
        root = tk.Tk()
        root.title("Initialization Error")
        ttk.Label(root, text=f"Failed to initialize application: {e}").pack(padx=20, pady=20)
        root.mainloop()
        return

    if data_manager.df.empty:
        root = tk.Tk()
        root.title("Data Loading Error")
        ttk.Label(root, text="Failed to load data. Check console for details.").pack(padx=20, pady=20)
        root.mainloop()
        return

    engine = TemporalQueryEngine(data_manager, loinc_manager)
    print("System Initialized. Launching GUI.")
    app = App(engine)
    app.mainloop()

if __name__ == "__main__":
    main()
