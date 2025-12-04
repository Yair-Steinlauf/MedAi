import pandas as pd
from datetime import datetime

class LoincManager:
    """Manages LOINC codes and their descriptions."""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.loinc_map = self._load_loinc_codes()

    def _load_loinc_codes(self) -> dict:
        try:
            df = pd.read_csv(self.file_path, header=None, names=['loinc_num', 'long_common_name'], usecols=[0, 1], dtype={'loinc_num': str})
            df['loinc_num'] = df['loinc_num'].str.strip()
            return pd.Series(df.long_common_name.values, index=df.loinc_num).to_dict()
        except FileNotFoundError:
            print(f"Error: LOINC file not found at {self.file_path}")
            return {}
        except (KeyError, AttributeError, IndexError):
            print(f"Error: Could not parse 'loinc_num' or 'long_common_name' from {self.file_path}.")
            return {}

    def get_long_name(self, loinc_code: str) -> str:
        return self.loinc_map.get(str(loinc_code).strip(), "Unknown LOINC Code")

class TemporalDataManager:
    """Manages loading, preprocessing, and enriching of the bi-temporal medical data."""
    def __init__(self, file_path: str, loinc_manager: LoincManager):
        self.file_path = file_path
        self.loinc_manager = loinc_manager
        self.df = self._load_and_prepare_data(self.file_path)

    def _load_and_prepare_data(self, file_path) -> pd.DataFrame:
        """
        Loads data from a specific file path, standardizes it, and enriches it.
        """
        try:
            header_df = pd.read_excel(file_path, nrows=0, engine='openpyxl')
            valid_columns = [col for col in header_df.columns if not str(col).startswith('Unnamed:')]
            df = pd.read_excel(file_path, engine='openpyxl', usecols=valid_columns, dtype={'LOINC-NUM': str, 'loinc-num': str})
        except (FileNotFoundError, ImportError) as e:
            print(f"Error during file loading: {e}")
            return pd.DataFrame()

        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        if 'loinc_num' in df.columns:
            df.rename(columns={'loinc_num': 'loinc_code'}, inplace=True)
        
        if 'loinc_code' in df.columns:
            df['loinc_code'] = df['loinc_code'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            df['concept_name'] = df['loinc_code'].map(self.loinc_manager.loinc_map)
            df['concept_name'] = df['concept_name'].fillna('Unknown Concept')
        else:
            # This is a critical failure for a file to be valid
            raise ValueError("'loinc_num' or 'loinc_code' column not found in the file.")

        if 'valid_stop_time' not in df.columns:
            df['valid_stop_time'] = pd.NaT

        time_cols = ['valid_start_time', 'transaction_time']
        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                raise ValueError(f"Required time column '{col}' not found in the file.")
        
        df['valid_stop_time'] = pd.to_datetime(df['valid_stop_time'], errors='coerce')

        end_of_time = pd.Timestamp.max
        df['valid_stop_time'] = df['valid_stop_time'].fillna(end_of_time)
        
        return df

    def append_data_from_file(self, new_file_path: str):
        """
        Loads a new data file, prepares it, and appends it to the main DataFrame.
        """
        try:
            print(f"Loading and preparing new data from: {new_file_path}")
            new_df = self._load_and_prepare_data(new_file_path)
            
            if new_df.empty:
                raise ValueError("The new data file is empty or could not be loaded.")

            # Append the new data
            self.df = pd.concat([self.df, new_df], ignore_index=True)
            
            # Crucially, re-sort the entire dataset by transaction time
            self.df.sort_values(by='transaction_time', ascending=False, inplace=True)
            
            print("Data appended and system re-sorted successfully.")
            return {"success": True, "rows_added": len(new_df)}
        
        except Exception as e:
            print(f"Error appending data: {e}")
            return {"success": False, "error": str(e)}
