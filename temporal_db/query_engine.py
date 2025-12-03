import pandas as pd
from datetime import datetime, time

def _normalize_timezone(dt):
    """Helper function to ensure a datetime object is timezone-naive."""
    if dt.tzinfo is not None:
        return dt.tz_localize(None)
    return dt

class TemporalQueryEngine:
    """
    The core engine for performing bi-temporal queries on the medical dataset.
    """
    def __init__(self, data_manager, loinc_manager):
        self.df = data_manager.df
        self.loinc_manager = loinc_manager

    def point_in_time_query(self, first_name: str, last_name: str, loinc_code: str, 
                              valid_time: str, transaction_time: str = None):
        """
        Retrieves the value of a specific measurement for a patient, identified by LOINC code,
        at a given valid time, as known at a specific transaction time.
        """
        try:
            vt = _normalize_timezone(pd.to_datetime(valid_time))
            tt = _normalize_timezone(pd.to_datetime(transaction_time)) if transaction_time else _normalize_timezone(pd.to_datetime(datetime.now()))
            is_date_only = vt.time() == time(0, 0)

            patient_df = self.df[
                (self.df['first_name'].str.lower() == first_name.lower()) &
                (self.df['last_name'].str.lower() == last_name.lower()) &
                (self.df['loinc_code'].str.strip() == loinc_code.strip())
            ]

            if patient_df.empty: return {"error": f"No records found for patient '{first_name} {last_name}' with LOINC code '{loinc_code}'."}
            db_state_at_tt = patient_df[patient_df['transaction_time'] <= tt]
            if db_state_at_tt.empty: return {"error": f"No records for this LOINC code were known to the system at {tt.strftime('%Y-%m-%d %H:%M')}."}
            final_records = db_state_at_tt[(db_state_at_tt['valid_start_time'] <= vt) & (db_state_at_tt['valid_stop_time'] > vt)]
            if final_records.empty: return {"error": f"No measurement found for the specified valid time: {vt.strftime('%Y-%m-%d %H:%M')}."}

            result_record = final_records.sort_values(by='valid_start_time', ascending=False).iloc[0] if is_date_only else final_records.sort_values(by='transaction_time', ascending=False).iloc[0]
            return result_record.to_dict()

        except Exception as e:
            return {"error": f"An unexpected error occurred: {e}"}

    def history_query(self, first_name: str, last_name: str, loinc_code: str = None, concept_name: str = None,
                        valid_start: str = None, valid_end: str = None, transaction_time: str = None):
        """
        Retrieves the history of measurements for a patient where the measurement's start time
        falls within the given valid time range.
        """
        try:
            tt = _normalize_timezone(pd.to_datetime(transaction_time)) if transaction_time else _normalize_timezone(pd.to_datetime(datetime.now()))
            vs = _normalize_timezone(pd.to_datetime(valid_start)) if valid_start else pd.Timestamp.min
            ve = _normalize_timezone(pd.to_datetime(valid_end)) if valid_end else pd.Timestamp.max
            
            patient_df = self.df[
                (self.df['first_name'].str.lower() == first_name.lower()) &
                (self.df['last_name'].str.lower() == last_name.lower())
            ]
            
            if loinc_code: patient_df = patient_df[patient_df['loinc_code'].str.strip() == loinc_code.strip()]
            elif concept_name: patient_df = patient_df[patient_df['concept_name'].str.lower() == concept_name.lower()]

            if patient_df.empty: return {"error": "No records found for this patient and criteria.", "count": 0}

            db_state_at_tt = patient_df[patient_df['transaction_time'] <= tt]
            if db_state_at_tt.empty: return {"error": f"No records were known to the system at {tt.strftime('%Y-%m-%d %H:%M')}.", "count": 0}

            # --- MODIFIED LOGIC ---
            # Find records where the *start time* is within the query range.
            final_records = db_state_at_tt[
                (db_state_at_tt['valid_start_time'] >= vs) &
                (db_state_at_tt['valid_start_time'] < ve)
            ].copy()

            if final_records.empty: return {"error": "No measurements found that started in the specified valid time range.", "count": 0}
            
            final_records.sort_values(by='valid_start_time', ascending=True, inplace=True)

            return {"data": final_records.to_dict('records'), "count": len(final_records)}

        except Exception as e:
            return {"error": f"An unexpected error occurred: {e}", "count": 0}
