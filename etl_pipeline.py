import requests
from requests.exceptions import RequestException
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import time
from datetime import datetime, timezone


# Config
DB_URL = "sqlite:///data/workday.db"
EMP_URL = "http://localhost:5000/raas/employees"
COMP_URL = "http://localhost:5000/raas/compensation"
DEPT_URL = "http://localhost:5000/raas/departments"
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds (exponential backoff)

# Logging setup
logger = logging.getLogger("workday_etl")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

engine = create_engine(DB_URL, connect_args={"check_same_thread": False})  # sqlite tweak


# Utility: HTTP GET with simple retries/backoff, returns dict
def http_get_with_retry(url: str):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            logger.info(f"GET {url} (attempt {attempt+1})")
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            return r.json()
        
        except RequestException as e:
            attempt += 1
            wait = RETRY_BACKOFF ** attempt
            logger.warning(f"Request failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts")

# --- Step 1: Extract from multiple RaaS endpoints ---
# Fetch employee, compensation, department data, returns 3 dataframes
def extract_data():
    emp_df = pd.DataFrame(http_get_with_retry(EMP_URL).get("Report_Entry", []) )    
    comp_df = pd.DataFrame(http_get_with_retry(COMP_URL).get("Report_Entry", []) )
    dept_df = pd.DataFrame(http_get_with_retry(DEPT_URL).get("Report_Entry", []) )
    logger.info(f"Extracted {len(emp_df)} employees, {len(comp_df)} compensation rows, {len(dept_df)} departments")
    return emp_df, comp_df, dept_df

# --- Step 2: Validate data integrity ---
# Basic validations + referential integrity checks
def validate(emp_df: pd.DataFrame, comp_df: pd.DataFrame, dept_df: pd.DataFrame):
    errors = []
    # Employee validations
    # is null or duplicate append to errors list
    if emp_df['Employee_ID'].isnull().any(): 
        errors.append("Null Employee_ID in employee dataset")
    if not emp_df['Employee_ID'].duplicated().any() == False: 
        # if any duplicates -> add error
        if emp_df['Employee_ID'].duplicated().any():
            errors.append("Duplicate Employee_ID found in employees")

    # Compensation validations
    # salary < 0 or null, append to errors list
    if (comp_df['Monthly_Salary'] <= 0).any(): 
        errors.append("Monthly_Salary must be > 0 in compensation dataset")
    if comp_df['Employee_ID'].isnull().any():
        errors.append("Null Employee_ID in compensation dataset")

    # Departments validations, if null dept_id append to errors list
    if dept_df['Department_ID'].isnull().any():
        errors.append("Null Department_ID in departments dataset")

    # Referential integrity checks
    emp_ids = set(emp_df['Employee_ID']) # unordered unique set of employee ids
    comp_ids = set(comp_df['Employee_ID']) # unordered unique set of employee ids from compensation table
    missing_comp = emp_ids - comp_ids
    if missing_comp:
        logger.warning(f"Employees without compensation records: {missing_comp}")  # not necessarily fatal

    if errors:
        raise ValueError("Validation failed: " + "; ".join(errors))
    logger.info("Validation passed")

# --- Step 3: Transform + Load ---
# Transform + join
def transform(emp_df: pd.DataFrame, comp_df: pd.DataFrame, dept_df: pd.DataFrame) -> pd.DataFrame:
    # Merge datasets
    merged = pd.merge(emp_df, comp_df, on='Employee_ID', how='left')
    merged = pd.merge(merged, dept_df[['Department_ID', 'Department_Name']], on='Department_ID', how='left')

    # Fill defaults
    merged['Monthly_Salary'] = merged['Monthly_Salary'].fillna(0).astype(float)
    merged['Bonus'] = merged['Bonus'].fillna(0).astype(float)

    # Calculated fields
    merged['Annual_Salary'] = merged['Monthly_Salary'] * 12
    merged['Total_Compensation'] = merged['Annual_Salary'] + merged['Bonus']

    # Filter active employees
    merged = merged[merged['Status'] == 'ACTIVE'].reset_index(drop=True)

    # Add ETL metadata
    merged['etl_executed_at'] = datetime.now(timezone.utc).isoformat()

    logger.info(f"Transformed data; {len(merged)} active rows ready to load")
    return merged

# --- Step 4: Load into SQLite DB ---
# Load: write to DB and insert an etl_runs record
def load(df: pd.DataFrame, run_meta: dict):
    with engine.begin() as conn:
        # create or replace employee_profile
        df.to_sql('employee_profile', con=conn, if_exists='replace', index=False)

        # create etl_runs table if not exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                ended_at TEXT,
                status TEXT,
                rows_loaded INTEGER,
                notes TEXT
            )
        """))

        conn.execute(
            text("INSERT INTO etl_runs (started_at, ended_at, status, rows_loaded, notes) VALUES (:started_at, :ended_at, :status, :rows_loaded, :notes)"),
            run_meta
        )
    logger.info(f"Loaded {len(df)} rows into employee_profile and recorded ETL run")

    # Main runner with error handling and metadata
    # main etl to extract, validate and then transform + Load
def run_etl():
    started_at = datetime.now(timezone.utc).isoformat()
    status = "SUCCESS"
    notes = ""
    rows_loaded = 0
    try:
        emp_df, comp_df, dept_df = extract_data() # Step 1 extract
        validate(emp_df, comp_df, dept_df) # step 2 validate
        final_df = transform(emp_df, comp_df, dept_df) # step 3 transform
        rows_loaded = len(final_df)
        load(final_df, {                # Step 4 load
            "started_at": started_at,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "rows_loaded": rows_loaded,
            "notes": "OK"
        })
    except Exception as e:
        status = "FAILED"
        notes = str(e)
        logger.exception("ETL failed")
        # record failure
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT,
                    ended_at TEXT,
                    status TEXT,
                    rows_loaded INTEGER,
                    notes TEXT
                )
            """))
            conn.execute(text("INSERT INTO etl_runs (started_at, ended_at, status, rows_loaded, notes) VALUES (:started_at, :ended_at, :status, :rows_loaded, :notes)"),
                         {
                             "started_at": started_at,
                             "ended_at": datetime.now(timezone.utc).isoformat(),
                             "status": status,
                             "rows_loaded": rows_loaded,
                             "notes": notes
                         })
        raise
    return {"started_at": started_at, "ended_at": datetime.now(timezone.utc).isoformat(), "status": status, "rows_loaded": rows_loaded, "notes": notes}

# If run directly, run etl
if __name__ == "__main__":
    result = run_etl()
    logger.info(f"ETL run complete: {result}")
