import requests
import pandas as pd
from sqlalchemy import create_engine

# --- Step 1: Extract from multiple RaaS endpoints ---
url_emp = "http://localhost:5000/raas/employees"
url_comp = "http://localhost:5000/raas/compensation"

emp_df = pd.DataFrame(requests.get(url_emp).json()['Report_Entry'])
comp_df = pd.DataFrame(requests.get(url_comp).json()['Report_Entry'])

print(" Employee Data:")
print(emp_df)
print("\n Compensation Data:")
print(comp_df)

# --- Step 2: Transform & Join ---
# Join on Employee_ID
merged_df = pd.merge(emp_df, comp_df, on='Employee_ID', how='left')

# Add calculated columns
merged_df['Annual_Salary'] = merged_df['Monthly_Salary'] * 12
merged_df['Total_Compensation'] = merged_df['Annual_Salary'] + merged_df['Bonus']

# Filter active employees only
final_df = merged_df[merged_df['Status'] == 'ACTIVE'].reset_index(drop=True)

print("\n Unified Employee Profile:")
print(final_df)

# --- Step 3: Load into database ---
engine = create_engine('sqlite:///data/workday.db')
final_df.to_sql('employee_profile', con=engine, if_exists='replace', index=False)

print("\n Loaded unified employee profile into database (table: employee_profile)")