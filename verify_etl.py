import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///data/workday.db')
df = pd.read_sql('SELECT * FROM employee_profile', con=engine)
print(df)
with open('data/etl_data.txt', 'w') as f:
    f.write(df.to_string())
    print("ETL data written to data/etl_data.txt")