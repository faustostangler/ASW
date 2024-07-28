# Cell 1
import os
import sqlite3
import pandas as pd

# Construct the path to the database file
print('test')
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(base_dir, 'data', 'b3 BENS INDUSTRIAIS.db')

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# Read the finsheet table into a DataFrame
query = "SELECT * FROM finsheet"
df_finsheet = pd.read_sql_query(query, conn).drop_duplicates()

# Close the database connection
conn.close()

df_finsheet.loc[:, 'quarter'] = pd.to_datetime(df_finsheet['quarter'], errors='coerce')

# Cell 2
last_quarters = ['3', '4']
all_quarters = ['6', '7']

# Cell 3
# Ensure 'quarter' is in datetime format and handle any conversion errors
df_finsheet.loc[:, 'quarter'] = pd.to_datetime(df_finsheet['quarter'], errors='coerce')
df_finsheet = df_finsheet.dropna(subset=['quarter'])

df = df_finsheet[(df_finsheet['company_name'] == 'EMBRAER SA') &
                          (df_finsheet['tipo'] == 'DFs Consolidadas') &
                          (pd.to_datetime(df_finsheet['quarter'], errors='coerce').dt.year == 2016) &
                          (df_finsheet['conta'] == '3.01')].copy()

# Check the prefix of 'conta'
conta_prefix = df['conta'].iloc[0][0]

# Initialize index and value variables for each quarter
i3 = i6 = i9 = i12 = None
v3 = v6 = v9 = v12 = 0

# Find out values for each month
try:
    df_march = df[df['quarter'].dt.month == 3]
    if not df_march.empty:
        i3 = df_march.index[0]
        v3 = df_march['valor'].max()
except Exception as e:
    pass
try:
    df_june = df[df['quarter'].dt.month == 6]
    if not df_june.empty:
        i6 = df_june.index[0]
        v6 = df_june['valor'].max()
except Exception as e:
    pass
try:
    df_september = df[df['quarter'].dt.month == 9]
    if not df_september.empty:
        i9 = df_september.index[0]
        v9 = df_september['valor'].max()
except Exception as e:
    pass
try:
    df_december = df[df['quarter'].dt.month == 12]
    if not df_december.empty:
        i12 = df_december.index[0]
        v12 = df_december['valor'].max()
except Exception as e:
    pass

if conta_prefix in last_quarters:
    print('last quarter)')
    try:
        v12 = v12 - (v9 + v6 + v3)
    except Exception as e:
        print('Error in B3 math for last quarters:', e, df.iloc[0])

if conta_prefix in all_quarters:
    try:
        v3 = v3 - 0
        v6 = v6 - (v3)
        v9 = v9 - (v6 + v3)
        v12 = v12 - (v9 + v6 + v3)
    except Exception as e:
        print('Error in B3 math for all quarters:', e, df.iloc[0])

# Update values
if i3 is not None:
    df.loc[i3, 'valor'] = v3
if i6 is not None:
    df.loc[i6, 'valor'] = v6
if i9 is not None:
    df.loc[i9, 'valor'] = v9
if i12 is not None:
    df.loc[i12, 'valor'] = v12

# # Cell 4
# # Filtering for speed debug purpose
# df_filtered = df_finsheet[(df_finsheet['company_name'] == 'EMBRAER SA') &
#                           (df_finsheet['tipo'] == 'DFs Consolidadas') &
#                           (pd.to_datetime(df_finsheet['quarter'], errors='coerce').dt.year == 2016) &
#                           (df_finsheet['conta'] == '3.01')].copy()

# # Save the filtered DataFrame to a CSV file for inspection
# csv_path_filtered = 'finsheet_embraer_2016_filtered.csv'
# df_filtered.to_csv(csv_path_filtered, index=False)

# print(f"Data saved to {csv_path_filtered}")
# df_filtered

# # Cell 6
# # Apply B3 math to the filtered dataframe
# df_transformed = df_filtered.groupby(['company_name', 'quadro', 'conta'], group_keys=False).apply(apply_b3_math).reset_index(drop=True)

# # Save the transformed DataFrame as a CSV file
# csv_path_transformed = 'finsheet_embraer_2016_transformed.csv'
# df_transformed.to_csv(csv_path_transformed, index=False)

# print(f"Data saved to {csv_path_transformed}")
