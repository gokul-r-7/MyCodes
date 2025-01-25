import pandas as pd
import numpy as np
from datetime import datetime, timedelta
df = pd.read_csv(r"D:\TestCode\COX_data_V4.csv")
print(df)

df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
latest_date = df['Date'].max()
start_date = latest_date - pd.Timedelta(days=90)
filtered_df = df[(df['Date'] > start_date) & (df['Date'] < latest_date)]
print(filtered_df)
dates = filtered_df['Date'].unique()
print(dates)

quartiles = filtered_df.groupby(['Operating System Type', 'Metric'])['Value'].quantile([0.25, 0.75]).unstack()
quartiles.reset_index(inplace=True)
quartiles.columns.name = None  # Remove the index name
quartiles.columns = ['Operating System Type', 'Metric', '1st Quartile', '3rd Quartile']
quartiles['1st Quartile'] = quartiles['1st Quartile'].round(1)
quartiles['3rd Quartile'] = quartiles['3rd Quartile'].round(1)
# Step 11: Calculate IQR
quartiles['IQR'] = quartiles['3rd Quartile'] - quartiles['1st Quartile']


# Step 12: Calculate Upper and Lower bounds
# For all metrics except "Devices Page Visits"
quartiles.loc[quartiles['Metric'] != 'Devices Page Visits', 'Upper'] = quartiles['3rd Quartile'] + (0.75 * quartiles['IQR'])
quartiles.loc[quartiles['Metric'] != 'Devices Page Visits', 'Lower'] = quartiles['1st Quartile'] - (0.75 * quartiles['IQR'])

# Special case for "Devices Page Visits"
devices_page_visits = quartiles[quartiles['Metric'] == 'Devices Page Visits']
quartiles.loc[quartiles['Metric'] == 'Devices Page Visits', 'Upper'] = devices_page_visits['3rd Quartile'] + (1.25 * devices_page_visits['IQR'])
quartiles.loc[quartiles['Metric'] == 'Devices Page Visits', 'Lower'] = devices_page_visits['1st Quartile'] - (1.25 * devices_page_visits['IQR'])
quartiles['Upper'] = quartiles['Upper'].round(1)

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)      # Show 100 rows
#print(quartiles.head(100))
