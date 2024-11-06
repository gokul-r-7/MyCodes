import pandas as pd
import numpy as np
from datetime import datetime
original_df = pd.read_csv(r"D:\TestCode\part-00000-aa2c8d4c-29c8-4be1-b4b8-5ed36122c7ae-c000.csv")


df = original_df[original_df['Metric'].str.endswith("%") | (original_df['Metric'] == "Devices Page Visits") ]
df =df.reset_index(drop=True)
print(df)

all_columns_list = list(df.columns)
dates = [date for date in all_columns_list if '/' in date]
sorted_dates = sorted(dates, key=lambda x: datetime.strptime(x, '%m/%d/%Y'))
last7datecolumns = sorted_dates[-8:-1]
last30datecolumns = sorted_dates[-31:-1]
last_7days_df = df[last7datecolumns]
last_30days_df = df[last30datecolumns]


device_page_visit_df_7 = last_7days_df[df['Metric']=="Devices Page Visits"]
meandf_7 = device_page_visit_df_7.mean(axis=1)

device_page_visit_df_30 = last_30days_df[df['Metric']=="Devices Page Visits"]
meandf_30 = device_page_visit_df_30.mean(axis=1)


devicepagevisit_df_7 = last_7days_df[df['Metric']=="Devices Page Visits"]
devicepagevisit_df_7 = devicepagevisit_df_7.reset_index(drop=True)

devicepagevisit_df_30 = last_30days_df[df['Metric']=="Devices Page Visits"]
devicepagevisit_df_30 = devicepagevisit_df_30.reset_index(drop=True)

results_7 = []
results_30 = []

all_metrics = list(set(df['Metric']))
all_metrics.remove("Devices Page Visits")
for items in all_metrics:
    newdf_7 = last_7days_df[df['Metric']==items]
    old_index_7 = newdf_7.index 
    newdf_7 = newdf_7.reset_index(drop=True)
    sumproduct_7 = (newdf_7 * devicepagevisit_df_7).sum(axis=1)
    result_7 = sumproduct_7/devicepagevisit_df_7.sum(axis=1)
    result_7.index = old_index_7
    results_7.append(result_7)

results_7.append(meandf_7)
final_result_7 = pd.concat(results_7)
df['Last 7 Days'] = final_result_7


for item in all_metrics:
    newdf_30 = last_30days_df[df['Metric']==item]
    old_index_30 = newdf_30.index 
    newdf_30 = newdf_30.reset_index(drop=True)
    sumproduct_30 = (newdf_30 * devicepagevisit_df_30).sum(axis=1)
    result_30 = sumproduct_30/devicepagevisit_df_30.sum(axis=1)
    result_30.index = old_index_30
    results_30.append(result_30)

results_30.append(meandf_30)
final_result_30 = pd.concat(results_30)
df['Last 30 Days'] = final_result_30

df['% Change Last 7 Days'] =  (df['Yesterday']- df['Last 7 Days'])/df['Last 7 Days']
df['% Change Last 7 Days']*=100
df['% Change Last 7 Days'] = df['% Change Last 7 Days'].round(1)
df['% Change Last 30 Days'] =  (df['Yesterday']- df['Last 30 Days'])/df['Last 30 Days']
df['% Change Last 30 Days']*=100
df['% Change Last 30 Days'] = df['% Change Last 30 Days'].round(1)
df['Metric + OS'] = df['Metric']+ ' ' + df['Operating System Type']

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)      # Show 100 rows
printable_columns = df[['Metric', '% Change Last 7 Days', '% Change Last 30 Days']]
print(df.head(70))