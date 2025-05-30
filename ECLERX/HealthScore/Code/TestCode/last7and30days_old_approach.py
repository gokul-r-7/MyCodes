import pandas as pd
import numpy as np
df = pd.read_csv(r"D:\TestCode\sampledata.csv")


allcolumns = list(df.columns)
last7datecolumns = allcolumns[:-4][-7:]
last30datecolumns = allcolumns[:-4][-30:]

last_7days_df = df[last7datecolumns]

last_30days_df = df[last30datecolumns]




device_page_visit_df_7 = last_7days_df[df['Metric']=="Devices Page Visits"]
print(device_page_visit_df_7)
meandf_7 = device_page_visit_df_7.mean(axis=1)
print(meandf_7)

device_page_visit_df_30 = last_30days_df[df['Metric']=="Devices Page Visits"]
print(device_page_visit_df_30)
meandf_30 = device_page_visit_df_30.mean(axis=1)
print(meandf_30)


devicepagevisit_df_7 = last_7days_df[df['Metric']=="Devices Page Visits"]
print(devicepagevisit_df_7)
device_page_visit_df_7 = device_page_visit_df_7.reset_index(drop=True)
print(device_page_visit_df_7)

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
