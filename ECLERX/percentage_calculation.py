import pandas as pd
import os
import numpy as np
from datetime import datetime
df = pd.read_csv(r"D:\shalini\Input_Healthscore.csv")
print(df)
date_columns = df.columns[3:].tolist()

# Step 3: Melt the DataFrame to long format
id_vars = ['Metric+OS','Metrics', 'Operating System Type']
df_melted = df.melt(id_vars=id_vars, value_vars=date_columns, var_name='Date', value_name='Value')
print(df_melted)

df_pivot = df_melted.pivot_table(index=['Date', 'Operating System Type'], columns='Metrics', values='Value', aggfunc='first').reset_index()
print(df_pivot)

column_mapping = {
    'DR_SmartHelp - Agent Chat' : 'Interacted with Chat',
    'DR_SmartHelp - Billing & Account Support' : 'Clicked Billing & Account Support',
    'DR_SmartHelp - Homelife Support' : 'Clicked Homelife Support',
    'DR_SmartHelp - Internet Support' : 'Clicked Internet Support',
    'DR_SmartHelp - Landing Page' : 'Devices Page Visits',
    'DR_SmartHelp - P&P' : 'Started Proactive and Preventive (P&P)',
    'DR_SmartHelp - Reboot Flow Started' : 'Did a Device Reboot',
    'DR_SmartHelp - Result' : 'Saw a Result',
    'DR_SmartHelp - Started' : 'Started Troubleshooting (Solution Center)',
    'DR_SmartHelp - TV Support' : 'Clicked TV Support',
    'DR_SmartHelp Result - Disconnected' : '2nd common reason (Disconnected)',
    'DR_SmartHelp Result - Incomplete Install' : '3rd common reason (Incomplete Install)',
    'DR_SmartHelp Result - Unknown' : '1st common reason (Unknown)',
    'DR_Smarthelp P&P - Connect with a support agent' : 'Connect with Agent',
    'DR_Smarthelp P&P - Interacted with Chat Agent' : 'Interacted with Chat_1',
    "DR_Smarthelp P&P - Let's schedule a technician visit" : 'Schedule Tech',
    "DR_Smarthelp P&P - Let's troubleshoot together" : 'Started P&P troubleshooting',
    'DR_Smarthelp P&P - Reset Complete' : 'Reset Complete'
}
df_pivot.rename(columns = column_mapping, inplace = True)
today_date = datetime.today()
df_pivot['Create_Date'] = today_date





# Creating new columns as per the specified calculations and rounding off to 1 decimal places
df_pivot['1st common reason (Unknown) %'] = (df_pivot['1st common reason (Unknown)'] / df_pivot['Saw a Result'] * 100).round(3)
df_pivot['2nd common reason (Disconnected) %'] = (df_pivot['2nd common reason (Disconnected)'] / df_pivot['Saw a Result'] * 100).round(3)
df_pivot['3rd common reason (Incomplete Install) %'] = (df_pivot['3rd common reason (Incomplete Install)'] / df_pivot['Saw a Result'] * 100).round(3)
df_pivot['Clicked Billing & Account Support %'] = (df_pivot['Clicked Billing & Account Support'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Clicked Homelife Support %'] = (df_pivot['Clicked Homelife Support'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Clicked Internet Support %'] = (df_pivot['Clicked Internet Support'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Clicked TV Support %'] = (df_pivot['Clicked TV Support'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Connect with Agent %'] = (df_pivot['Connect with Agent'] / df_pivot['Started Proactive and Preventive (P&P)'] * 100).round(3)
df_pivot['Did a Device Reboot %'] = (df_pivot['Did a Device Reboot'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Interacted with Chat %'] = (df_pivot['Interacted with Chat'] / df_pivot['Started Proactive and Preventive (P&P)'] * 100).round(3)
df_pivot['Interacted with Chat_1 %'] = (df_pivot['Interacted with Chat_1'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Reset Complete %'] = (df_pivot['Reset Complete'] / df_pivot['Started Proactive and Preventive (P&P)'] * 100).round(3)
df_pivot['Saw a Result %'] = (df_pivot['Saw a Result'] / df_pivot['Started Troubleshooting (Solution Center)'] * 100).round(3)
df_pivot['Schedule Tech %'] = (df_pivot['Schedule Tech'] / df_pivot['Started Proactive and Preventive (P&P)'] * 100).round(3)
df_pivot['Started P&P troubleshooting %'] = (df_pivot['Started P&P troubleshooting'] / df_pivot['Started Proactive and Preventive (P&P)'] * 100).round(3)
df_pivot['Started Proactive and Preventive (P&P) %'] = (df_pivot['Started Proactive and Preventive (P&P)'] / df_pivot['Devices Page Visits'] * 100).round(3)
df_pivot['Started Troubleshooting (Solution Center) %'] = (df_pivot['Started Troubleshooting (Solution Center)'] / df_pivot['Devices Page Visits'] * 100).round(3)

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)      # Show 100 rows
print(df_pivot.head(10))
print(df_pivot.columns)
