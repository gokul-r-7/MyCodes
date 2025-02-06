# %%

import os
import json
import functools
import numpy as np
import pandas as pd
from datetime import datetime

# %%
df = pd.read_csv(r"D:\shalini\HS_DataClean_Nov.csv")

# %%
# Step 1: Read the config.json file
with open("D:\\Healthscore Codes\\healthscore_dashboard_final.json", 'r') as file:
    config_data = json.load(file)
    print(config_data)

# %%
# Function to parse the nested dictionary
def parse_data(data):
    feature_groups = {}
    feature_seqno_mapping = {}
    display_name_mapping = {}
    metric_nature_mapping = {}
    metric_sequence_number_mapping = {}

    # Loop through the features in the data
    for feature in data['features']:
        feature_name = feature['feature_name']
        feature_seq_num = feature['feature_seq_num']
        
        # Add feature to feature groups
        feature_groups[feature_name] = []
        
        # Add feature seq no mapping
        feature_seqno_mapping[feature_name] = feature_seq_num
        
        for metric in feature['metrics']:
            metric_name = metric['metrics']
            display_name = metric['display_names']
            metric_nature = metric['metric_nature']
            metric_seq_num = metric['metric_sequence_num']
            
            # Add display name mapping
            display_name_mapping[metric_name] = display_name
            
            # Add metric nature mapping
            metric_nature_mapping[display_name] = metric_nature
            
            # Add metric sequence number mapping
            metric_sequence_number_mapping[display_name] = metric_seq_num
            
            # Add the metric name to the respective feature group
            feature_groups[feature_name].append(metric_name)

    return feature_groups, feature_seqno_mapping, display_name_mapping, metric_nature_mapping, metric_sequence_number_mapping

# %%
# Parse the data and get the mappings
feature_groups, feature_seqno_mapping, display_name_mapping, metric_nature_mapping, metric_sequence_number_mapping = parse_data(config_data)

# %%
# Step 2: Extract config1 and config2 from the file
level_1_metrics = config_data['level_1_metrics']
level_2_metrics = config_data['level_2_metrics']
level_3_metrics = config_data['level_3_metrics']
level_4_metrics = config_data['level_4_metrics']
level_5_metrics = config_data['level_5_metrics']


# %%
# Print the results (optional)
print("Feature Groups =", feature_groups)
print("Feature Seqno Mapping =", feature_seqno_mapping)
print("Display Name Mapping =", display_name_mapping)
print("Metric Nature Mapping =", metric_nature_mapping)
print("Metric Sequence Number Mapping =", metric_sequence_number_mapping)
print("level_1_metrics = ", level_1_metrics)
print("level_2_metrics = ", level_2_metrics)
print("level_3_metrics = ", level_3_metrics)
print("level_4_metrics = ", level_4_metrics)
print("level_5_metrics = ", level_5_metrics)

# %%
df['display_names'] = df['Metrics'].map(display_name_mapping)

# %%
df['metric_nature'] = df['display_names'].map(metric_nature_mapping)

# %%
df['level_1'] = df['display_names'].apply(lambda x: x if x in level_1_metrics else None)
df['level_2'] = df['display_names'].apply(lambda x: x if x in level_2_metrics else None)
df['level_3'] = df['display_names'].apply(lambda x: x if x in level_3_metrics else None)
df['level_4'] = df['display_names'].apply(lambda x: x if x in level_4_metrics else None)
df['level_5'] = df['display_names'].apply(lambda x: x if x in level_5_metrics else None)

# %%
feature_column = 'Unknown'
df['Feature'] = feature_column
for group, metrics in feature_groups.items():
   df['Feature'] = np.where(df['Metrics'].isin(metrics), group, df['Feature'])

# %%
def move_row(df, from_idx, to_idx):
    row = df.iloc[from_idx]
    df = df.drop(from_idx).reset_index(drop=True)
    df = pd.concat([df.iloc[:to_idx], row.to_frame().T, df.iloc[to_idx:]]).reset_index(drop=True)
    return df

df = move_row(df, 50, 46)
df = move_row(df, 51, 47)
df = move_row(df, 52, 48)
df = move_row(df, 53, 49)
df = move_row(df, 164, 158)
df = move_row(df, 165, 159)
df = move_row(df, 180, 174)
df = move_row(df, 181, 175)
df = move_row(df, 218, 206)
df = move_row(df, 219, 207)
df = move_row(df, 220, 208)
df = move_row(df, 221, 209)
df = move_row(df, 280, 268)
df = move_row(df, 281, 269)
df = move_row(df, 282, 270)
df = move_row(df, 283, 271)

# %%
df['unique_identifier'] = df['Feature'].apply(lambda x: ''.join([word[0].upper() for word in x.split()]))

# %%
def generate_hierarchy_id_grouped_by_os(df):
    df['Hierarchy_ID'] = None

   
    df['Feature_Order'] = df.groupby('Operating System Type')['unique_identifier'].transform(lambda x: pd.Series(range(len(x)), index=x.index))

    grouped = df.groupby(['Operating System Type', 'unique_identifier'], group_keys=False)
    results = []

   
    for (os_name, unique_id), group in grouped:
        current_ids = {'level_1': 0, 'level_2': 0, 'level_3': 0, 'level_4': 0, 'level_5': 0}  

        group = group.sort_values('Feature_Order')  

        for index, row in group.iterrows():
            if pd.isna(row['display_names']):
                continue  

            if not pd.isna(row['level_1']):  
                current_ids['level_1'] += 1
                current_ids['level_2'] = 0  
                current_ids['level_3'] = 0  
                current_ids['level_4'] = 0  
                current_ids['level_5'] = 0  
                group.at[index, 'Hierarchy_ID'] = f"{os_name}_{unique_id}_{current_ids['level_1']}"

            elif not pd.isna(row['level_2']):  
                current_ids['level_2'] += 1
                current_ids['level_3'] = 0  
                current_ids['level_4'] = 0  
                current_ids['level_5'] = 0  
                group.at[index, 'Hierarchy_ID'] = f"{os_name}_{unique_id}_{current_ids['level_1']}.{current_ids['level_2']}"

            elif not pd.isna(row['level_3']):  
                current_ids['level_3'] += 1
                current_ids['level_4'] = 0  
                current_ids['level_5'] = 0  
                group.at[index, 'Hierarchy_ID'] = f"{os_name}_{unique_id}_{current_ids['level_1']}.{current_ids['level_2']}.{current_ids['level_3']}"

            elif not pd.isna(row['level_4']):  
                current_ids['level_4'] += 1
                current_ids['level_5'] = 0  
                group.at[index, 'Hierarchy_ID'] = f"{os_name}_{unique_id}_{current_ids['level_1']}.{current_ids['level_2']}.{current_ids['level_3']}.{current_ids['level_4']}"

            elif not pd.isna(row['level_5']):  
                current_ids['level_5'] += 1
                group.at[index, 'Hierarchy_ID'] = f"{os_name}_{unique_id}_{current_ids['level_1']}.{current_ids['level_2']}.{current_ids['level_3']}.{current_ids['level_4']}.{current_ids['level_5']}"

        results.append(group)

   
    df = pd.concat(results, ignore_index=True)

    
    #df = df[df['Display_Names'].notna()]

    
    cols = [col for col in df.columns if col != 'Hierarchy_ID' and col != 'Feature_Order'] + ['Hierarchy_ID']
    return df[cols]


df = generate_hierarchy_id_grouped_by_os(df)


# %%
df = generate_hierarchy_id_grouped_by_os(df)

def assign_parent_id(row):
    if not pd.isna(row['level_5']):  
        return f"{row['Hierarchy_ID'].rsplit('.', 1)[0]}"
    elif not pd.isna(row['level_4']):  
        return f"{row['Hierarchy_ID'].rsplit('.', 1)[0]}"
    elif not pd.isna(row['level_3']):  
        return f"{row['Hierarchy_ID'].rsplit('.', 1)[0]}"
    elif not pd.isna(row['level_2']):  
        return f"{row['Hierarchy_ID'].rsplit('.', 1)[0]}"
    elif not pd.isna(row['level_1']): 
        return None
    else:
        return None


df['Parent_ID'] = df.apply(assign_parent_id, axis=1)


# %%
df['metric_id'] = range(1, len(df) + 1)

# %%
df['metric_sequence_num'] = df.groupby('Operating System Type').cumcount() + 1

# %%
df['metric_seqno'] = df['display_names'].map(metric_sequence_number_mapping)

# %%
def calculate_level_no(row):
   
    if pd.isna(row['Hierarchy_ID']) or pd.isna(row['Parent_ID']):
        return 1
    
   
    return row['Hierarchy_ID'].count('.') + 1


df['level_no'] = df.apply(calculate_level_no, axis=1)


# %%
feature_names = df['Feature'].unique()
feature_mapping = {feature_name: idx + 1 for idx, feature_name in enumerate(feature_names)}

df['feature_id'] = df['Feature'].map(feature_mapping)


# %%
# Create 'feature_seq_num' column based on the dictionary
df['feature_seq_num'] = df['Feature'].map(feature_seqno_mapping)

# %%
df['create_dt'] = pd.to_datetime('today').normalize().date()

# %%
df = df.rename(columns={'Parent_ID': 'parent_id_old'})

# %%
def calculate_parent_id(row, df):
    
    if pd.isna(row['Hierarchy_ID']):
        return None
   
    hierarchy_parts = row['Hierarchy_ID'].rsplit('.', 1)
    if len(hierarchy_parts) > 1:
       
        parent_hierarchy = hierarchy_parts[0]
        
        parent_row = df[df['Hierarchy_ID'] == parent_hierarchy]
        if not parent_row.empty:
            return parent_row['metric_id'].values[0]

    return None

df['parent_id'] = df.apply(calculate_parent_id, axis=1, df=df)


df.loc[df['Hierarchy_ID'].isna(), 'parent_id'] = None


# %%
df = df.rename(columns={
    'Metrics': 'metrics',
    'Operating System Type': 'operating_system_type',
    'Feature': 'feature_name',
    'Operating System Type': 'operating_system_type',
    'Feature': 'feature_name',
    'Operating System Type': 'operating_system_type',
    'Hierarchy_ID': 'hierarchy_id',
})

# %%
df.shape

# %%
print(list(df.columns))

# %%
first_table_columns = ['metric_id', 'metrics', 'display_names', 'operating_system_type', 'feature_name', 'unique_identifier', 'level_1', 'level_2', 'level_3', 'level_4','level_5', 'hierarchy_id', 'parent_id_old', 'metric_sequence_num', 'metric_seqno', 'level_no', 'feature_id', 'feature_seq_num', 'parent_id', 'metric_nature', 'create_dt' ]
first_table_df = df[first_table_columns]

# %%
#first_table_df.to_csv(r'D:\Healthsore Data\table_1_data_all_features.csv', index = False)

# %%
def is_date_column(col_name):
    date_formats = ['%d-%m-%Y']
    for fmt in date_formats:
        try:
            pd.to_datetime(col_name, format=fmt)
            return True
        except ValueError:
            continue  # Try the next format
    return False

# Filter columns with date format 'm/d/Y'
date_columns = [col for col in df.columns if is_date_column(col)]

# %%
second_table_columns1 = ['metric_id', 'create_dt', 'metrics', 'operating_system_type']
second_table_columns = second_table_columns1 + date_columns
second_table_df = df[second_table_columns]

# %%
filtered_date_columns = second_table_df.columns[4:].to_list()

# %%
id_vars = ['metric_id', 'create_dt']

# %%
second_table_df_melted = second_table_df.melt(id_vars = id_vars, value_vars = filtered_date_columns, var_name = 'Date', value_name = 'Value')

# %%
second_table_df_melted.shape

# %%
#second_table_df_melted.to_csv(r'D:\Healthsore Data\Table2.csv', index = False)

# %%
sample_df = df[df['feature_name'].isin(['SMART HELP', 'RESET MODEM', 'RESET TV BOX', 'LOG IN', 'BIL LING', 'OVER ALL', 'EASY CON NECT', 'DATA USAGE', 'RE GISTRATION (NATIVE)'])]
sample_df.shape

# %%
def calculate_percentage(df, datecolumns, metricid, parentid):
    # Ensure these columns exist in the DataFrame
    if metricid not in df.columns or parentid not in df.columns:
        raise ValueError(f"'{metricid}' or '{parentid}' columns not found in DataFrame.")
    
    # Create a copy of the DataFrame to avoid setting on a view
    df = df.copy()
    
    # Iterate through each row and calculate the percentage for each DATE column
    for idx, row in df.iterrows():
        # Iterate through all the DATE columns
        for date_col in datecolumns:
            # Get the numerator (DATE value)
            numerator = row[date_col]
            
            # Get the parent hierarchy ID
            parent_id = row[parentid]
            
            # Check if the parent is empty
            if pd.isna(parent_id) or parent_id == "":
                # If the PARENTID is empty, keep the original DATE value
                percentage = numerator  # Assuming 100% when no parent is found
            else:
                # Find the row where HIERARCHYID == parent_hierarchyid to get the parent's DATE value
                parent_row = df[df[metricid] == parent_id]
                
                # If the parent exists, get the parent's DATE value for the current DATE column
                if not parent_row.empty:
                    denominator = parent_row[date_col].values[0]
                else:
                    denominator = 0  # In case no parent is found, avoid division by zero
                
                # Calculate the percentage
                if denominator != 0:
                    percentage = (numerator / denominator) * 100
                else:
                    percentage = 0  # To handle division by zero if no parent found
                
            # Round to 3 decimal places
            percentage = round(percentage, 3)
            
            # Add the percentage to the DataFrame in a new column
            percentage_column_name = f"{date_col} PERCENTAGE"
            if percentage_column_name not in df.columns:
                df[percentage_column_name] = None  # Initialize the column safely
            
            # Set the percentage for the current row and current date column using .loc
            df.loc[idx, percentage_column_name] = percentage

    return df


# %%
percentage_df = calculate_percentage(df, date_columns, 'metric_id', 'parent_id')
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)
percentage_df.head()

# %%
percentage_df.shape

# %%
id_columns = ['create_dt', 'metric_id', 'feature_id', 'feature_seq_num', 'metric_sequence_num', 'metric_seqno','level_no', 'parent_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names', 'operating_system_type', 'metric_nature']

# %%
# List of columns that contain "PERCENTAGE"
percentage_columns = [col for col in percentage_df.columns if 'PERCENTAGE' in col]

# %%
percent_df = percentage_df[id_columns + percentage_columns]

# %%
# Rename columns to remove the word "PERCENTAGE"
percent_df.columns = [col.replace(" PERCENTAGE", "") if "PERCENTAGE" in col else col for col in percent_df.columns]

# %%
percent_df.shape

# %%
percent_df.head()

# %%
def calculate_both(df, metricid, parentid):
    """
    Generate new rows for each unique display_name based on given conditions.

    Args:
    - df (DataFrame): Input DataFrame containing the original data.

    Returns:
    - DataFrame: Updated DataFrame with newly calculated rows.
    """
    new_rows = []  # Store new rows
    seq_num = 1  # Sequential ID for metric_id

    for display_name in df['display_names'].unique():
        # Filter rows for the current display_name
        display_df = df[df['display_names'] == display_name]
            
        # Case 1: If `parent_id_old` has null or empty values
        if display_df[parentid].isnull().any() or display_df[parentid].eq('').any():
            # Sum of `Apple iOS` and `Google Android` for `parent_id_old` being null
            ios_rows = display_df[display_df["operating_system_type"] == "Apple iOS"]
            android_rows = display_df[display_df["operating_system_type"] == "Google Android"]
            
            # Calculate numerator using `where` to avoid downcasting issues
            ios_sums = ios_rows[date_columns].apply(pd.to_numeric, errors='coerce').reset_index(drop=True)
            android_sums = android_rows[date_columns].apply(pd.to_numeric, errors='coerce').reset_index(drop=True)
            numerator = ios_sums + android_sums
            
            # Store numerator in new row, rounded to 3 decimal places
            new_row_values = numerator.sum(axis=0).fillna(0).apply(lambda x: round(x, 3)).to_dict()
        else:
            # Case 2: If `parent_id_old` is not null
            ios_rows = display_df[(display_df["operating_system_type"] == "Apple iOS") & 
                                  (display_df[parentid].notna()) & 
                                  (display_df[parentid] != '')]
            android_rows = display_df[(display_df["operating_system_type"] == "Google Android") & 
                                      (display_df[parentid].notna()) & 
                                      (display_df[parentid] != '')]
            
            # Calculate numerator using `where` to avoid downcasting issues
            ios_sums = ios_rows[date_columns].apply(pd.to_numeric, errors='coerce').reset_index(drop=True)
            android_sums = android_rows[date_columns].apply(pd.to_numeric, errors='coerce').reset_index(drop=True)
            numerator = ios_sums + android_sums
            
            # Filter rows where `parent_id_old` matches `hierarchy_id`
            filtered_data = df[df[metricid].isin(display_df[parentid])]
            ios_data = filtered_data[filtered_data['operating_system_type'] == "Apple iOS"]
            android_data = filtered_data[filtered_data['operating_system_type'] == "Google Android"]

            # Calculate denominator
            ios_sums_denom = ios_data[date_columns].apply(pd.to_numeric, errors='coerce').reset_index(drop=True)
            android_sums_denom = android_data[date_columns].apply(pd.to_numeric, errors='coerce').reset_index(drop=True)
            denominator = ios_sums_denom + android_sums_denom

            # Calculate result as percentage
            result = (numerator / denominator) * 100 if not denominator.empty else pd.DataFrame(0, columns=date_columns, index=numerator.index)
            result = result.round(3)  # Round result to 3 decimal places
            new_row_values = result.sum(axis=0).fillna(0).apply(lambda x: round(x, 3)).to_dict()

        # Create the new row for the current display_name
        new_row = {
            'display_names': display_name,
            'operating_system_type': 'Both',
            'metric_id': f'100000{seq_num}'
        }
        # Add calculated values to the new row
        new_row.update(new_row_values)
        new_rows.append(new_row)
        seq_num += 1

    # Append new rows to the original DataFrame
    new_rows_df = pd.DataFrame(new_rows)
    result_df = pd.concat([df, new_rows_df], ignore_index=True)

    # Reset index
    result_df.reset_index(drop=True, inplace=True)
    return new_rows_df


# %%
both_calc_df = calculate_both(df,'metric_id', 'parent_id')
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 300)      # Show 100 rows
both_calc_df.head(5)

# %%
both_calc_df.shape

# %%
unique_display_names_df_test = df.drop_duplicates(subset='display_names')
unique_display_names_df_test.shape

# %%
id_columns_both_test = ['create_dt', 'feature_id', 'feature_seq_num', 'metric_sequence_num', 'metric_seqno', 'parent_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names','metric_nature' ]
id_column_both_df_test = unique_display_names_df_test[id_columns_both_test]
#id_column_both_df = id_column_both_df[id_column_both_df['feature_name'].isin(['SMART HELP'])]
id_column_both_df_test.shape

# %%
# Perform a left merge on 'display_names'
both_df_test =  pd.merge(both_calc_df, id_column_both_df_test, on='display_names', how='inner')
both_df_test.shape

# %%
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 300)      # Show 100 rows
both_df_test.head()

# %%
print(list(both_df_test))

# %%
both_df_test.shape

# %%
modifiled_first_table_df = pd.concat([df, both_df_test], ignore_index=True)

# %%
modifiled_first_table_df.shape

# %%
print(list(modifiled_first_table_df))

# %%
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 500)      # Show 100 rows
modifiled_first_table_df = modifiled_first_table_df[['metric_id', 'metrics', 'display_names', 'operating_system_type', 'feature_name', 'unique_identifier', 'metric_nature', 'level_1', 'level_2', 'level_3', 'level_4', 'level_5', 'hierarchy_id', 'parent_id_old', 'metric_sequence_num', 'metric_seqno', 'level_no', 'feature_id', 'feature_seq_num', 'parent_id', 'create_dt']]
modifiled_first_table_df.head()

# %%
#modifiled_first_table_df.to_csv(r'D:\Healthsore Data\Table1.csv', index = False)

# %%


# %%
unique_display_names_df = df.drop_duplicates(subset='display_names')
unique_display_names_df.shape

# %%
id_columns_both = ['create_dt', 'feature_id', 'feature_seq_num', 'metric_sequence_num', 'metric_seqno', 'parent_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names','metric_nature' ]
id_column_both_df = unique_display_names_df[id_columns_both]
#id_column_both_df = id_column_both_df[id_column_both_df['feature_name'].isin(['SMART HELP'])]
id_column_both_df.shape

# %%
# Perform a left merge on 'display_names'
both_df =  pd.merge(both_calc_df, id_column_both_df, on='display_names', how='inner')
both_df.shape

# %%
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 300)      # Show 100 rows
both_df.head()


# %%
third_table_df = pd.concat([percent_df, both_df], ignore_index=True)

# %%
third_table_df.shape

# %%
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 200)      # Show 100 rows
third_table_df.head()

# %%
print(list(third_table_df))

# %%
id_columns = ['create_dt', 'metric_id', 'feature_id', 'feature_seq_num', 'metric_sequence_num', 'metric_seqno', 'level_no', 'parent_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names', 'operating_system_type', 'metric_nature']

# %%
#third_table_df.to_csv(r'D:\Healthsore Data\Table3.csv', index = False)

# %%
third_table_melted_df = pd.melt(third_table_df, id_vars=id_columns, 
                     value_vars=date_columns, 
                     var_name='Date', 
                     value_name='Value')

# %%
third_table_melted_df.shape

# %%
third_table_melted_df.columns

# %%
third_table_final_df = third_table_melted_df[['create_dt', 'metric_id', 'feature_id', 'feature_name', 'metrics', 'display_names', 'operating_system_type', 'Date', 'Value']]

# %%
third_table_final_df.head()

# %%
third_table_final_df.shape

# %%
#third_table_final_df.to_csv(r'D:\Healthsore Data\Table3_athena.csv', index = False)

# %%
third_table_melted_df.dtypes

# %%
# Convert 'create_dt' and 'Date' to datetime
third_table_melted_df['create_dt'] = pd.to_datetime(third_table_melted_df['create_dt'], errors='coerce')
third_table_melted_df['Date'] = pd.to_datetime(third_table_melted_df['Date'], errors='coerce')

# Convert 'feature_id', 'metric_sequence_num', 'level_no', 'parent_id' to integers
third_table_melted_df['feature_id'] = third_table_melted_df['feature_id'].astype('Int64')  # 'Int64' to handle missing values (NaN)
third_table_melted_df['feature_seq_num'] = third_table_melted_df['feature_seq_num'].astype('Int64')
third_table_melted_df['metric_sequence_num'] = third_table_melted_df['metric_sequence_num'].astype('Int64')
#third_table_melted_df['metric_seqno'] = third_table_melted_df['metric_seqno'].astype('Int64')
third_table_melted_df['level_no'] = third_table_melted_df['level_no'].astype('Int64')
third_table_melted_df['parent_id'] = third_table_melted_df['parent_id'].astype('Int64')
#third_table_melted_df['metric_id'] = third_table_melted_df['metric_id'].astype('Int64')

# Convert 'metric_id', 'unique_identifier', 'feature_name', 'metrics', 'display_names', 'operating_system_type' to strings
third_table_melted_df['metric_id'] = third_table_melted_df['metric_id'].astype(str)
third_table_melted_df['unique_identifier'] = third_table_melted_df['unique_identifier'].astype(str)
third_table_melted_df['feature_name'] = third_table_melted_df['feature_name'].astype(str)
third_table_melted_df['metrics'] = third_table_melted_df['metrics'].astype(str)
third_table_melted_df['display_names'] = third_table_melted_df['display_names'].astype(str)
third_table_melted_df['operating_system_type'] = third_table_melted_df['operating_system_type'].astype(str)
third_table_melted_df['metric_nature'] = third_table_melted_df['metric_nature'].astype(str)

# Convert 'Value' to float
third_table_melted_df['Value'] = pd.to_numeric(third_table_melted_df['Value'], errors='coerce')

# %%
third_table_melted_df.dtypes

# %%
#Calculate 1st and 3rd quartiles
quartiles = third_table_melted_df.groupby(['operating_system_type', 'display_names'])['Value'].quantile([0.25, 0.75]).unstack()
quartiles.reset_index(inplace=True)
quartiles.columns.name = None  # Remove the index name
quartiles.columns = ['operating_system_type', 'display_names', '1st Quartile', '3rd Quartile']
#Calculate IQR
quartiles['IQR'] = quartiles['3rd Quartile'] - quartiles['1st Quartile']

# %%
quartiles.shape

# %%
quartiles_iqr_df = pd.merge(third_table_df, quartiles, on=['display_names', 'operating_system_type'], how='outer')

# %%
quartiles_iqr_df.shape

# %%
pd.set_option('display.max_columns', None)  # Show all columns 
pd.set_option('display.max_rows', 200)      # Show 100 rows 
quartiles_iqr_df.head()

# %%

def calculate_upper_lower(df, parent_column='parent_id'):
    # Ensure parent_column is present in the dataframe
    if parent_column not in df.columns:
        raise ValueError(f"{parent_column} not found in the dataframe")
    
    # Define the function to apply to each row
    def calculate_row(row):
        if pd.isna(row[parent_column]) or row[parent_column] == '':
            # Apply 1.25 logic for rows where parent_id is null, empty, or None
            upper = row['3rd Quartile'] + (1.25 * row['IQR'])
            lower = row['1st Quartile'] - (1.25 * row['IQR'])
        else:
            # Apply 0.75 logic for other rows
            upper = row['3rd Quartile'] + (0.75 * row['IQR'])
            lower = row['1st Quartile'] - (0.75 * row['IQR'])
        
        return pd.Series({'Upper': upper, 'Lower': lower})
    
    # Apply the function row by row
    df[['Upper', 'Lower']] = df.apply(calculate_row, axis=1)

    # Round the Upper column to 1 decimal place
    #df['Upper'] = df['Upper'].round(1)

    return df


# %%
upper_lower_df = calculate_upper_lower(quartiles_iqr_df)
#upper_lower_df =upper_lower_df[['display_names','operating_system_type','1st Quartile','3rd Quartile', 'IQR', 'Upper', 'Lower']]
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 200)      # Show 100 rows
upper_lower_df.head()

# %%
upper_lower_df.shape

# %%
import numpy as np
import pandas as pd

def calculate_last7_and_30_days(df):
    # Extract the date columns (skip the non-date columns like 'display_names' and 'operating_system_type')
    datecolumn = [col for col in df.columns if '-' in col]
    
    # Exclude the latest date column (the first one)
    last_7_columns = datecolumn[-8:-1]  # Get the last 7 columns excluding the latest date
    last_30_columns = datecolumn[-31:-1]

    # Extract the date part and convert them to datetime objects
    date_objects = [pd.to_datetime(col.split()[0], format='%d-%m-%Y') for col in datecolumn]

    # Get the column corresponding to the latest date
    latest_date = max(date_objects)
    latest_date_column = datecolumn[date_objects.index(latest_date)]

    # Create a new column 'Yesterday' with the values from the latest date column
    df.loc[:, 'Yesterday'] = df[latest_date_column]
    
    # Ensure numeric columns before performing mean calculation
    df[last_7_columns] = df[last_7_columns].apply(pd.to_numeric, errors='coerce')
    df[last_30_columns] = df[last_30_columns].apply(pd.to_numeric, errors='coerce')

    # Calculate the mean for each row across the last 7 and 30 date columns
    df.loc[:, 'last_7_days'] = df[last_7_columns].mean(axis=1)
    df.loc[:, 'last_30_days'] = df[last_30_columns].mean(axis=1)

    # Replace 0 and NaN values in 'last_7_days' and 'last_30_days' with NaN to avoid division by zero
    df['last_7_days'] = df['last_7_days'].replace(0, np.nan)
    df['last_30_days'] = df['last_30_days'].replace(0, np.nan)

    # Calculate the percentage change for 'Last 7 Days' with a check for NaN
    df.loc[:, '% Change Last 7 Days'] = np.where(
        df['last_7_days'].isna(), 0, 
        (df['Yesterday'] - df['last_7_days']) / df['last_7_days'] * 100
    )

    # Calculate the percentage change for 'Last 30 Days' with a check for NaN
    df.loc[:, '% Change Last 30 Days'] = np.where(
        df['last_30_days'].isna(), 0, 
        (df['Yesterday'] - df['last_30_days']) / df['last_30_days'] * 100
    )

    # Round the percentage changes to 1 decimal place
    df.loc[:, '% Change Last 7 Days'] = df['% Change Last 7 Days'].round(1)
    df.loc[:, '% Change Last 30 Days'] = df['% Change Last 30 Days'].round(1)
    
    return df


# %%
last_7_and_30_days_df  = calculate_last7_and_30_days(upper_lower_df)
#last_7_and_30_days_df = last_7_and_30_days_df[['metric_id', 'metrics', 'display_names', 'operating_system_type', 'feature_name', 'parent_id','Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days']]
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)
last_7_and_30_days_df.head()

# %%
last_7_and_30_days_df.shape

# %%
last_7_and_30_days_df[['1st Quartile', '3rd Quartile', 'IQR','Upper', 'Lower', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days']] = last_7_and_30_days_df[['1st Quartile', '3rd Quartile', 'IQR','Upper', 'Lower', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days']].round(3)

# %%
last_7_and_30_days_df.head()

# %%
last_7_and_30_days_df.shape

# %%
import pandas as pd

def add_colour_indicators(df):
    # Function to assign colour_indicator based on the logic
    def assign_colour_indicator(row):
        if row['metric_nature'] == 'Positive':
            if row['Yesterday'] > row['Upper']:
                return 'Positive Above'
            elif row['Yesterday'] < row['Lower']:
                return 'Positive Below'
            else:
                return 'Positive Within'
        elif row['metric_nature'] == 'Negative':
            if row['Yesterday'] > row['Upper']:
                return 'Negative Above'
            elif row['Yesterday'] < row['Lower']:
                return 'Negative Below'
            else:
                return 'Negative Within'
        return None  # In case metric_nature is neither positive nor negative

    # Function to assign colour_indicator1 based on colour_indicator values
    def assign_colour_indicator1(row):
        if row['metric_nature_indicator'] in ['Positive Above', 'Negative Below']:
            return 'Green'
        elif row['metric_nature_indicator'] in ['Positive Below', 'Negative Above']:
            return 'Red'
        elif row['metric_nature_indicator'] in ['Positive Within', 'Negative Within']:
            return 'White'
        return None  # In case colour_indicator is not set
    
    # Apply the function to create the colour_indicator column
    df['metric_nature_indicator'] = df.apply(assign_colour_indicator, axis=1)
    
    # Apply the function to create the colour_indicator1 column
    df['colour_indicator'] = df.apply(assign_colour_indicator1, axis=1)
    
    return df  # Return the modified DataFrame



# %%
# Call the function and get the updated DataFrame
colour_indicators_df  = add_colour_indicators(last_7_and_30_days_df)
colour_indicators_df = colour_indicators_df[['create_dt', 'metric_id', 'feature_name', 'metrics', 'display_names', 'operating_system_type', '1st Quartile', '3rd Quartile', 'IQR', 'Upper', 'Lower', 'Yesterday', 'last_7_days', 'last_30_days', '% Change Last 7 Days', '% Change Last 30 Days', 'metric_nature_indicator', 'colour_indicator']]

# Display the resulting dataframe
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 500)
colour_indicators_df.head()


# %%
#colour_indicators_df.to_csv(r'D:\Healthsore Data\Table4_athena.csv', index = False)



