import pandas as pd
import numpy as np
from datetime import datetime
df = pd.read_csv(r"D:\shalini\HS_ID.csv")




filtered_df = df[df['Feature'].isin(['SMART HELP'])]
#print(filtered_df[['Display_Names','8/29/2024','Hierarchy_ID (Num)', 'Parent_ID(Den)']].head(500))
#print(filtered_df.head(500))






def is_date_column(col_name):
    try:
        pd.to_datetime(col_name, format='%m/%d/%Y')
        return True
    except ValueError:
        return False

# Filter columns with date format 'm/d/Y'
date_columns = [col for col in filtered_df.columns if is_date_column(col)]
print(date_columns)


percentage_columns = []

# Iterate through each row and calculate the percentage for each DATE column
for idx, row in filtered_df.iterrows():
    # Iterate through all the DATE columns
    for date_col in date_columns:
        # Get the numerator (DATE value)
        numerator = row[date_col]
        
        # Get the parent hierarchy ID
        parent_hierarchyid = row['Parent_ID(Den)']
        
        # Check if the parent is empty
        if pd.isna(parent_hierarchyid) or parent_hierarchyid == "":
            # If the PARENTID is empty, keep the original DATE value
            percentage = numerator  # Assuming 100% when no parent is found
        else:
            # Find the row where HIERARCHYID == parent_hierarchyid to get the parent's DATE value
            parent_row = filtered_df[filtered_df['Hierarchy_ID (Num)'] == parent_hierarchyid]
            
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
        if percentage_column_name not in filtered_df.columns:
            df[percentage_column_name] = None  # Initialize the column
        
        # Set the percentage for the current row and current date column
        filtered_df.at[idx, percentage_column_name] = percentage



pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 100)
print(filtered_df.head(100))
