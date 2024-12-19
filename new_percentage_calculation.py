import pandas as pd
import numpy as np

# Load the CSV file
file_path = '/path/to/your/file.csv'
df = pd.read_csv(file_path)

# Identify date columns (columns with recognizable date formats)
date_columns = [col for col in df.columns if '/' in col or '-' in col]

# Create a dictionary to map Parent_ID values
parent_values = df.set_index('Hierarchy_ID (Num)')[date_columns]

# Define a function to calculate percentages
def calculate_percentage_safe(row):
    if row['Parent_ID(Den)'] in parent_values.index:
        parent_row = parent_values.loc[row['Parent_ID(Den)']]
        # Replace zero denominators with NaN to avoid division errors
        safe_parent_row = parent_row.replace(0, np.nan)
        percentages = (row[date_columns] / safe_parent_row) * 100
        return percentages.round(3)
    return row[date_columns]  # Keep the same values if Parent_ID is not found

# Apply the function row-wise
df[date_columns] = df.apply(calculate_percentage_safe, axis=1)

# Save the updated dataframe to a new file
df.to_csv('/path/to/output/file.csv', index=False)