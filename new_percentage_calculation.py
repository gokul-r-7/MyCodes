import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('your_file.csv')

# Get all the columns that represent dates (assumed to be columns starting with 'DATE')
date_columns = [col for col in df.columns if col.startswith('DATE')]

# Initialize the list of columns to store the percentage values for each DATE column
percentage_columns = []

# Iterate through each row and calculate the percentage for each DATE column
for idx, row in df.iterrows():
    # Iterate through all the DATE columns
    for date_col in date_columns:
        # Get the numerator (DATE value)
        numerator = row[date_col]
        
        # Get the parent hierarchy ID
        parent_hierarchyid = row['PARENTID']
        
        # Check if the parent is empty
        if pd.isna(parent_hierarchyid) or parent_hierarchyid == "":
            # If the PARENTID is empty, keep the original DATE value
            percentage = 100.0  # Assuming 100% when no parent is found
        else:
            # Find the row where HIERARCHYID == parent_hierarchyid to get the parent's DATE value
            parent_row = df[df['HIERARCHYID'] == parent_hierarchyid]
            
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
        percentage_column_name = f"{date_col}_PERCENTAGE"
        if percentage_column_name not in df.columns:
            df[percentage_column_name] = None  # Initialize the column
        
        # Set the percentage for the current row and current date column
        df.at[idx, percentage_column_name] = percentage

# Save the modified DataFrame to a new CSV
df.to_csv('modified_file.csv', index=False)

# Display the updated DataFrame
print(df)
