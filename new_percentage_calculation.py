import pandas as pd

# Read your CSV file into a DataFrame (assuming your CSV is structured with multiple DATE columns)
df = pd.read_csv('your_file.csv')  # Replace with your actual file path

# List of all DATE columns (assuming they start from 'DATE_1', 'DATE_2', ..., 'DATE_90')
date_columns = [col for col in df.columns if col.startswith('DATE')]

# Iterate through each DATE column and calculate the percentage
for date_col in date_columns:
    # Initialize an empty list to store the percentages for this column
    percentages = []
    
    # Iterate through each row and calculate the percentage
    for idx, row in df.iterrows():
        # Get the numerator (the DATE value corresponding to the current DATE column)
        numerator = row[date_col]
        
        # Get the parent hierarchy ID
        parent_hierarchyid = row['PARENTID']
        
        # Check if the parent is empty
        if pd.isna(parent_hierarchyid) or parent_hierarchyid == "":
            # If the PARENTID is empty, keep the original DATE value
            percentages.append(numerator)
        else:
            # Find the row where HIERARCHYID == parent_hierarchyid to get the parent's DATE value
            parent_row = df[df['HIERARCHYID'] == parent_hierarchyid]
            
            # If the parent exists, get the parent's DATE value from the corresponding column
            if not parent_row.empty:
                # The parent date value is taken from the same DATE column
                denominator = parent_row[date_col].values[0]
            else:
                denominator = 0  # In case no parent is found, avoid division by zero
            
            # Calculate the percentage
            if denominator != 0:
                percentage = (numerator / denominator) * 100
            else:
                percentage = 0  # To handle division by zero if no parent found
            
            # Append the calculated percentage
            percentages.append(round(percentage, 3))
    
    # Add the PERCENTAGE column for the current DATE column
    df[f'PERCENTAGE_{date_col}'] = percentages

# Save the DataFrame with the new percentage columns back to a CSV file
df.to_csv('output_with_percentages.csv', index=False)

# Display the DataFrame to check
print(df)