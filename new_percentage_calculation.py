import pandas as pd

# Sample DataFrame for demonstration
data = {
    'Display_Name': ['A', 'B', 'C', 'D'],
    'HierarchyID': ['H1', 'H2', 'H3', 'H4'],
    'ParentID': ['P1', 'P2', 'P1', 'P3'],
    'Date_Column': [20230101, 20230101, 20230102, 20230102]
}

df = pd.DataFrame(data)

# Initialize the Percentage column
df['Percentage'] = 0.0

# Iterate through the rows of the dataframe
for idx, row in df.iterrows():
    # Get the HierarchyID (numerator) and ParentID
    numerator = row['HierarchyID']
    parent_id = row['ParentID']
    date_column = row['Date_Column']
    
    # Filter the dataframe for the same date
    date_df = df[df['Date_Column'] == date_column]
    
    # Find the row where ParentID matches HierarchyID in the same date group (denominator)
    denominator_row = date_df[date_df['HierarchyID'] == parent_id]
    
    if not denominator_row.empty:
        # Extract the index of the denominator row
        denominator_idx = denominator_row.index[0]
        
        # Calculate the percentage
        percentage = (1 / (denominator_idx + 1)) * 100  # denominator + 1 is for 1-index based logic
        
        # Assign the result to the 'Percentage' column
        df.at[idx, 'Percentage'] = round(percentage, 3)

print(df)
