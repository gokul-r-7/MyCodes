import pandas as pd

# Sample DataFrame (Replace with your actual DataFrame)
data = {
    'number_column': [100, 200, 150, 300, 250],
    'HierarchyID': ['A', 'B', 'C', 'D', 'E'],
    'ParentID': ['B', 'C', 'D', 'E', 'A']
}

df = pd.DataFrame(data)

# Step 1: Create a mapping of ParentID to HierarchyID index for reference
parent_to_hierarchy = {row['HierarchyID']: row['number_column'] for _, row in df.iterrows()}

# Step 2: Calculate the percentage
def calculate_percentage(row):
    numerator = row['number_column']
    denominator = parent_to_hierarchy.get(row['ParentID'], 1)  # Default to 1 if ParentID not found
    return round((numerator / denominator) * 100, 3)

# Apply the function to calculate percentage for each row
df['percentage'] = df.apply(calculate_percentage, axis=1)

# Display the result
print(df)












import pandas as pd

# Sample DataFrame
data = {
    'DATE': [2021, 2022, 2023, 2024],
    'HIERARCHYID': ['A', 'B', 'C', 'D'],
    'PARENTID': ['P', 'P', 'Q', 'Q']
}

df = pd.DataFrame(data)

# Create dictionaries to map HIERARCHYID to their indices
hierarchy_index = {value: idx for idx, value in enumerate(df['HIERARCHYID'])}
parent_index = {value: idx for idx, value in enumerate(df['PARENTID'])}

# Initialize an empty list to store the percentages
percentages = []

# Iterate through each row and calculate the percentage
for idx, row in df.iterrows():
    # Get the numerator from the HIERARCHYID index of the DATE
    numerator = hierarchy_index[row['HIERARCHYID']]
    
    # Get the denominator from the PARENTID index, using HIERARCHYID of the parent
    parent_hierarchyid = row['PARENTID']
    denominator = hierarchy_index[df[df['HIERARCHYID'] == parent_hierarchyid].iloc[0]['HIERARCHYID']]
    
    # Calculate the percentage and round to 3 decimal places
    percentage = (numerator / denominator) * 100 if denominator != 0 else 0
    percentages.append(round(percentage, 3))

# Add the PERCENTAGE column to the DataFrame
df['PERCENTAGE'] = percentages

# Display the DataFrame
print(df)





