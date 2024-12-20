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
