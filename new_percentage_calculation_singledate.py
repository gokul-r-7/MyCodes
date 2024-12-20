import pandas as pd

# Sample DataFrame
data = {
    'DATE': [46, 46, 36, 5, 4, 22, 967, 518, 147, 155, 29, 120, 265, 236, 14, 7, 18],
    'HIERARCHYID': ["Google Android_SH_1.1", "Google Android_SH_1.1.1", "Google Android_SH_1.1.1.1", "Google Android_SH_1.1.1.2", "Google Android_SH_1.1.1.3", "Google Android_SH_1.2", "Google Android_SH_1.3", "Google Android_SH_1.3.1", "Google Android_SH_1.3.2", "Google Android_SH_1.3.3", "Google Android_SH_1.3.4", "Google Android_SH_1.3.5", "Google Android_SH_1.4", "Google Android_SH_1.5", "Google Android_SH_1.6", "Google Android_SH_1.7", "Google Android_SH_1.8"
],
    'PARENTID': ["Google Android_SH_1", "Google Android_SH_1.1", "Google Android_SH_1.1.1", "Google Android_SH_1.1.1", "Google Android_SH_1.1.1", "Google Android_SH_1", "Google Android_SH_1", "Google Android_SH_1.3", "Google Android_SH_1.3", "Google Android_SH_1.3", "Google Android_SH_1.3", "Google Android_SH_1.3", "Google Android_SH_1", "Google Android_SH_1", "Google Android_SH_1", "Google Android_SH_1", "Google Android_SH_1"]
}

df = pd.DataFrame(data)

# Initialize an empty list to store the percentages
percentages = []

# Iterate through each row and calculate the percentage
for idx, row in df.iterrows():
    # Get the numerator, which is the DATE value corresponding to the HIERARCHYID
    numerator = row['DATE']
    print("Numerator:", numerator)
    
    # Get the parent hierarchy ID
    parent_hierarchyid = row['PARENTID']
    
    # Check if the parent is empty
    if pd.isna(parent_hierarchyid) or parent_hierarchyid == "":
        # If the PARENTID is empty, keep the original DATE value
        percentages.append(numerator)
        print("PARENTID is empty, keeping the original DATE value.")
    else:
        # Find the row where HIERARCHYID == parent_hierarchyid to get the parent's DATE value
        parent_row = df[df['HIERARCHYID'] == parent_hierarchyid]
        
        # If the parent exists, get the parent's DATE value
        if not parent_row.empty:
            denominator = parent_row['DATE'].values[0]
        else:
            denominator = 0  # In case no parent is found, avoid division by zero
        print("Denominator:", denominator)
        
        # Calculate the percentage
        if denominator != 0:
            percentage = (numerator / denominator) * 100
        else:
            percentage = 0  # To handle division by zero if no parent found
        
        # Round to 3 decimal places and append to the list
        percentages.append(round(percentage, 3))
        print("Percentage:", percentage)

# Add the PERCENTAGE column to the DataFrame
df['PERCENTAGE'] = percentages

# Display the DataFrame
print(df)
