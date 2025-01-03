import pandas as pd

def calculate_sums(df, items_dict):
    # Sum date columns for each combination of display_names and operating_system_type
    sum_df = df.groupby(['display_names', 'operating_system_type']).sum().reset_index()
    
    # Identify iOS and Android rows
    ios_rows = sum_df[sum_df['operating_system_type'] == 'Apple iOS']
    android_rows = sum_df[sum_df['operating_system_type'] == 'Google Android']
    
    # Initialize an empty list to hold 'Both' rows
    both_rows = []

    # Iterate over each item in the dictionary
    for item in items_dict:
        if any(ios_rows['display_names'].str.contains(item)) or any(android_rows['display_names'].str.contains(item)):
            # Get the relevant iOS and Android rows for this item
            ios_item_row = ios_rows[ios_rows['display_names'].str.contains(item)]
            android_item_row = android_rows[android_rows['display_names'].str.contains(item)]
            
            # Create a new row for 'Both' by summing iOS and Android values
            new_row = {'display_names': item, 'operating_system_type': 'Both'}
            for date in df.columns[2:]:  # Assuming date columns start from index 2
                new_row[date] = ios_item_row[date].sum() + android_item_row[date].sum()
            
            both_rows.append(new_row)

    # Convert both_rows to a DataFrame
    both_df = pd.DataFrame(both_rows)

    # Append the 'Both' rows to the sum_df
    sum_df = pd.concat([sum_df, both_df], ignore_index=True)

    # Drop unnecessary columns if they exist
    sum_df = sum_df.drop(columns=['operating_system_type_ios', 'operating_system_type_Android'], errors='ignore')

    return sum_df
