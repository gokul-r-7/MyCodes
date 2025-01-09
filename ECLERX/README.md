

def calculate_percentage(df, datecolumns, column1, column2):
    # Ensure these columns exist in the DataFrame
    if column1 not in df.columns or column2 not in df.columns:
        raise ValueError(f"'{column1}' or '{column2}' columns not found in DataFrame.")
    
    # Iterate through each row and calculate the percentage for each DATE column
    for idx, row in df.iterrows():
        # Iterate through all the DATE columns
        for date_col in datecolumns:
            # Get the numerator (DATE value)
            numerator = row[date_col]
            
            
            # Get the parent hierarchy ID
            parent_hierarchyid = row[column2]
            
            
            # Check if the parent is empty
            if pd.isna(parent_hierarchyid) or parent_hierarchyid == "":
                # If the PARENTID is empty, keep the original DATE value
                percentage = numerator  # Assuming 100% when no parent is found
            else:
                # Find the row where HIERARCHYID == parent_hierarchyid to get the parent's DATE value
                parent_row = df[df[column1] == parent_hierarchyid]
                
                
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
                df[percentage_column_name] = None  # Initialize the column
            
            # Set the percentage for the current row and current date column
            df.at[idx, percentage_column_name] = percentage

    return df


df = calculate_percentage(sample_df, date_columns, 'hierarchy_id', 'parent_id_old')
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 5)
df.head(5)




# Function to calculate combined metrics (same as in the original code)
def calculate_both_metrics_2(df, new_row, denominator_ios, denominator_android, numerator_ios, numerator_android):
    # Identify the date columns (all columns except 'display_names' and 'operating_system_type')
    date_columns = [col for col in df.columns if col not in ['display_names', 'operating_system_type']]

    # Iterate through each date column to calculate the combined value
    for date in date_columns:
        # Access the date value directly from the rows (this assumes there's only one row for each condition)
        denominator_ios_value = denominator_ios[date].values[0]
        denominator_android_value = denominator_android[date].values[0]
        numerator_ios_value = numerator_ios[date].values[0]
        numerator_android_value = numerator_android[date].values[0]

        # Apply the formula for the combined troubleshooting value
        result = (numerator_ios_value + numerator_android_value) / (denominator_ios_value + denominator_android_value)
        result = result * 100
        # Add the calculated result to the new row
        new_row[date] = result

    # Convert the new row into a DataFrame
    new_row_df = pd.DataFrame([new_row])

    # Concatenate the new row with the original DataFrame
    df = pd.concat([df, new_row_df], ignore_index=True)

    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.max_rows', 100)
    
    return new_row_df


# Initialize an empty list to store the result DataFrames
combined_dfs2 = []

# Loop over the config and dynamically fetch numerators, denominators, and new rows
for item in config2:
    numerator_ios = sample_actual_data_df[(sample_actual_data_df['display_names'] == item['numerator_ios']) & (sample_actual_data_df['operating_system_type'] == 'Apple iOS')]
    numerator_android = sample_actual_data_df[(sample_actual_data_df['display_names'] == item['numerator_android']) & (sample_actual_data_df['operating_system_type'] == 'Google Android')]

    denominator_ios = sample_actual_data_df[(sample_actual_data_df['display_names'] == item['denominator_ios']) & (sample_actual_data_df['operating_system_type'] == 'Apple iOS')]
    denominator_android = sample_actual_data_df[(sample_actual_data_df['display_names'] == item['denominator_android']) & (sample_actual_data_df['operating_system_type'] == 'Google Android')]

    # Pass the current configuration to the function
    result_df = calculate_both_metrics_2(
        sample_actual_data_df, 
        item['new_row'], 
        denominator_ios, 
        denominator_android, 
        numerator_ios, 
        numerator_android
    )
    
    # Append the result to the list of DataFrames
    combined_dfs2.append(result_df)

# Concatenate all DataFrames into one
combined_df2 = pd.concat(combined_dfs2, ignore_index=True)

# Show the first 20 rows of the combined DataFrame
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', 500)      # Show 100 rows
combined_df2.head(500)
