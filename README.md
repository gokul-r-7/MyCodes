def calculate_both(df, datecolumns, metricid, parentid):
    # Ensure these columns exist in the DataFrame
    if metricid not in df.columns or parentid not in df.columns:
        raise ValueError(f"'{metricid}' or '{parentid}' columns not found in DataFrame.")
    
    # Initialize an empty list to store the new rows
    new_rows = []
    
    # Iterate through each row to generate new rows
    for idx, row in df.iterrows():
        # Check if 'parent_id_old' is None, NaN, or empty
        if pd.isna(row[parentid]) or row[parentid] == None or row[parentid] == '':
            # Handle the case where parent_id_old is missing (second function logic)
            display_name = row['display_names']
            display_name_df = df[df['display_names'] == display_name]
            
            # Create a new row for 'Both' operating system type
            new_row = row.to_dict()
            
            # Sum the date columns for iOS and Android
            for date_column in datecolumns:
                ios_sum = display_name_df[display_name_df['operating_system_type'] == 'Apple iOS'][date_column].sum()
                android_sum = display_name_df[display_name_df['operating_system_type'] == 'Google Android'][date_column].sum()
                
                # Add the sum of both iOS and Android
                new_row[date_column] = ios_sum + android_sum
            
            new_row['operating_system_type'] = 'Both'
            
            # Append the new row to the list of new rows
            new_rows.append(new_row)
        
        else:
            # Handle the case where parent_id_old is not missing (first function logic)
            new_row = row.to_dict()

            # Numerator: Sum the iOS and Android values for the same display_name
            ios_sum = df[(df['operating_system_type'] == 'Apple iOS') & (df['display_names'] == row['display_names'])][datecolumns].sum()
            android_sum = df[(df['operating_system_type'] == 'Google Android') & (df['display_names'] == row['display_names'])][datecolumns].sum()

            for date_col in datecolumns:
                numerator = ios_sum[date_col] + android_sum[date_col]

                # Denominator: Check if the current `parent_id` exists in `metric_id` for both operating systems
                denominator_ios = df[
                    (df[metricid] == row[parentid]) & (df['operating_system_type'] == 'Apple iOS')
                ][date_col].sum()

                denominator_android = df[
                    (df[metricid] == row[parentid]) & (df['operating_system_type'] == 'Google Android')
                ][date_col].sum()

                denominator = denominator_ios + denominator_android

                # Calculate percentage
                if denominator != 0:
                    result = (numerator / denominator) * 100
                else:
                    result = 0

                # Add the calculated result to the new row
                new_row[date_col] = round(result, 3)

            # Set the operating_system_type to "Both"
            new_row['operating_system_type'] = 'Both'

            # Add the new row to the list of new rows
            new_rows.append(new_row)

    # Convert the list of new rows into a DataFrame
    new_rows_df = pd.DataFrame(new_rows)
    
    # Drop duplicates based on `display_names` and `operating_system_type`
    new_rows_df = new_rows_df.drop_duplicates(subset=['display_names', 'operating_system_type'], keep='first')

    # Append the new rows to the original DataFrame
    df = pd.concat([df, new_rows_df], ignore_index=True)

    return new_rows_df