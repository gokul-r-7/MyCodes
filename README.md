def process_csv_data(df, hierarchy_col, parent_id_col):
    """
    Processes the dataframe to generate new rows based on the given logic.
    
    Args:
        df (pd.DataFrame): The input dataframe.
        hierarchy_col (str): The column name for hierarchy ID.
        parent_id_col (str): The column name for parent ID.
        
    Returns:
        pd.DataFrame: The updated dataframe with the generated rows.
    """
    # Create a list for new rows and identify date columns
    new_rows = []
    date_cols = [col for col in df.columns if "2024" in col]  # Identify date columns
    sequence_number = 1

    # Iterate over unique display_names
    for display_name in df["display_names"].unique():
        # Filter for Apple iOS and Google Android rows for this display_name
        ios_rows = df[(df["display_names"] == display_name) & 
                      (df["operating_system_type"] == "Apple iOS")]
        android_rows = df[(df["display_names"] == display_name) & 
                          (df["operating_system_type"] == "Google Android")]
        
        # Check if the rows exist
        if ios_rows.empty or android_rows.empty:
            continue
        
        # Sum the date values for numerator
        ios_sums = ios_rows[date_cols].sum()
        android_sums = android_rows[date_cols].sum()
        numerator = ios_sums + android_sums
        
        # Determine parent_id_old status and calculate denominator if needed
        for _, ios_row in ios_rows.iterrows():
            if pd.isna(ios_row["parent_id_old"]):  # Case: parent_id_old is null/empty
                result = numerator
            else:  # Case: parent_id_old is not null/empty
                parent_hierarchy_id = ios_row["parent_id_old"]
                
                # Retrieve parent rows
                parent_ios_row = df[(df[hierarchy_col] == parent_hierarchy_id) & 
                                    (df["operating_system_type"] == "Apple iOS")]
                parent_android_row = df[(df[hierarchy_col] == parent_hierarchy_id) & 
                                        (df["operating_system_type"] == "Google Android")]
                
                # Sum values and store in dictionaries
                parent_sums = {
                    "parent_ios_sums": parent_ios_row[date_cols].sum() if not parent_ios_row.empty else 0,
                    "parent_android_sums": parent_android_row[date_cols].sum() if not parent_android_row.empty else 0
                }
                
                # Remove zero values
                parent_sums = {k: v for k, v in parent_sums.items() if v.sum() != 0}
                
                if parent_sums:  # Check if the dictionary has values
                    denominator = sum(parent_sums.values())
                    result = (numerator + denominator) * 100
                else:
                    continue  # Skip if no valid parent sums exist
            
            # Generate new row
            new_row = {
                "display_names": display_name,
                "metric_id": f"Both_ID_{sequence_number}",
                "hierarchy_id": None,
                "parent_id_old": None,
                "parent_id": None,
                **result.to_dict()  # Add date column values
            }
            new_rows.append(new_row)
            sequence_number += 1
    
    # Append new rows to the original dataframe
    new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return new_df