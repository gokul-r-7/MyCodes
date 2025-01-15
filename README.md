[767397783814_ps-appteam-rw-n]
aws_access_key_id=ASIA3FLDY5EDPPDGQX5W
aws_secret_access_key=qtidqOJfI0pW2/ehMbgRo0RXqEIoFo84tlqxxDxD
aws_session_token=IQoJb3JpZ2luX2VjEFgaCXVzLWVhc3QtMSJHMEUCIQCT8aXDP1VV1PSqNTkBOi0ov403yG3q1qr0Vi7oCXAOkQIgdgeOkwWA4ftdNvqevVKntIeZhJIpkmTo2e2VI0v2OL8qlAMIQBAAGgw3NjczOTc3ODM4MTQiDE9a1Jkwd7vkiNJ5xCrxAmp07NyY+FaPRw8cv8KWtFo6RdG8NLN2TWI28cRACZyja3B/62/YjR07OGn9wjW5TzsC8Il9qzYFc1yeI6ycGs6M4F+qMK9ReNkdkL7sZaLXBj2wYws2g+M4Umjf07Z65b2X5BbHvQVlv/xMr2Nsu6D+d7Rwm+qmjJ6b9cauYBXnz+W48okqx9fSz9R08QV5UIKnNFd9JidGhGl6E2smgtufKCAFMlM6EAk3CMNSLPRC0yqsahg8kgM+1e2qcJw6PUkMcVGjtqal0qZVD6vluQiPuXVOjCgpDCG0aJ+NWuQ2J56uNnYaQKdDMzDIaJwyIFLLYgSdxyDPlx10IKdWtgNkl6fXXTMIhgYm0ecWUpPKenRyUAAIJ82OfSLB84xi9LNCdY2b7aSRTfjnIlwJwq7c1hqbVxIip2OZu4hTwtOyWPzRK4ijt989dDG0mu8kMCJ4UeosUckBp1AVUbkZ5XBltFiix3gUbmbcI3YW/1+YJjCagu67BjqmAV1jWPOoTxghvMUxVvp822j8MAGGfzL6wPXRM7EwXfWVEOfsVyOPg+31BcURriL6D3FeyKNkDsR+Q346kMLPdc0Y91H59oTTrM0RreVKjDvoPymd5CXvg9BTviPDXQGKmID1ICjRMYgCNxQfRyWTGfmf/kRIhpAu3XoB8AA3OHal3zV80KyRL9VEEjfN51wAdPHsbs1brvgpyKHIUrWuDrIesUELQrs=




import pandas as pd

def process_dataframe(df, hierarchy_col, parent_col):
    """
    Processes a DataFrame to add new rows for each distinct 'display_names' with specific rules for 'Both' operating_system_type.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        hierarchy_col (str): The column name for Hierarchy_ID.
        parent_col (str): The column name for Parent_ID.
        
    Returns:
        pd.DataFrame: The modified DataFrame with new rows and updated values.
    """
    # Step 1: Generate new rows for each distinct "display_names" with "Both" as operating_system_type
    distinct_display_names = df['display_names'].unique()
    new_rows = []

    for idx, name in enumerate(distinct_display_names, start=1):
        new_row = {
            'metric_id': max(df['metric_id']) + idx,  # Generate sequential metric_id
            'display_names': name,
            'operating_system_type': 'Both',
            hierarchy_col: None,
            parent_col: None,
        }
        # Initialize date columns as 0 for now
        for col in df.columns:
            if isinstance(col, pd.Timestamp):
                new_row[col] = 0
        new_rows.append(new_row)

    # Add the new rows to the dataframe
    new_rows_df = pd.DataFrame(new_rows)
    extended_df = pd.concat([df, new_rows_df], ignore_index=True)

    # Step 2: Update date column values based on the specified formulas
    for index, row in new_rows_df.iterrows():
        name = row['display_names']
        os_ios = df[(df['display_names'] == name) & (df['operating_system_type'] == 'Apple iOS')]
        os_android = df[(df['display_names'] == name) & (df['operating_system_type'] == 'Google Android')]

        for date_col in df.columns[6:]:  # Assuming date columns start from index 6
            if pd.isna(row[parent_col]):  # If Parent_ID is empty
                row[date_col] = os_ios[date_col].sum() + os_android[date_col].sum()
            else:  # If Parent_ID is not empty
                parent_id = row[parent_col]
                parent_ios = df[(df[hierarchy_col] == parent_id) & (df['operating_system_type'] == 'Apple iOS')]
                parent_android = df[(df[hierarchy_col] == parent_id) & (df['operating_system_type'] == 'Google Android')]
                if not parent_ios.empty and not parent_android.empty:
                    row[date_col] = (
                        os_ios[date_col].sum() + os_android[date_col].sum()
                    ) / (parent_ios[date_col].sum() + parent_android[date_col].sum())

    return extended_df

# Example usage:
file_path = '/mnt/data/HS_ID 2 copy.xlsx'
df = pd.read_excel(file_path, sheet_name='in')
processed_df = process_dataframe(df, 'Hierarchy_ID', 'Parent_ID')

# Save the final dataframe to a file
output_path = '/mnt/data/processed_dataset.xlsx'
processed_df.to_excel(output_path, index=False)
print(f"File saved to: {output_path}")
