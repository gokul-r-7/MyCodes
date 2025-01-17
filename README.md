[767397783814_ps-appteam-rw-n]
aws_access_key_id=ASIA3FLDY5EDPPDGQX5W
aws_secret_access_key=qtidqOJfI0pW2/ehMbgRo0RXqEIoFo84tlqxxDxD
aws_session_token=IQoJb3JpZ2luX2VjEFgaCXVzLWVhc3QtMSJHMEUCIQCT8aXDP1VV1PSqNTkBOi0ov403yG3q1qr0Vi7oCXAOkQIgdgeOkwWA4ftdNvqevVKntIeZhJIpkmTo2e2VI0v2OL8qlAMIQBAAGgw3NjczOTc3ODM4MTQiDE9a1Jkwd7vkiNJ5xCrxAmp07NyY+FaPRw8cv8KWtFo6RdG8NLN2TWI28cRACZyja3B/62/YjR07OGn9wjW5TzsC8Il9qzYFc1yeI6ycGs6M4F+qMK9ReNkdkL7sZaLXBj2wYws2g+M4Umjf07Z65b2X5BbHvQVlv/xMr2Nsu6D+d7Rwm+qmjJ6b9cauYBXnz+W48okqx9fSz9R08QV5UIKnNFd9JidGhGl6E2smgtufKCAFMlM6EAk3CMNSLPRC0yqsahg8kgM+1e2qcJw6PUkMcVGjtqal0qZVD6vluQiPuXVOjCgpDCG0aJ+NWuQ2J56uNnYaQKdDMzDIaJwyIFLLYgSdxyDPlx10IKdWtgNkl6fXXTMIhgYm0ecWUpPKenRyUAAIJ82OfSLB84xi9LNCdY2b7aSRTfjnIlwJwq7c1hqbVxIip2OZu4hTwtOyWPzRK4ijt989dDG0mu8kMCJ4UeosUckBp1AVUbkZ5XBltFiix3gUbmbcI3YW/1+YJjCagu67BjqmAV1jWPOoTxghvMUxVvp822j8MAGGfzL6wPXRM7EwXfWVEOfsVyOPg+31BcURriL6D3FeyKNkDsR+Q346kMLPdc0Y91H59oTTrM0RreVKjDvoPymd5CXvg9BTviPDXQGKmID1ICjRMYgCNxQfRyWTGfmf/



import pandas as pd
import numpy as np

def generate_new_rows(df, hierarchy_id_col, parent_id_col):
    """
    Generate new rows for every unique 'display_names' and calculate results based on given formulas.

    Args:
    - df (pd.DataFrame): Input DataFrame.
    - hierarchy_id_col (str): Name of the Hierarchy_ID column.
    - parent_id_col (str): Name of the Parent_ID column.

    Returns:
    - pd.DataFrame: Updated DataFrame with new rows.
    """
    # Extract date columns (assume these are all numeric columns after the first 5 columns)
    date_cols = df.columns[5:]

    # Initialize a list to hold new rows
    new_rows = []

    # Step 1: Generate rows for "Both" operating_system_type
    unique_display_names = df['display_names'].unique()
    for idx, display_name in enumerate(unique_display_names, start=1):
        ios_rows = df[(df['display_names'] == display_name) & (df['operating_system_type'] == 'Apple iOS')]
        android_rows = df[(df['display_names'] == display_name) & (df['operating_system_type'] == 'Google Android')]

        # Sum date values across operating systems
        date_sums = ios_rows[date_cols].sum().values + android_rows[date_cols].sum().values

        # Create a new row for "Both"
        new_row = {
            'metric_id': f'Both_ID_{idx}',
            'display_names': display_name,
            'operating_system_type': 'Both',
            hierarchy_id_col: np.nan,
            parent_id_col: np.nan,
        }
        new_row.update(dict(zip(date_cols, date_sums)))
        new_rows.append(new_row)

    # Step 2: Calculate formulas for rows based on Parent_ID
    for new_row in new_rows:
        display_name = new_row['display_names']

        if pd.isna(new_row[parent_id_col]):  # If Parent_ID is null
            ios_rows = df[(df['display_names'] == display_name) & (df['operating_system_type'] == 'Apple iOS')]
            android_rows = df[(df['display_names'] == display_name) & (df['operating_system_type'] == 'Google Android')]

            # Numerator is the sum of date values
            numerator = ios_rows[date_cols].sum().values + android_rows[date_cols].sum().values
            formula_row = new_row.copy()
            formula_row.update(dict(zip(date_cols, numerator)))
            new_rows.append(formula_row)
        else:  # If Parent_ID is not null
            parent_hierarchy_id = new_row[parent_id_col]
            parent_rows = df[df[hierarchy_id_col] == parent_hierarchy_id]
            if not parent_rows.empty:
                ios_parent_rows = parent_rows[parent_rows['operating_system_type'] == 'Apple iOS']
                android_parent_rows = parent_rows[parent_rows['operating_system_type'] == 'Google Android']

                # Denominator is the sum of date values for the parent rows
                denominator = ios_parent_rows[date_cols].sum().values + android_parent_rows[date_cols].sum().values
                numerator = np.array(new_row[date_cols])
                result = numerator + denominator * 100

                formula_row = new_row.copy()
                formula_row.update(dict(zip(date_cols, result)))
                new_rows.append(formula_row)

    # Combine the original DataFrame with the new rows
    updated_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return updated_df



updated_df = generate_new_rows(df, 'Hierarchy_ID', 'Parent_ID')

