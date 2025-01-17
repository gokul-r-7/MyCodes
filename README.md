def generate_new_rows_optimized(df, hierarchy_id_col, parent_id_col):
    """
    Generate new rows for every unique 'display_names' and calculate results efficiently.

    Args:
    - df (pd.DataFrame): Input DataFrame.
    - hierarchy_id_col (str): Name of the Hierarchy_ID column.
    - parent_id_col (str): Name of the Parent_ID column.

    Returns:
    - pd.DataFrame: Updated DataFrame with new rows.
    """
    # Extract date columns
    date_cols = df.columns[5:]
    
    # Step 1: Calculate "Both" operating system rows
    both_rows = (
        df.pivot_table(
            index="display_names",
            values=date_cols,
            aggfunc="sum",
            columns="operating_system_type"
        )
        .sum(axis=1, level=0)  # Sum iOS and Android rows
        .reset_index()
    )
    both_rows['operating_system_type'] = 'Both'
    both_rows[hierarchy_id_col] = None
    both_rows[parent_id_col]None 
    
### SUM

Here is the complete and **optimized version** of the function. It eliminates redundant loops and uses vectorized operations to improve performance:

### Optimized Function
```python
def generate_new_rows_optimized(df, hierarchy_id_col, parent_id_col):
    """
    Generate new rows for every unique 'display_names' and calculate results efficiently.

    Args:
    - df (pd.DataFrame): Input DataFrame.
    - hierarchy_id_col (str): Name of the Hierarchy_ID column.
    - parent_id_col (str): Name of the Parent_ID column.

    Returns:
    - pd.DataFrame: Updated DataFrame with new rows.
    """
    # Extract date columns
    date_cols = df.columns[5:]

    # Step 1: Create rows for "Both"
    both_rows = (
        df.groupby("display_names")[date_cols]
        .sum()
        .reset_index()
        .assign(
            operating_system_type="Both",
            metric_id=lambda x: "Both_ID_" + (x.index + 1).astype(str),
            Hierarchy_ID=None,
            Parent_ID=None,
        )
    )

    # Step 2: Add formulas based on Parent_ID
    new_rows = []
    for _, row in both_rows.iterrows():
        display_name = row["display_names"]

        # Filter iOS and Android rows for the same display_name
        ios_rows = df[
            (df["display_names"] == display_name)
            & (df["operating_system_type"] == "Apple iOS")
        ]
        android_rows = df[
            (df["display_names"] == display_name)
            & (df["operating_system_type"] == "Google Android")
        ]

        # Calculate numerator
        numerator = ios_rows[date_cols].sum().values + android_rows[date_cols].sum().values

        if pd.isna(row[parent_id_col]):  # If Parent_ID is null
            formula_row = row.copy()
            formula_row.update(dict(zip(date_cols, numerator)))
            new_rows.append(formula_row)
        else:  # If Parent_ID is not null
            parent_hierarchy_id = row[parent_id_col]
            parent_rows = df[df[hierarchy_id_col] == parent_hierarchy_id]

            if not parent_rows.empty:
                ios_parent_rows = parent_rows[
                    parent_rows["operating_system_type"] == "Apple iOS"
                ]
                android_parent_rows = parent_rows[
                    parent_rows["operating_system_type"] == "Google Android"
                ]

                # Denominator
                denominator = ios_parent_rows[date_cols].sum().values + android_parent_rows[date_cols].sum().values

                # Result
                result = numerator + denominator * 100
                formula_row = row.copy()
                formula_row.update(dict(zip(date_cols, result)))
                new_rows.append(formula_row)

    # Combine the new rows
    final_df = pd.concat([df, both_rows, pd.DataFrame(new_rows)], ignore_index=True)
    return final_df