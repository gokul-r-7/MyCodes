def generate_new_rows_corrected(df, hierarchy_id_col, parent_id_col):
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

        # Add logic for Parent_ID
        if not ios_rows.empty and not android_rows.empty:
            parent_ids = df.loc[
                (df["display_names"] == display_name) & df[parent_id_col].notna(), parent_id_col
            ].unique()

            for parent_id in parent_ids:
                parent_rows = df[df[hierarchy_id_col] == parent_id]

                if not parent_rows.empty:
                    ios_parent_rows = parent_rows[
                        parent_rows["operating_system_type"] == "Apple iOS"
                    ]
                    android_parent_rows = parent_rows[
                        parent_rows["operating_system_type"] == "Google Android"
                    ]

                    # Denominator
                    denominator = (
                        ios_parent_rows[date_cols].sum().values + android_parent_rows[date_cols].sum().values
                    )

                    # Calculate result
                    result = numerator + denominator * 100

                    formula_row = row.copy()
                    formula_row[parent_id_col] = parent_id
                    formula_row.update(dict(zip(date_cols, result)))
                    new_rows.append(formula_row)

    # Combine original rows with both_rows and new formula rows
    final_df = pd.concat([df, both_rows, pd.DataFrame(new_rows)], ignore_index=True)
    return final_df