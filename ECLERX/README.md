import pandas as pd
import numpy as np

def calculate_last_7_days(df):
    # Assume the last 7 date columns are the last 7 columns in the DataFrame
    date_columns = df.columns[-7:]
    
    # Create a new column for last_7_days
    df['last_7_days'] = np.nan
    
    # Process each row in the DataFrame
    for index, row in df.iterrows():
        if pd.isnull(row['parent_id']) or row['parent_id'] == 0:
            # Calculate mean for the last 7 dates
            df.at[index, 'last_7_days'] = row[date_columns].mean()
        else:
            # Calculate sum_product
            actual_number_df = row[date_columns]
            sum_product = (df.loc[df['feature_names'] == row['feature_names'], 'all_metrics_df'].values * actual_number_df.sum()).sum()
            result = sum_product / actual_number_df.sum() if actual_number_df.sum() != 0 else np.nan
            df.at[index, 'last_7_days'] = result
            
    return df

# Example usage
# df = pd.read_csv('your_data.csv')  # Load your DataFrame
# result_df = calculate_last_7_days(df)
