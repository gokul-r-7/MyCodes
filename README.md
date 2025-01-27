import pandas as pd

# Create the initial DataFrame
data = {
    'operating_system_type': ['Apple iOS', 'Apple iOS', 'Apple iOS', 'Apple iOS', 'Apple iOS',
                              'Google Android', 'Google Android', 'Google Android', 'Google Android',
                              'Google Android', 'Both', 'Both', 'Both', 'Both', 'Both'],
    'display_names': ['Devices Page Visits', 'Started Troubleshooting (Solution Center)', 
                      'Saw a Result', '1st common reason (Unknown)', '2nd common reason (Disconnected)',
                      'Devices Page Visits', 'Started Troubleshooting (Solution Center)', 
                      'Saw a Result', '1st common reason (Unknown)', '2nd common reason (Disconnected)',
                      'Devices Page Visits', 'Started Troubleshooting (Solution Center)', 
                      'Saw a Result', '1st common reason (Unknown)', '2nd common reason (Disconnected)'],
    'parent_id': [None, 183, 184, 185, 185, None, 417, 418, 419, 419, None, None, None, None, None]
}

df = pd.DataFrame(data)

# Convert parent_id to string to avoid ValueError
df['parent_id'] = df['parent_id'].astype(str)

# Extract parent_ids for Apple iOS
apple_ios_parent_ids = df[df['operating_system_type'] == 'Apple iOS'].set_index('display_names')['parent_id']

# Update Both rows
for index, row in df[df['operating_system_type'] == 'Both'].iterrows():
    display_name = row['display_names']
    if display_name in apple_ios_parent_ids.index:
        df.at[index, 'parent_id'] = f'Both_parent_id_{apple_ios_parent_ids[display_name]}'

# Display the updated DataFrame
print(df)
