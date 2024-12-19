df_pivot['New Visitors %'] = (pd.to_numeric(df_pivot['New Visitors']) / pd.to_numeric(df_pivot['Visits']) * 100).round(3)	
df_pivot['Server Errors %'] = (pd.to_numeric(df_pivot['Server Errors']) / pd.to_numeric(df_pivot['Visits']) * 100).round(3)	
df_pivot['Visits to Chat %'] = (pd.to_numeric(df_pivot['Visits to Chat']) / pd.to_numeric(df_pivot['Visits']) * 100).round(3)
