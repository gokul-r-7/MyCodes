import pandas as pd

df = pd.read_csv('Customer.csv')

print(df) 

df.to_csv(r'E:\VisualStudioWorkspace\PersonalWorkspace\Python\DataFrametoTextfile\Textfile.txt', header = True, index=None, sep='\t', mode='a')
