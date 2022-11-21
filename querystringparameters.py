querystrings = {
    'name' : 'gokila',
    'place' : 'vellore',
    'work' : 'it',
    'state' : 'tn',
    'country' : 'us',
    'education' : 'be',
    'domain' : 'de',
    'marital status' : 'single',
    'age': '23'
}
arr = []
query = 'select * from table where '
for (k, v) in querystrings.items():
    arr.append(k + ' = '+  v + ' and ')
newquery = ''
for i in arr:
    newquery += i
newquery=  newquery[:-4] 
final_query = query + newquery 
print(final_query)
