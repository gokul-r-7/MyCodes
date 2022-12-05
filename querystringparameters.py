queryStringParameters = {
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
#queryStringParameters = None

if queryStringParameters == None:
    select_query = 'select * from tablename'
    print(select_query)
else:
    arr = []
    query = 'select * from table where '
    for (k, v) in queryStringParameters.items():
        arr.append(k + ' = '+  v + ' and ')
    newquery = ''
    for i in arr:
        newquery += i
    newquery=  newquery[:-4] 
    select_query = query + newquery 
    print(select_query)
