import requests
from requests.auth import HTTPBasicAuth
from secret_manager import username, password
import json

login_url = 'http://127.0.0.1:5000/login'
viewbooks_url = 'http://127.0.0.1:5000/books/viewall'

def login_togettoken(login_url):
    global token
    response_login = requests.get(login_url, auth=HTTPBasicAuth(username,password))
    login_statuscode = response_login.status_code
    if login_statuscode == 200:
        login_data = response_login.text
        login_parsejson = json.loads(login_data)
        token = login_parsejson['token']
        return token
    else:
        return login_statuscode


def getdata_frombooks(viewbooks_url,header_data):
    response_view_books = requests.get(viewbooks_url, headers = header_data)
    view_books_statuscode = response_view_books.status_code
    if view_books_statuscode == 200:
        books_data = response_view_books.text
        books_parsejson = json.loads(books_data)
        return books_parsejson
    else:
        return view_books_statuscode
    #data = response_API.text
    #parse_json = json.loads(data)
    #print(data)



token = login_togettoken(login_url)
print(token)

header_data = {
    'token' : token
}
books_data = getdata_frombooks(viewbooks_url,header_data)
print(books_data)