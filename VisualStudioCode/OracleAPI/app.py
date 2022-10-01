import os
from OracleConnection import *
from flask import Flask, jsonify, request, session, make_response
from secret_manager import username, password, apiusername, apipassword
from functools import wraps
import datetime
import jwt


app = Flask(__name__)
app.config["DEBUG"] = True
app.config['SECRET_KEY'] = os.urandom(24).hex()

def check_token(func):
    @wraps(func)
    def wrapped(*args, **kwargs):

        if 'token' in request.headers:
            token = request.headers['token']
        else:
            return jsonify({'Message' :  'Please Login to get token and Enter Token in Headers'}), 403
     

        try:
            tokendata = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except:
            return jsonify({'Invalid token' : 'Invalid Token'}), 403
        return func(*args, **kwargs)
    return wrapped

@app.route('/login')
def login():
    auth = request.authorization

    if auth.username == apiusername and auth.password == apipassword:
        token = jwt.encode({'user' : auth.username, 'exp' : datetime.datetime.utcnow() + datetime.timedelta(seconds = 60)}, app.config['SECRET_KEY'], algorithm="HS256")
        return jsonify({'token' : token})
    else:
        return jsonify({'Message' : 'Please enter corrrect username & password'}), 403


@app.route('/books/viewall', methods=['GET'])
#@auth.login_required
@check_token
def getdata():

    oracle_connection =  oracledb_connection()
    cursor = oracle_connection.cursor()
    
    if request.method == 'GET':
    
        getdata = "Select * from books"        
        cursor.execute(getdata)
        columns = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        books_data = cursor.fetchall()
        return books_data


@app.route('/books/add', methods=['POST'])
@check_token
def insertdata():

    oracle_connection =  oracledb_connection()
    cursor = oracle_connection.cursor()

    if request.method == 'POST':
        author = request.json['author']
        first_sentence = request.json['first_sentence']
        published = request.json['published']
        language = request.json['language']
        title = request.json['title']
        id = request.json['id']

        insertdata = 'INSERT INTO books (AUTHOR, FIRST_SENTENCE, PUBLISHED, LANGUAGE, TITLE, ID) VALUES (:author,:first_sentence,:published,:language,:title,:id)'
        column_names =  (author,first_sentence,published,language,title,id)
        cursor = cursor.execute(insertdata, column_names)
        oracle_connection.commit()
        return "Data inserted successfully"

@app.route('/books/update/<int:id>', methods=['PUT'])
@check_token
def updatedata(id):
    oracle_connection =  oracledb_connection()
    cursor = oracle_connection.cursor()

    if request.method == 'PUT':

        update_data = "UPDATE books SET title=:title, first_sentence=:first_sentence, published=:published, author=:author, language=:language WHERE id=:id"
        author = request.json['author']
        first_sentence = request.json['first_sentence']
        published = request.json['published']
        language = request.json['language']
        title = request.json['title']

        updated_book = {
            'id' : id,
            'author' : author,
            'first_sentence' : first_sentence,
            'published' : published,
            'language' : language,
            'title' : title
        }
        column_names = (title, first_sentence, published, author, language, id)
        cursor = cursor.execute(update_data, column_names)
        oracle_connection.commit()
        return jsonify(updated_book)

@app.route('/books/delete/<int:id>', methods=['DELETE'])
@check_token
def deletedata(id):
    
    oracle_connection =  oracledb_connection()
    cursor = oracle_connection.cursor()

    if request.method == 'DELETE':
        delete_data = "DELETE FROM books WHERE id=:id"
        cursor = cursor.execute(delete_data, (id,))
        oracle_connection.commit()
        return "The book with id:{} has been deleted.".format(id), 200 

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404

    
if __name__ == '__main__':
    app.run()

