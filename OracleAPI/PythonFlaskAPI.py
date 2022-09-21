from OracleConnection import *
import flask
from flask import request, jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True



@app.route('/books/viewall', methods=['GET'])
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
def deletedata(id):
    
    oracle_connection =  oracledb_connection()
    cursor = oracle_connection.cursor()

    if request.method == 'DELETE':
        delete_data = "DELETE FROM books WHERE id=:id"
        cursor = cursor.execute(delete_data, (id,))
        oracle_connection.commit()
        return "The book with id:{} has been deleted.".format(id), 200 


if __name__ == '__main__':
    app.run()

