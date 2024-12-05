'''
This program demonstrats Big Data.
Author: Michael Mc Laughlin
Institution: C.O.D.
Course: CIS-2532-Net01
Semester: Fall 2024.
Homework: 12
Chapter: 17 Big Data: Hadoop, Spark, NoSQL and IoT
Section: 17.2 & Exercise: 17.1
Due Date: 11/03/2024 @ 11:59 PM.
'''

'''Section: 17.2'''
'''Connecting to the Database'''
import sqlite3
connection = sqlite3.connect('books.db')

'''Viewing the authors Table's Content'''
import pandas as pd 
pd.options.display.max_columns = 10
pd.read_sql('SELECT * FROM authors', connection, index_col=['id'])

'''titles Table's Content'''
pd.read_sql('SELECT * FROM titles', connection)

'''author_ISBN Table's Content'''
df = pd.read_sql('SELECT * FROM author_ISBN', connection)
df.head()

'''SELECT Queries'''
pd.read_sql('SELECT first, last FROM authors', connection)

pd.read_sql("""SELECT title, edition, copyright 
            FROM titles
            WHERE copyright > '2016'""", connection)

pd.read_sql("""SELECT id, first, last 
                FROM authors
                WHERE last LIKE 'D%'""",
            connection, index_col=['id'])

pd.read_sql("""SELECT id, first, last 
                FROM authors
                WHERE first LIKE '_b%'""",
            connection, index_col=['id'])

'''ORDER BY  Clause'''
pd.read_sql('SELECT title FROM titles ORDER BY title ASC', connection)

pd.read_sql("""SELECT id, first, last 
                FROM authors
                ORDER BY last, first""",
            connection, index_col=['id'])

pd.read_sql("""SELECT id, first, last 
                FROM authors
                ORDER BY last DESC, first ASC""",
            connection, index_col=['id'])

pd.read_sql("""SELECT isbn, title, edition, copyright 
            FROM titles
            WHERE title LIKE '%How to Program'
            ORDER BY title""", connection)

'''Merging Data from Multiple Tables: INNER JOIN'''
pd.read_sql("""SELECT first, last, isbn 
                FROM authors
                INNER JOIN author_ISBN
                    ON authors.id = author_ISBN.id
                ORDER BY last, first""", connection).head()

cursor = connection.cursor()

'''INSERT INTO Statement'''
cursor = cursor.execute("""INSERT INTO authors (first, last)
                        VALUES ('Sue', 'Red')""")

pd.read_sql('SELECT id, first, last FROM authors',
             connection, index_col=['id'])

'''UPDATE Statement'''
cursor = cursor.execute("""UPDATE authors SET last = 'Black'
                        WHERE id = 6""")

cursor.rowcount

pd.read_sql('SELECT id, first, last FROM authors',
             connection, index_col=['id'])

'''DELETE FROM Statement'''
cursor = cursor.execute('DELETE FROM authors WHERE id = 6')

cursor.rowcount

pd.read_sql('SELECT id, first, last FROM authors',
             connection, index_col=['id'])


'''Exercise: 17.1'''

'''a'''
pd.read_sql('''SELECT last FROM authors
            ORDER BY last DESC''', connection)

'''b'''
pd.read_sql('''SELECT title FROM titles
            ORDER BY title ASC''', connection)

'''c'''
pd.read_sql("""SELECT T.title, T.copyright, AI.isbn 
                FROM authors A
                INNER JOIN author_ISBN AI
                    ON A.id = AI.id
                INNER JOIN titles T
                    ON T.isbn = AI.isbn
                WHERE A.id = 1
                ORDER BY title""", 
                connection)

'''d'''
cursor = cursor.execute("""INSERT INTO authors (first, last)
                        VALUES ('Gilbert', 'Held')""")

'''e'''
cursor = cursor.execute("""INSERT INTO author_ISBN (id, isbn)
                        VALUES (6, 0672309343)""")

cursor = cursor.execute("""INSERT INTO titles (isbn, title, edition, copyright)
                        VALUES (0672309343, 'Understanding Data Communications', 5, 1996)""")

'''Closing the Database'''
connection.close()