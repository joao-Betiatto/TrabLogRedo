import psycopg2 

def conecta_bd():                 #criando conexão com o banco
    connect = psycopg2.connect(

        host='localhost', 
        database='TrabLog',
        user='postgres', 
        password='12345')

    return connect