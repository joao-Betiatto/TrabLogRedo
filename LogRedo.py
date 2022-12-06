import psycopg2 

def conecta_bd():                 #criando conexão com o banco
    connect = psycopg2.connect(

        host='localhost', 
        database='TrabLog',
        user='postgres', 
        password='Joaointer23')

    return connect

#teste abrindo arquivo
fileName='entrada.txt'
try:
    file=open(fileName, "r", encoding="utf-8")
except:
    print('Não foi possível abrir o arquivo')
    exit(0)

fileArray=file.read().splitlines()
