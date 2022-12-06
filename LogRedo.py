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

log=[]
bd_inicial=[]

#lendo cada linha do arquivo se a abertura der certo

for i in fileArray:
	if(i.startswith("<")):
		log.append(i)
	else:
		bd_inicial.append(i)

numEspacos=0
for j in bd_inicial:
	if(j==''):
		numEspacos+=1	
for i in range(0,numEspacos,1):
	bd_inicial.remove('')
