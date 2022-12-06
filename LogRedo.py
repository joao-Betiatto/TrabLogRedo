import psycopg2


def execQuery(connect, sql):
    cur = connect.cursor()
    cur.execute(sql)
    connect.commit()


def conecta_bd():  # criando conexão com o banco
    connect = psycopg2.connect(

        host='localhost',
        database='trablog',
        user='postgres',
        password='Joaointer23')

    return connect


# teste abrindo arquivo
fileName = 'entrada.txt'
try:
    file = open(fileName, "r", encoding="utf-8")
except:
    print('Não foi possível abrir o arquivo')
    exit(0)

fileArray = file.read().splitlines()

log = []
bd_inicial = []

# lendo cada linha do arquivo se a abertura der certo

for i in fileArray:
    if (i.startswith("<")):
        log.append(i)
    else:
        bd_inicial.append(i)

numEspacos = 0
for j in bd_inicial:
    if (j == ''):
        numEspacos += 1
for i in range(0, numEspacos, 1):
    bd_inicial.remove('')

connect = conecta_bd()

bd_vetor = []
for line in bd_inicial:
    splitedLine = line.split('=')
    for i in range(0, len(splitedLine), 1):
        splitedLine[i] = splitedLine[i].strip()
        if ',' in splitedLine[i]:
            splitedLine[i] = splitedLine[i].split(',')
    splitedLine.append('Nao inserido')

    bd_vetor.append(splitedLine)

sql = 'DROP TABLE IF EXISTS log_table'
execQuery(connect, sql)  # se a tabela já existir, será excluída.

column = []
sqlColumns = ''

for item in range(0, len(bd_vetor), 1):
    if bd_vetor[item][0][0] not in column:
        sqlColumns = sqlColumns+','+bd_vetor[item][0][0]+' INT'
        column.append(bd_vetor[item][0][0])

sql = 'CREATE TABLE log_table (id INT'+sqlColumns+')'
execQuery(connect, sql)  # Aqui foi criada a tabela

zerosNum = ''
for i in range(0, len(column), 1):
    zerosNum = zerosNum+',0'

for item in range(0, len(bd_vetor), 1):
    if bd_vetor[item][2] == 'Nao inserido':
        sql = 'INSERT INTO log_table VALUES (' + \
            bd_vetor[item][0][1]+zerosNum+')'
        execQuery(connect, sql)
        for itemTemp in range(0, len(bd_vetor), 1):
            if bd_vetor[itemTemp][0][1] == bd_vetor[item][0][1]:
                bd_vetor[itemTemp][2] = 'Inserido'

for item in range(0, len(bd_vetor), 1):
    sql = 'UPDATE log_table SET id = ' + \
        bd_vetor[item][0][1]+', '+bd_vetor[item][0][0]+' = ' + \
        bd_vetor[item][1]+' WHERE id ='+bd_vetor[item][0][1]
    execQuery(connect, sql)

# Começa checkpoints
commitedTransactions = {}
checkpointStart = 0  # início
checkpointFuncional = False

for line in range(len(log)-1, -1, -1):
    if 'CKPT' in log[line] and 'Start' in log[line]:
        checkpointStart = line
        for lineEndCkpt in range(len(log)-1, -1, -1):
            if 'End' in log[lineEndCkpt] and lineEndCkpt > line:
                checkpointFuncional = True
                for lineCkpt in range(line, len(log)-1, 1):
                    if 'commit' in log[lineCkpt]:
                        splitedCommit = log[lineCkpt].split(' ')
                        commitedTransactions[splitedCommit[1]
                                             [:-1]] = 'Nao visitado'
                break

lastStartLine = 0
for line in range(len(log)-1, -1, -1):
    allStarts = True
    if 'unvisited' in commitedTransactions.values():  # caso nao encontrar todos os starts
        allStarts = False
    if allStarts == True:
        break
    if 'start' in log[line] and 'CKPT' not in log[line]:
        splitedStart = log[line].split(' ')
        transaction = splitedStart[1][:-1]
        if transaction in commitedTransactions.keys():
            commitedTransactions[transaction] = 'visited'

    lastStartLine = line

transactionInependent = []  # transacao commitada independente de checkpoint
for line in range(0, len(log)-1, 1):
    if 'commit' in log[line]:
        splitedCommit = log[line].split(' ')
        transactionInependent.append(splitedCommit[1][:-1])

# verificar se fez REDO
if checkpointFuncional == True:
    print('Saida')
    for i in commitedTransactions.keys():
        print('* A transacao', i, 'realizou REDO *')
    for line in range(lastStartLine, len(log)-1, 1):
        noMoreOrlessLine = log[line][1:-1]
        splitedLine = noMoreOrlessLine.split(',')
        if len(splitedLine) == 4:
            if splitedLine[0] in commitedTransactions.keys():
                sql = 'UPDATE log_table SET ' + \
                    splitedLine[2] + '='+splitedLine[3] + \
                    ' WHERE id ='+splitedLine[1]
                execQuery(connect, sql)
else:
    print('Saida')
    for i in transactionInependent:
        print('* A transacao ', i, ' realizou REDO *')
    for line in range(0, len(log)-1, 1):
        noMoreOrlessLine = log[line][1:-1]
        splitedLine = noMoreOrlessLine.split(',')
        if len(splitedLine) == 4:
            if splitedLine[0] in transactionInependent:
                sql = 'UPDATE log_table SET ' + \
                    splitedLine[2] + '='+splitedLine[3] + \
                    ' WHERE id ='+splitedLine[1]
                execQuery(connect, sql)

# pegar as informacoes e mostrar o estado final do banco
cursor = connect.cursor()
query = "select * from log_table"
cursor.execute(query)
logTestrecords = cursor.fetchall()
print('\nEstado final:')
print('    ', end="")
for i in column:
    print(i+'    ', end="")
print('')

for row in logTestrecords:
    for i in row:
        print(i, '  ', end="")
    print('')

connect.close()
exit(0)
