
import os
import pandas as pd
import logging



def identSql(path):
    contenido = os.listdir(path)
    print(contenido)
    sql = []
    for fichero in contenido:
        if os.path.isfile(os.path.join(path, fichero)) and fichero.endswith('.sql'):
            sql.append(fichero)
    return sql


def sqlCommand(path):
    
    logging.info('create_command_sql')
    sql = identSql(path)
    print(sql)
    print(sql[0])
    fd = open(path+sql[0], 'r')
    sqlFile = fd.read()
    fd.close()
    # # print(sqlFile)
    sqlcomands = sqlFile.split(';')
    sqlcomands.pop()
    # print(sqlcomands)
    # print(len(sqlcomands))
 


    # print(sqlcomands)
    return sqlcomands


if __name__ =='__main__':
    
    var =sqlCommand(path='./database/')
    print(var)
 

    

    # print(contenido)

