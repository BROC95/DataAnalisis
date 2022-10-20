
from asyncio.log import logger
import requests
import io
import pandas as pd
from createDir import mkdirCat


from consulta import requestData
from sqlCommand import sqlCommand
from pathlib import Path

from sqlalchemy import create_engine
import logging

from connection import connectionBD


def getResponse(url):
    try:
        openUrl = requests.get(url)  # Query
        nurl = openUrl.url.split('/')
        name = nurl[-1]
        # print(name)
        logging.info("data: %s", name)
        if(openUrl.status_code == 200):
            # data info
            # print(openUrl.status_code)
            logging.info("request:   %s ", openUrl.status_code)
            decoded_content = openUrl.content.decode('utf-8')

            # Read data  pandas
            df = pd.read_csv(io.StringIO(decoded_content))
            if isinstance(df, pd.DataFrame):
                return df
            else:
                # print("No es data")
                logging.warning("Not is data")
                return None
    except:
        # print("Error receiving data URL")
        logging.warning("Error receiving data URL")


def vercaol(datacol, da):

    # print("Yes esta ")
    for i in datacol:
        if i in list(da):
            # print(i)
            pass
    # print("No esta")
    cont = 0
    for i in datacol:

        if i in list(da):
            pass
        else:
            # df2.rename(columns={df2: i}, inplace=True)
            # print(i, '!=', da[cont])
            pass
            # print(i)
        cont += 1


def identCol(df, datacol):
    # print(df)
    Ncol = []
    # print("Yes")
    for i in datacol:
        if i in df:
            # print(i)
            pass
    # print("No")
    for i in datacol:
        if i in df:
            pass
        else:
            Ncol.append(i)
            # print(i)
    # print("*"*4)
    # for i in df:
    #     if not i in datacol:
    #         print(i)

    return Ncol


def IdentColR(refCol, Col):
    Col = list(Col)
    n = 0
    datN = []
    for i in refCol:
        if i in Col:
            # print(i)
            n += 1
        else:
            datN.append(i)

    # print(len(refCol), n)
    #    # print(datN)


def DataSql(url, sqlcommands):
    df = requestData(url)
    logging.info('Data_start')

    df1, df2, df3 = getResponse(df[0]), getResponse(df[1]), getResponse(df[2])

    datacol = "cod_localidad,id_provincia,id_departamento,categoria,provincia,localidad,nombre,domicilio,cp,telefono,mail,web"

    datacol = datacol.split(',')

    dfc1 = list(df1.columns)
    dfc2 = list(df2.columns)
    dfc3 = list(df3.columns)
    dfc1 = list(map(lambda col: col.lower(), dfc1))
    dfc2 = list(map(lambda col: col.lower(), dfc2))
    dfc3 = list(map(lambda col: col.lower(), dfc3))

    df1.columns = dfc1
    df2.columns = dfc2
    df3.columns = dfc3

    Ncol1 = identCol(dfc1, datacol)
    Ncol2 = identCol(dfc2, datacol)
    Ncol3 = identCol(dfc3, datacol)

    if len(Ncol1) != 0:
        df1.rename(columns={'cod_loc': Ncol1[0], 'idprovincia': Ncol1[1],
                            'iddepartamento': Ncol1[2], 'direccion': Ncol1[3]}, inplace=True)
    if len(Ncol2) != 0:
        df2.rename(columns={'cod_loc': Ncol2[0], 'idprovincia': Ncol2[1],
                            'iddepartamento': Ncol2[2], 'categoría': Ncol2[3], 'teléfono': Ncol2[4]}, inplace=True)
    if len(Ncol3) != 0:
        df3.rename(columns={'direccion': Ncol3[0]}, inplace=True)

    df3 = df3.assign(telefono=None)
    df3 = df3.assign(mail=None)
    # print(IdentColR(datacol, df3.columns))
    # Al actualizar colummnas se procede a guardar.

    # Directorios
    logging.info('create_dir')
    paths = mkdirCat(catego=["museos", "librerias", "cines"])

    df1.to_csv(paths[0]+'/museos.csv')
    df1.to_csv(paths[1]+'/librerias.csv')
    df1.to_csv(paths[2]+'/cines.csv')

    tab_df1 = df1[datacol]
    tab_df2 = df2[datacol]
    tab_df3 = df3[datacol]
    tab_gen = pd.concat([tab_df1, tab_df2, tab_df3])
    print("Tabla general")
    print(tab_gen)
    print("-"*10)
    datacol2 = ['provincia', 'pantallas', 'butacas', 'espacio_incaa', 'fuente']

    tabla_cine = df3[datacol2]

    logging.info('create_sql')
    # Se crea motor con SQLAlquemy
    # engine = create_engine('sqlite:///InfoArg.sqlite', echo=False)
    # Se crea connection
    print("Tabla cine")
    print(tabla_cine)
    print("-"*10)
    engine = connectionBD()
    if engine != None:
        print("YES")
        logging.info("Connection ok bd")

        # Se crean las tablas con la info base
        try:
            tab_gen.to_sql('Ubi', con=engine)
        except:
            pass
        try:
            tabla_cine.to_sql('Cine', con=engine)
        except:
            pass

        #  SQL COMMADS
        # sqlcommands[0]

        # Table Ubicaciones

        # engine.execute(sqlcommands[0]).fetchall()
        # Prueba

        print("Consultas")
        for i in range(len(sqlcommands)):
            engine.execute(sqlcommands[i]).fetchall()

        # print(engine.execute(sqlcommands[1]).fetchall())
        var1 = engine.execute(sqlcommands[1]).fetchall()
        # Create Table info
        df = pd.DataFrame(var1, columns=['count',
                                         'categoria',  'provincia'])

        try:
            df.to_sql('Ubi_N', con=engine)
        except:
            pass
        #  Table Cine
        engine.execute(sqlcommands[2]).fetchall()

        var = engine.execute(sqlcommands[3]).fetchall()

        # Create Table info
        df = pd.DataFrame(var, columns=['espacio_incaa',
                                        'pantallas', 'butacas', 'provincia'])

        # Se crea tabla Nueva con los filtros sql
        try:
            df.to_sql('Cine_N', con=engine)
        except:
            pass

        engine.connect().close()
    else:
        print("No connect")
        logging.warning("Erro connection")


if __name__ == '__main__':
    url = "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7"
    commandSql = sqlCommand(path='./')
    # print(commandSql)

    DataSql(url, commandSql)
