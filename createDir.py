
import logging
import requests
import io
import pandas as pd
from consulta import requestData
from pathlib import Path
from datetime import date
from datetime import datetime
import locale


# # from pandasql import sqldf


def mkdirCat(catego):
    # Idioma "es-ES" (código para el español de España)
    logging.info("mkdir")
    # locale.setlocale(locale.LC_ALL, 'es-ES')
    dt = datetime.now()
    # print(dt.strftime("%A %d %B %Y %I:%M"))
    year_mounth = dt.strftime("%Y-%B")
    # print(dt.strftime("%A %d de %B del %Y - %H:%M"))  # %I 12h -
    # catego = ["museos", "cines", "librerias"]
    today = date.today()
    month = dt.month
    year = dt.year
    day = dt.day
    today = f"-{day}-{month}-{year}"
    pathday = [catego[i] + '/'+year_mounth+'/' +
               catego[i]+today for i in range(len(catego))]

    exist = list(map(lambda path: Path(path), catego))
    for i in range(len(catego)):
        # print(exist[i].exists())
        if exist[i].exists():
            list(map(lambda path: Path(path).mkdir(
                parents=True, exist_ok=True), pathday))
        else:
            list(map(lambda path: Path(path).mkdir(
                parents=True, exist_ok=True), pathday))
    return pathday


if __name__ == '__main__':

    # url = "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7"

    # df = requestData(url)
    # print(df)

    # df1, df2, df3 = getResponse(df[0]), getResponse(df[1]), getResponse(df[2])
    # print(df1.head())
    # print(df2.head())
    # print(df3.head())

    pass
