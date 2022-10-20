


from urllib import request
from bs4 import BeautifulSoup  # this module helps in web scrapping.
import requests  # this module helps us to download a web page
import json
# from createDir import mkdirCat
import csv
import pandas as pd
import logging

# url = "http://www.ibm.com"


def requestData(url):

    data = requests.get(url).text
    logging.info("requestData")

    # print(data)
    soup = BeautifulSoup(data, features="html.parser")
    # print(soup.prettify)
    addLink =[]
    # # in html image is represented by the tag <img>
    for link in soup.find_all('script'):
        # 
        addLink.append(link)
        

    data = addLink[2]



    data = str(data)
 

    data = data.replace('</script>', '')
    new_string = data.replace('<script type="application/ld+json">', '')

    cad = '"@graph":'
    n =new_string.find(cad)

    data =new_string[n+len(cad):]



    nR =data[::-1].find(']')

    # print(nR)

    data =data[:len(data)-nR]
    data =data.replace('@','')
    data = json.loads(data)


    da = list(map(dict,data))
    keys =list(da[0].keys())
    key = keys[-1]
    # for i in range(len(da)):
    # 
    # 

    dap =[]
    for i in range(len(da)):
        try:
            dap.append(da[i][key])
        except:
            pass

    text=[]
    for i in dap:
        f=i.split('/')
        text.append(f[-1])

    # print(text)

    dataDict = dict(zip(text,dap))

    logging.info('SetUrl')
    dataframe = [dataDict['museos_datosabiertos.csv'],
                dataDict['biblioteca_popular.csv'], dataDict['salas_cine.csv']]




    return dataframe


if __name__== "__main__" :
    url = "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7"
    
    df =requestData(url)
    print(df)
    pass


