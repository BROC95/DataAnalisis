

import logging

from sqlCommand import sqlCommand
from data import DataSql


def main():
    logging.basicConfig(filename='myapp.log',
                        format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
    logging.info('Started')
    url = "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7"
    commandSql = sqlCommand(path='./database/')

    # print(commandSql)

    DataSql(url, commandSql)
    # logging.debug('This message should go to the log file')
    logging.info('Finish')
    # logging.warning('And this, too')
    # logging.error('And non-ASCII stuff, too, like Øresund and Malmö')



if __name__ == '__main__':
    main()
    