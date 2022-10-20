
from sqlalchemy import create_engine

# default
import logging
import os
from decouple import config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print(BASE_DIR)
SECRET_KEY = config('SECRET_KEY')
DEBUG = config('DEBUG', cast=bool)
DATABASES = {
     
        'ENGINE':config('DB_ENGINE'),
        'NAME':config('DB_NAME'),
        'USER':config('DB_USER'),
        'PASSWORD':config('DB_PASSWORD'),
        'HOST':config('DB_HOST'),
        'PORT':config('DB_PORT'),
    }
# print(DATABASES['ENGINE'])


def connectionBD():

    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.info("Init connect")
    engine = DATABASES["ENGINE"]
    user = DATABASES["USER"]
    pasdb = DATABASES["PASSWORD"]
    host_db = DATABASES["HOST"]
    port_db= DATABASES['PORT']
    name_db = DATABASES["NAME"]
    # url_db = f'{engine}://{user}:{pasdb}@{host_db}:{port_db}/{name_db}'
    url_db = f'{engine}://{user}:{pasdb}@{host_db}/{name_db}'
    try:
        print(url_db)
       
        engine = create_engine(url=url_db, echo=False)
        print(engine)
        # engine.execute('SELECT version').fetchone()
        logging.info("connection success")
        return engine
    except:
        logging.warning('Not connection')
        return None
       

if __name__=='__main__':
    engine=connectionBD()
    print("RETURN")
    print(type(engine))
    print(engine.connect())
    
    if engine!= None:
        print("YES")
        pass
    
    # engine.execute('SELECT version').fetchone()

