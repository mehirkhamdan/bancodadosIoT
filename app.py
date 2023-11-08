import os
import dotenv
from pprint import pprint

from pymongo.mongo_client import MongoClient

env_file =".env"
db_name = "geobr"
dotenv.load_dotenv(env_file) 
URL = os.environ["db_url"]

def get_connection():
  client = MongoClient(URL)
  bases = list(client.list_databases())
  # print(base)
  db = client[db_name]  
  return db 
db = get_connection()
print(db.list_collection_names())


def get_municipios(_uf):
  res = db.municipios.find(
    {"Uf":_uf},{'_id':0,'Nome':1}
  )  
  return list(res)

# result = get_municipios("AC")
# print(f' total{len(result)}')
# pprint(result)
# get_municipios("AC")  

# result = get_municipios("RR")
# print(f' total{len(result)}')
# pprint(result)
# get_municipios("RR")


def get_municipios_nome(_nome):
  res = db.municipios.find(
    {"Nome":_nome},{'_id':0,'Nome':1}
  )  
  return list(res) 

result = get_municipios_nome("W")
print(f' total{len(result)}')
pprint(result)
get_municipios("W")  



  

