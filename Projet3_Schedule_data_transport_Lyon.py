import schedule
import time
from datetime import datetime, timedelta
import pandas as pd
import requests, json
from requests.auth import HTTPBasicAuth

#création d'un DF vide
column_names = ['idtarretdestination',
     'coursetheorique',
     'direction',
     'ligne',
     'delaipassage',
     'heurepassage',
     'gid',
     'last_update_fme',
     'type',
     'id','Time_download_DDHHMMSS']
df = pd.DataFrame(columns = column_names)
df

#Plannification du début et de la fin de la récupération des données # code laetitia
#start=datetime(2021,7,22,11,55,00)+timedelta(minutes=1)
debut=datetime(2021,7,27,11,59)
fin=debut+ timedelta(hours=7)
#fin=debut+ timedelta(minutes=5)
#fin=datetime(2021,7,23,13,1)

#Création d'une définition retrieve_data pour récupéré automatiquement les données
def retrieve_data():
  global df  
  username = 'laetitia.virlouvet@gmail.com'
  password = 'qaw-EFV7gux1bnw3brq'
  link = 'https://download.data.grandlyon.com/ws/rdata/tcl_sytral.tclpassagearret/all.json?maxfeatures=-1&start=1'
  r = requests.get(link, auth=HTTPBasicAuth(username, password))

  data=r.json()
  passage=pd.json_normalize(data, ["values"])

  passage['Time_download_DDHHMMSS']=datetime.now().strftime("%d_%H_%M_%S")
  
  #passage.to_csv("passagearretMB.csv", index = False) # pour récupérer les fichiers intermédiaires
   
  df=pd.concat([df,passage])
  df.to_csv("Lyon_passagearret_LV_mar.csv", index = False)

  return df

#Creation d'une définition retrieve_alerte pour récupérer les alertes trafic automatiquement 
column_names1=['cause', 'debut', 'ligne_com', 'mode', 'titre', 'ligne_cli', 'message',
       'type', 'last_update_fme', 'fin','Time_download_DDHHMMSS']
df1=pd.DataFrame(columns= column_names1)
df1

def retrieve_alerte():
  global df1  
  username = 'laetitia.virlouvet@gmail.com'
  password = 'qaw-EFV7gux1bnw3brq'
  link1 = 'https://download.data.grandlyon.com/ws/rdata/tcl_sytral.tclalertetrafic_2/all.json?maxfeatures=-1&start=1'
  r = requests.get(link1, auth=HTTPBasicAuth(username, password))

  data=r.json()
  alerte_trafic =pd.json_normalize(data, ["values"])

  alerte_trafic['Time_download_DDHHMMSS']=datetime.now().strftime("%d_%H_%M_%S")
  
  #passage.to_csv("passagearretMB.csv", index = False) # pour récupérer les fichiers intermédiaires
   
  df1=pd.concat([df1,alerte_trafic])
  df1.to_csv("Lyon_alertetrafic_LV_mar.csv", index = False)

  return df1


#schedule.every(1).minutes.do(retrieve_data)

#while datetime.now() < fin : # permettra de choisir en amont quand stoper la récolte de donnée
#    schedule.run_pending()

#df.to_csv("df_passage_arret_mb.csv", index = False)


#automatisation shedule (1)
while datetime.now() < debut:
    test="oups"
    time.sleep(59)
else:
    schedule.every(1).minutes.do(retrieve_data)
    schedule.every(5).minutes.do(retrieve_alerte)
    while datetime.now() < fin :
        #schedule.run_pending()
        schedule.run_all()
    df.to_csv("Lyon_passagearret_LV_mar_fin.csv",index=False)
    df1.to_csv("Lyon_alertetrafic_LV_mar_fin.csv", index= False)