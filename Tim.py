import pandas as pd
import requests
import config
import json
import numpy as np
import glob
import os
from datetime import datetime, timedelta

class ClaUnwrapper():

    def run_all(self):
        self.extract_hours()
        # self.import_weekdagen()
        # self.regels_afsluiten()
        # self.delete_ftr_files()

    def extract_hours(self):
        # get token and environment from config file
        token = config.token
        environment = config.environment

        # build get url based on the above parameters
        get_url = 'https://{}.rest.afas.online/ProfitRestServices/connectors/Ontrafelaar_aBooth_reisuren_weekdagen?skip=-1'.format(environment)
        headers = {
            'authorization': token,
            'content-type': "application/json; charset=utf-8;",
            'cache-control': "no-cache"
        }

        # start the get request and put them into a dataframe. Save to feather to use in other def
        r = requests.get(get_url, timeout=(300, 300), headers=headers)
        info = json.loads(r.text)
        df = pd.json_normalize(info['rows'])
        df.to_csv('df_reis_week.csv')

        # Sorteer het dataframe op Medewerker, Datum en Begintijd
        df = df.sort_values(by=['Medewerker', 'Datum', 'Begintijd'])

        # Converteer Datum, Begintijd en Eindtijd naar datetime-objecten
        df['Datum'] = pd.to_datetime(df['Datum'])
        df['Begintijd'] = pd.to_datetime(df['Begintijd'], format='%H:%M:%S').dt.time
        df['Eindtijd'] = pd.to_datetime(df['Eindtijd'], format='%H:%M:%S').dt.time

        # Sorteer het dataframe op Medewerker, Datum en Begintijd
        df = df.sort_values(by=['Medewerker', 'Datum', 'Begintijd'])

        df['Urensoort_resultaat'] = 'temp'

        # Iterate through each row and print
        for index, row in df.iterrows():
            zondag = 7
            anderhalf_uur = 
            if row['Weekdag'] is zondag:
                print(
                    f"Index: {index}, Medewerker: {row['Medewerker']}, Weekdag: {row['Weekdag']}, Datum: {row['Datum']}, Aantal: {row['Aantal']}, Urensoort: {row['Urensoort']}, Begintijd: {row['Begintijd']}, Eindtijd: {row['Eindtijd']}")
            
        # # Functie om de reistijd en het type reisuren te berekenen
        # def bereken_reistijd(row):
        #     start_reistijd = datetime.strptime('06:00:00', '%H:%M:%S').time()
        #     einde_reistijd_100 = datetime.combine(datetime.today(), start_reistijd) + timedelta(hours=1, minutes=30)
        #     einde_reistijd_125 = datetime.strptime('22:00:00', '%H:%M:%S').time()
        #
        #     if row['Begintijd'] < start_reistijd:
        #         if row['Eindtijd'] <= einde_reistijd_100.time():
        #             return row['Aantal'], '100%'
        #         elif row['Eindtijd'] <= einde_reistijd_125:
        #             return einde_reistijd_100.time(), '100%', (
        #                         datetime.combine(datetime.today(), row['Eindtijd']) - einde_reistijd_100).time(), '125%'
        #         else:
        #             return einde_reistijd_100.time(), '100%', (datetime.combine(datetime.today(),
        #                                                                         einde_reistijd_125) - einde_reistijd_100).time(), '125%', (
        #                                datetime.combine(datetime.today(), row['Eindtijd']) - datetime.combine(
        #                            datetime.today(), einde_reistijd_125)).time(), '150%'
        #     else:
        #         if row['Begintijd'] <= einde_reistijd_100.time():
        #             if row['Eindtijd'] <= einde_reistijd_125:
        #                 return row['Aantal'], '100%', (
        #                             datetime.combine(datetime.today(), row['Eindtijd']) - datetime.combine(
        #                         datetime.today(), row['Begintijd'])).time(), '125%'
        #             else:
        #                 return row['Aantal'], '100%', (
        #                             datetime.combine(datetime.today(), einde_reistijd_125) - datetime.combine(
        #                         datetime.today(), row['Begintijd'])).time(), '125%', (
        #                                    datetime.combine(datetime.today(), row['Eindtijd']) - datetime.combine(
        #                                datetime.today(), einde_reistijd_125)).time(), '150%'
        #         elif row['Begintijd'] <= einde_reistijd_125.time():
        #             return (datetime.combine(datetime.today(), einde_reistijd_100) - datetime.combine(datetime.today(),
        #                                                                                               row[
        #                                                                                                   'Begintijd'])).time(), '100%', (
        #                                datetime.combine(datetime.today(),
        #                                                 row['Eindtijd']) - einde_reistijd_100).time(), '125%'
        #         else:
        #             return (datetime.combine(datetime.today(), einde_reistijd_100) - datetime.combine(datetime.today(),
        #                                                                                               row[
        #                                                                                                   'Begintijd'])).time(), '100%', (
        #                                datetime.combine(datetime.today(),
        #                                                 einde_reistijd_125) - einde_reistijd_100).time(), '125%', (
        #                                datetime.combine(datetime.today(), row['Eindtijd']) - datetime.combine(
        #                            datetime.today(), einde_reistijd_125)).time(), '150%'
        #
        # # Breid het dataframe uit met de nieuwe kolommen
        # df[['Reistijd_100', 'Type_100%', 'Reistijd_125', 'Type_125%', 'Reistijd_150', 'Type_150%']] = df.apply(
        #     bereken_reistijd, axis=1, result_type='expand')
        #
        # # Maak een nieuw dataframe met de gewenste resultaten
        # resultaten = pd.melt(df, id_vars=df.columns[:7],
        #                      value_vars=['Reistijd_100', 'Reistijd_125', 'Reistijd_150', 'Type_100%', 'Type_125%',
        #                                  'Type_150%'], var_name='type_reisuren', value_name='aantal_nieuw')
        #
        # # Sorteer het resultaat op Medewerker, Datum en Begintijd
        # resultaten = resultaten.sort_values(by=['Medewerker', 'Datum', 'Begintijd'])

        # Print het resultaat

        df.to_csv('result.csv')

    def import_weekdagen(self):
        df_hours = pd.read_feather('df_hours.ftr')

        for index, row in df_hours.iterrows():
            payload = {
                "PtRealization": {
                    "Element": {
                        "Fields": {
                            "Id": -1,
                            "DaTi": row['Datum'],
                            "VaIt": "1",
                            "ItCd": "*****",
                            "Qu": row['Aantal'],
                            "EmId": row['Medewerker'],
                            "StId": row['Urensoort_import'],
                            "Ds": 'Gewerkte uren',
                            "Ap": True,
                            "U38A2B67DA8B84B94BADBBB4D00FE4764": True
                        }
                    }
                }
            }
            payload = json.dumps(payload)
            print(payload)
            token = config.token
            environment = config.environment
            update_url = 'https://{}.afas.online/ProfitRestServices/connectors/PtRealization'.format(environment)
            headers = {
                'authorization': token,
                'content-type': "application/json; charset=utf-8;",
                'cache-control': "no-cache"
            }
            result = requests.request("POST", update_url, data=payload, headers=headers)
            print(result.content)

    def regels_afsluiten(self):
        df_regels_afsluiten = pd.read_feather('df_hours.ftr')

        for index, row in df_regels_afsluiten.iterrows():
            payload = {
                      "PtRealization": {
                        "Element": {
                          "Fields": {
                            "Id": row['Regelnummer'],
                            "DaTi": row['Datum'],
                            # "ItCd": "*****",
                            "U38A2B67DA8B84B94BADBBB4D00FE4764": True
                          }
                        }
                      }
                    }
            payload = json.dumps(payload)
            print(payload)
            token = config.token
            environment = config.environment
            update_url = 'https://{}.afas.online/ProfitRestServices/connectors/PtRealization'.format(environment)
            headers = {
                'authorization': token,
                'content-type': "application/json; charset=utf-8;",
                'cache-control': "no-cache"
            }
            result = requests.request("PUT", update_url, data=payload, headers=headers)
            print(result.content)

    def delete_ftr_files(self):
        # alle gebruikte files worden verwijderd
        for file in glob.glob("*.ftr"):
            os.remove(file)

ClaUnwrapper().run_all()