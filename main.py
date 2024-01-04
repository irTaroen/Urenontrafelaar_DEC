import globs
import json
import requests

from collections import defaultdict
from pprintpp import pprint
from datetime import datetime, timedelta


def getAfasData(url, connector, filters, token):
    endpoint = f"{url}{connector}{filters}"
    headers = {
        'authorization': token,
        'content-type': "application/json; charset=utf-8;"
    }
    data = requests.get(url=endpoint, headers=headers).json()['rows']

    return data


def sortAfasData(data):
    sorted_data = defaultdict(lambda: defaultdict(list))
    # data = [data[0]]
    for entry in data:
        medewerker_key = entry["Medewerker"]
        datum_key = entry['Datum'][:-10]
        sorted_data[medewerker_key][datum_key].append(entry)

    with open("data/output/reisuren_sorted.json", 'w') as jsonfile:
        json.dump(sorted_data, jsonfile, indent=4)

    return sorted_data


def checkTotalHours(data_value):
    total_hours = 0
    if len(data_value) > 1:
        for entry in data_value:
            total_hours += entry['Aantal']
    else:
        total_hours = data_value[0]['Aantal']

    if total_hours > 1.5:
        return True, total_hours
    else:
        return False, total_hours


def toeslagUrenBepalen(data_value, uren_vergoed):
    normaalUren = []
    toeslagUren = []
    urenVergoed = uren_vergoed
    for entry in data_value:
        uren = entry['Aantal']

        if urenVergoed > uren:
            urenVergoed -= uren
            normaalUren.append(entry)

        elif 0 < urenVergoed < uren:
            normaal = urenVergoed
            toeslag = uren - normaal

            entry_normal = entry.copy()
            entry_normal['Aantal'] = normaal
            newEndTime = datetime.strptime(
                entry_normal['Begintijd'], '%H:%M:%S') + timedelta(hours=normaal)
            entry_normal['Eindtijd'] = newEndTime.strftime('%H:%M:%S')
            entry_normal["Toeslag"] = "R100"
            normaalUren.append(entry_normal)

            entry_toeslag = entry.copy()
            entry_toeslag['Aantal'] = toeslag
            newStartTime = datetime.strptime(
                entry_toeslag['Eindtijd'], '%H:%M:%S') - timedelta(hours=toeslag)
            entry_toeslag['Begintijd'] = newStartTime.strftime('%H:%M:%S')
            toeslagUren.append(entry_toeslag)

            urenVergoed = 0

        elif urenVergoed == 0:
            toeslagUren.append(entry)

        else:
            pass

    # if len(normaalUren) > 0:
    #     print(f"Normale uren:")
    #     pprint(normaalUren)

    # if len(toeslagUren) > 0:
    #     print(f"Toeslag uren:")
    #     pprint(toeslagUren)


    # print("\n")
    return entry_normal, entry_toeslag


def weekdagToeslag(data_value):

    dagUren_begin = datetime.strptime("06:00:00", '%H:%M:%S').time()
    dagUren_eind = datetime.strptime("20:00:00", '%H:%M:%S').time()

    avondUren_begin = datetime.strptime("20:00:00", '%H:%M:%S').time()
    avondUren_eind = datetime.strptime("22:00:00", '%H:%M:%S').time()

    nachtUren1_begin = datetime.strptime("22:00:00", '%H:%M:%S').time()
    nachtUren1_eind = datetime.strptime("23:59:59", '%H:%M:%S').time()

    nachtUren2_begin = datetime.strptime("00:00:00", '%H:%M:%S').time()
    nachtUren2_eind = datetime.strptime("06:00:00", '%H:%M:%S').time()

    tijdsvakken = {
        "dagUren": {"begin":  dagUren_begin, "eind": dagUren_eind, "toeslag": "R100"},
        "avondUren": {"begin": avondUren_begin, "eind": avondUren_eind, "toeslag": "R125"},
        "nachtUren1": {"begin": nachtUren1_begin, "eind": nachtUren1_eind, "toeslag": "R150"},
        "nachtUren2": {"begin": nachtUren2_begin, "eind": nachtUren2_eind, "toeslag": "R150"},
    }
    newEntries = []

    for entry in data_value:
        begintijd = datetime.strptime(entry["Begintijd"], '%H:%M:%S').time()
        eindtijd = datetime.strptime(entry["Eindtijd"], '%H:%M:%S').time()

        # splitten van de uren
        for naam, tijdvak in tijdsvakken.items():
            tempdict = entry.copy()

            # check of de geschreven tijdvak in dit vak zitten
            if eindtijd < tijdvak['begin'] or begintijd > tijdvak['eind']:
                continue

            # Sorteerd begintijd
            if tijdvak['begin'] <= begintijd <= tijdvak['eind']:
                tempdict['Begintijd'] = begintijd.strftime('%H:%M:%S')
            else:
                tempdict["Begintijd"] = tijdvak['begin'].strftime('%H:%M:%S')

            # Sorteerd eindtijd
            if tijdvak['begin'] <= eindtijd <= tijdvak['eind']:
                tempdict['Eindtijd'] = eindtijd.strftime('%H:%M:%S')
            else:
                tempdict['Eindtijd'] = tijdvak['eind'].strftime('%H:%M:%S')

            # Add toeslag
            tempdict['Toeslag'] = tijdvak['toeslag']

            # Add new amount
            time1 = datetime.strptime(tempdict['Eindtijd'], '%H:%M:%S').time(
            )
            time2 = datetime.strptime(tempdict['Begintijd'], '%H:%M:%S').time()
            time_difference = (datetime.combine(
                datetime.today(), time1) - datetime.combine(datetime.today(), time2)).total_seconds()/3600
            
            tempdict['Aantal'] = round(time_difference,1)
            
            if time_difference:
                newEntries.append(tempdict) 

    return newEntries


def feestdagCheck(date, list_of_holidays):
    found = any(entry['Datum'].split('T')[0] ==
                date for entry in list_of_holidays)


    if found:
        return True
    else:
        return False


def postAfasData(new_entry, omgevingURL, connector, token):

    for entry in new_entry:
        pprint(entry)

        medewerker = entry["Medewerker"]
        datum = entry["Datum"]
        aantal = entry["Aantal"]
        toeslag = entry["Toeslag"]

        payload = {
            "PtRealization": {
                "Element": {
                    "Fields": {
                        "Id": -1,
                        "DaTi": datum,
                        "VaIt": "1",
                        "ItCd": "*****",
                        "Qu": aantal,
                        "EmId": medewerker,
                        "StId": toeslag,
                        "Ds": 'Gewerkte uren',
                        "Ap": True,
                        "U38A2B67DA8B84B94BADBBB4D00FE4764": True
                    }
                }
            }
        }
        payload_json = json.dumps(payload)

        url = f'{omgevingURL}{connector}'
        headers = {
            'authorization': token,
            'content-type': "application/json"
        }
        updateTotalAmount = requests.post(url=url, 
                                        headers=headers,
                                        data=payload_json)


def applyConditions(date, data_value, data_feestdagen):
    # for emp_key, dates in sorted_data.items():
    #     employee = emp_key

    #     for date_key, data_value in dates.items():
    # date = date_key
    dag = data_value[0]['Weekdag']
    uren_meer_dan_anderhalf, totallHours = checkTotalHours(data_value)

    if not uren_meer_dan_anderhalf:

        for entry in data_value:
                entry["Toeslag"] = "R100"

        print("Valt onder eerste 1.5 uur:")
        # postAfasData(data_list=data_value)
        return data_value

    if uren_meer_dan_anderhalf:
        normaalUren, toeslagUren = toeslagUrenBepalen(data_value=data_value,
                                                        uren_vergoed=1.5)
        print("Valt onder eerste 1.5 uur:")
        
        isFeestdag = feestdagCheck(date, data_feestdagen)
        if isFeestdag:
            print(f"{date} is een feestdag")

            toeslagUren["Toeslag"] = "R250"

            # postAfasData(data_list=toeslagUren)
            return[normaalUren,toeslagUren]

        else:
            isWeekdag = dag in [1, 2, 3, 4, 5]
            if isWeekdag:
                print(f"{date} is een doordeweekse dag")
                toeslag_berekend = weekdagToeslag(data_value=toeslagUren)

                # postAfasData(data_list=toeslag_berekend)
                return[normaalUren,toeslag_berekend]


            isZaterdag = dag in [6]
            if isZaterdag:
                print(f"{date} is een zaterdag")

                for entry in toeslagUren:
                    entry["Toeslag"] = "R150"

                # postAfasData(data_list=toeslagUren)
                return[normaalUren,toeslagUren]
                
            isZondag = dag in [7]
            if isZondag:
                print(f"{date} is een zondag")

                for entry in toeslagUren:
                    entry["Toeslag"] = "R200"

                # postAfasData(data_list=toeslagUren)  
                return[normaalUren,toeslagUren]


if __name__ == "__main__":
    omgeving = globs.LIVE
    connectoren = globs.CONNECTOREN

    token = omgeving['token']
    url = omgeving['url']
    connector_Reisuren = connectoren['connector_Reisuren']
    connector_Feestdagen = connectoren['connector_Feestdagen']
    updateConnector_PtRealization = connectoren["updateConnector_PtRealization"]

    data_feestdagen = getAfasData(url=url,
                                  connector=connector_Feestdagen,
                                  filters="/?skip=-1take=-1",
                                  token=token)
    # pprint(data_feestdagen)

    # data_reisuren = getAfasData(url=url,
    #                             connector=connector_Reisuren,
    #                             filters="/?skip=-1take=-1",
    #                             token=token)
    # with open("data/output/reisuren.json", 'w') as jsonfile:
    #     json.dump(data_reisuren, jsonfile, indent=4)

    with open("data/output/reisuren.json", 'r') as jsonfile:
        data_reisuren = json.load(jsonfile)

    sorted_data = sortAfasData(data_reisuren)

    for emp_key, dates in sorted_data.items():
        employee = emp_key

        for date_key, data_value in dates.items():
            date = date_key

            toeslag_berekend = applyConditions(date=date,
                                                data_value=data_value, 
                                                data_feestdagen=data_feestdagen)
            
            for entry in toeslag_berekend:
                pprint(entry)
