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


def toeslagUrenBepalen(data_value):
    normaalUren = []
    toeslagUren = []
    urenVergoed = 1.5
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

    if len(normaalUren) > 0:
        pprint(f"Dit zijn de normale uren {normaalUren}")

    if len(toeslagUren) > 0:
        print(f"Dit zijn de toeslag uren {toeslagUren}")

    return normaalUren, toeslagUren


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
        "dagUren": {"begin":  dagUren_begin, "eind": dagUren_eind, "toeslag": 1},
        "avondUren": {"begin": avondUren_begin, "eind": avondUren_eind, "toeslag": 1.25},
        "nachtUren1": {"begin": nachtUren1_begin, "eind": nachtUren1_eind, "toeslag": 1.5},
        "nachtUren2": {"begin": nachtUren2_begin, "eind": nachtUren2_eind, "toeslag": 1.5},
    }
    newEntries = []

    for entry in data_value:
        begintijd = datetime.strptime(entry["Begintijd"], '%H:%M:%S').time()
        eindtijd = datetime.strptime(entry["Eindtijd"], '%H:%M:%S').time()

        # splitten van de uren
        for naam, tijden in tijdsvakken.items():
            tempdict = entry.copy()

            # check of de geschreven tijden in dit vak zitten
            if eindtijd < tijden['begin'] or begintijd > tijden['eind']:
                continue

            # Sorteerd begintijd
            if tijden['begin'] <= begintijd < tijden['eind']:
                tempdict['Begintijd'] = begintijd.strftime('%H:%M:%S')
            else:
                tempdict["Begintijd"] = tijden['begin'].strftime('%H:%M:%S')

            # Sorteerd eindtijd
            if tijden['begin'] <= eindtijd < tijden['eind']:
                tempdict['Eindtijd'] = eindtijd.strftime('%H:%M:%S')
            else:
                tempdict['Eindtijd'] = tijden['eind'].strftime('%H:%M:%S')

            # Add toeslag
            tempdict['toeslag'] = tijden['toeslag']

            # Add new amount
            time1 = datetime.strptime(tempdict['Eindtijd'], '%H:%M:%S').time(
            )
            time2 = datetime.strptime(tempdict['Begintijd'], '%H:%M:%S').time()
            time_difference = (datetime.combine(
                datetime.today(), time1) - datetime.combine(datetime.today(), time2)).total_seconds()/3600
            tempdict['Aantal'] = time_difference

            # Add entry to list
            newEntries.append(tempdict)

    print()
    pprint(newEntries)


def overigeToeslag(data_value, toeslag):
    for entry in data_value:
        entry["toeslagAantal"] = toeslag*entry["Aantal"]

    return data_value


def feestdagCheck(date, list_of_holidays):
    found = any(entry['Datum'].split('T')[0] ==
                date for entry in list_of_holidays)

    if found:
        return True
    else:
        return False


def postAfasData(data_value, toeslag):
    date = data_value[0]["Datum"].split('T')[0]
    print(
        F"Uren op {date} zijn binnen de eerste 1.5 reisuren. Toeslag {toeslag}")
    pass


def applyConditions(sorted_data, data_feestdagen):
    for emp_key, date_value in sorted_data.items():
        employee = emp_key

        for date_key, data_value in date_value.items():
            date = date_key
            dag = data_value[0]['Weekdag']

            uren_meer_dan_anderhalf, totallHours = checkTotalHours(data_value)

            if not uren_meer_dan_anderhalf:
                toeslag = 1
                postAfasData(data_value=data_value,
                             toeslag=toeslag)

            if uren_meer_dan_anderhalf:
                normaalUren, toeslagUren = toeslagUrenBepalen(
                    data_value=data_value)

                isFeestdag = feestdagCheck(date, data_feestdagen)
                if isFeestdag:
                    toeslag = 2.5
                    toeslagBerekend = overigeToeslag(data_value=toeslagUren,
                                                     toeslag=toeslag)
                    pprint(toeslagBerekend)

                    continue

                isWeekdag = dag in [1, 2, 3, 4, 5]
                if isWeekdag:
                    weekdagToeslag(data_value=toeslagUren)

                isZaterdag = dag in [6]
                if isZaterdag:
                    toeslag = 1.5
                    toeslagBerekend = overigeToeslag(data_value=toeslagUren,
                                                     toeslag=toeslag)
                    pprint(toeslagBerekend)

                isZondag = dag in [7]
                if isZondag:
                    toeslag = 2
                    toeslagBerekend = overigeToeslag(data_value=toeslagUren,
                                                     toeslag=toeslag)
                    pprint(toeslagBerekend)


if __name__ == "__main__":
    omgeving = globs.LIVE
    connectoren = globs.CONNECTOREN

    token = omgeving['token']
    url = omgeving['url']
    connector_Reisuren = connectoren['connector_Reisuren']
    connector_Feestdagen = connectoren['connector_Feestdagen']

    data_feestdagen = getAfasData(url=url,
                                  connector=connector_Feestdagen,
                                  filters="/?skip=-1take=-1",
                                  token=token)

    # data_reisuren = getAfasData(url=url,
    #                             connector=connector_Reisuren,
    #                             filters="/?skip=-1take=-1",
    #                             token=token)
    # with open("data/output/reisuren.json", 'w') as jsonfile:
    #     json.dump(data_reisuren, jsonfile, indent=4)

    with open("data/output/reisuren.json", 'r') as jsonfile:
        data_reisuren = json.load(jsonfile)

    sorted_data = sortAfasData(data_reisuren)

    applyConditions(sorted_data, data_feestdagen)
