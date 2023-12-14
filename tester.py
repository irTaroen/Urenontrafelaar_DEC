ru = [1, 1, 2, 2, 2]
totaal = 3.5
normaalUren = []
toeslagUren = []

for entree in ru:
    if totaal > entree:
        normaalUren.append(entree)
        totaal -= entree

    elif 0 < totaal < entree:
        toeslag = entree - totaal
        normaal = entree - toeslag
        normaalUren.append(normaal)
        toeslagUren.append(toeslag)
        totaal = 0

    elif totaal == 0:
        toeslagUren.append(entree)

    else:
        print("What now?")

if len(normaalUren) > 0:
    print(f"Dit zijn de normale entrees {normaalUren}")

if len(toeslagUren) > 0:
    print(f"Dit zijn de overige entrees {toeslagUren}")
