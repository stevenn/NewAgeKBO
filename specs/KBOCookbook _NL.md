# Cookbook KBO Open data

**Versie R018.00**

**Contactpersoon:** Vincent Lheureux  
Stafdienst ICT  
Elke werkdag van 9 tot 16 uur. In geval van onmogelijkheid tijdens deze uren, na afspraak.

Koning Albert II-laan 16  
1000 Brussel

T +32 (0) 2 277 94 50  
F +32 (0) 2 277 51 80

Vincent.lheureux@economie.fgov.be  
http://economie.fgov.be

---

FOD Economie, K.M.O., Middenstand en Energie

---

## Inhoud

- [Inleiding](#inleiding)
- [1. Algemene concepten](#1-algemene-concepten)
  - [1.1. Wie kan de bestanden gebruiken?](#11-wie-kan-de-bestanden-gebruiken)
  - [1.2. Waar worden de bestanden ter beschikking gesteld?](#12-waar-worden-de-bestanden-ter-beschikking-gesteld)
  - [1.3. Wanneer worden de bestanden ter beschikking gesteld?](#13-wanneer-worden-de-bestanden-ter-beschikking-gesteld)
  - [1.4. Welke gegevens bevatten de bestanden?](#14-welke-gegevens-bevatten-de-bestanden)
  - [1.5. Hoe zijn de bestanden opgebouwd?](#15-hoe-zijn-de-bestanden-opgebouwd)
    - [1.5.1. Het full bestand](#151-het-full-bestand)
    - [1.5.2. Het update bestand](#152-het-update-bestand)
- [2. Beschrijving van de bestanden](#2-beschrijving-van-de-bestanden)
  - [2.1. meta.csv](#21-metacsv)
  - [2.2. code.csv](#22-codecsv)
  - [2.3. enterprise.csv](#23-enterprisecsv)
  - [2.4. establishment.csv](#24-establishmentcsv)
  - [2.5. denomination.csv](#25-denominationcsv)
  - [2.6. address.csv](#26-addresscsv)
  - [2.7. contact.csv](#27-contactcsv)
  - [2.8. activity.csv](#28-activitycsv)
  - [2.9. branch.csv](#29-branchcsv)

---

## INLEIDING

Dit document is een technische beschrijving van de bestanden die onder de licentie KBO Open data worden aangeboden door de FOD Economie, K.M.O, Energie en Middenstand.

Deze bestanden bevatten publiek toegankelijke gegevens uit KBO (Kruispuntbank van Ondernemingen). Het volledig bestand wordt maandelijks aangepast, terwijl het updatebestand dagelijks wordt aangepast.

---

## 1. ALGEMENE CONCEPTEN

### 1.1. WIE KAN DE BESTANDEN GEBRUIKEN?

De opendatabestanden zijn voor iedereen beschikbaar die de licentie KBO Open data aanvaardt. Via de website https://kbopub.economie.fgov.be/kbo-open-data kan u een aanvraag opstarten om toegang te verkrijgen tot de bestanden KBO Open data.

### 1.2. WAAR WORDEN DE BESTANDEN TER BESCHIKKING GESTELD?

De bestanden KBO Open data kunnen gedownload worden via de website https://kbopub.economie.fgov.be/kbo-open-data of via SFTP-server. Hiervoor moet u zich op deze website registreren.

### 1.3. WANNEER WORDEN DE BESTANDEN TER BESCHIKKING GESTELD?

Elke eerste zondag van de maand worden er nieuwe bestanden aangemaakt op basis van een snapshot van de KBO-databank genomen op de voorafgaande vrijdag (om middernacht). Deze bestanden worden dan op deze eerste zondag van de maand, of de maandag die erop volgt, ter beschikking gesteld op de website. Elke maand wordt het extractnummer met 1 verhoogd. Enkel de laatste 4 extracten worden bewaard op de server. Indien er zich technische problemen voordoen, dan kan van dit schema worden afgeweken.

### 1.4. WELKE GEGEVENS BEVATTEN DE BESTANDEN?

De bestanden KBO Open data bevatten actieve gegevens over actieve geregistreerde entiteiten en vestigingseenheden zoals deze zijn ingeschreven in KBO op de snapshot datum.

De bestanden bevatten geen historiek. Indien bijvoorbeeld het adres van een actieve entiteit wijzigt, dan zal het bestand enkel het nieuwe adres bevatten en niet het oude adres.

In hoofdstuk 2 worden alle variabelen beschreven die voorkomen in de KBO Open data bestanden.

### 1.5. HOE ZIJN DE BESTANDEN OPGEBOUWD?

Er zijn 2 soorten bestanden:

- Een volledig bestand – dit bevat alle in hoofdstuk 2 opgesomde gegevens van alle actieve entiteiten en hun actieve vestigingseenheden opgenomen in KBO Open data (verder "full" bestand genoemd)
- Een update-bestand met de mutaties tussen het laatste en het voorlaatste full bestand.

De eerste keer dat u de gegevens oplaadt, gebruikt u uiteraard het full bestand. Om uw databank up-to-date te houden kan u nadien zelf kiezen of u maandelijks telkens opnieuw het full bestand oplaadt of u enkel uw databank bijwerkt met de wijzigingen van het update bestand.

De bestanden volgen deze naamgeving:
- full-bestand: KboOpenData_<extractnr>_<jaar>_<maand>_Full.zip
- update-bestand: KboOpenData_<extractnr>_<jaar>_<maand>_Update.zip

#### 1.5.1. HET FULL BESTAND

De gegevens in het full bestand worden geleverd onder de vorm van een ZIP bestand met daarin een aantal CSV bestanden:

- **meta.csv**: bevat enkele metagegevens over dit full bestand (versienummer, tijdstip van aanmaak, ...).
- **code.csv**: bevat de beschrijvingen van de codes die gebruikt worden in de andere bestanden.
- **contact.csv**: bevat contactgegevens van entiteiten en vestigingseenheden.
- **enterprise.csv**: bevat 1 lijn per entiteit met enkele basisgegevens.
- **establishment.csv**: bevat 1 lijn per vestigingseenheid met enkele basisgegevens.
- **activity.csv**: bevat 1 lijn per activiteit van een entiteit of vestigingseenheid. Een entiteit of vestigingseenheid kan meerdere activiteiten uitoefenen.
- **address.csv**: bevat 0, 1 of 2 lijnen per adres van een entiteit of vestigingseenheid. Voor een geregistreerde entiteit rechtspersoon geven we het adres van de zetel, en – indien van toepassing – het adres van het bijkantoor. Voor een geregistreerde entiteit natuurlijk persoon wordt geen enkel adres gegeven op het niveau van de zetel. Enkel het (de) adres(sen) van de vestigingseenhe(id)(en) worden gegeven.
- **denomination.csv**: bevat 1 lijn per naam van een entiteit, vestigingseenheid of bijkantoor. Een entiteit heeft steeds een naam. Daarnaast kunnen ook een commerciële naam en/of afkorting voorkomen.

Een vestigingseenheid heeft soms een commerciële naam. Een bijkantoor kan een naam van het bijkantoor en/of een afkorting hebben.

- **Branch.CSV**: één lijn per bijkantoor is gelinkt aan een buitenlandse entiteit. **Opgelet, het ID van een bijkantoor is geen officieel nummer. Dit nummer kan nooit gebruikt worden voor een opzoeking in andere public search producten.**

De gegevens uit de verschillende bestanden kunnen aan elkaar worden gekoppeld m.b.v. het ondernemingsnummer of het vestigingseenheidsnummer. De bestanden zijn zo opgezet dat zij eenvoudig op te laden zijn in een relationele databank.

Het is niet noodzakelijk alle bestanden op te laden. Indien u bijvoorbeeld enkel geïnteresseerd bent in entiteiten en hun naam en adres, dan hoeft u bijvoorbeeld het bestand activity.csv niet op te laden.

De CSV-kenmerken van de bestanden zijn:

- Scheidingsteken (delimiter): comma **-** ,
- Afbakening tekst: tussen dubbele quotes – **"**
- Decimaal punt: punt **-** .
- Datumformaat: dd-mm-yyyy

Sommige waarden kunnen leeg zijn (NULL VALUE). In dit geval komt onmiddellijk het volgende scheidingsteken.

#### 1.5.2. HET UPDATE BESTAND

De gegevens in het updatebestand worden geleverd onder de vorm van een ZIP bestand met daarin een aantal CSV bestanden. De gegevens worden op dezelfde wijze gestructureerd als in het full bestand.

Net als bij het full bestand is er een meta.csv. Het bestand code.csv bevat de beschrijvingen van alle codes. Voor de codes ontvangt u dus telkens opnieuw de hele lijst zoals in het full bestand, en niet enkel de wijzigingen.

Voor de andere bestanden uit het full bestand – enterprise.csv, establishment.csv, ... - komen er in het updatebestand 2 types voor:

- Een _delete bestand: bevat de entiteiten of vestigingseenheden waarvoor u in de 1ste stap gegevens moet wissen uit de databank.
- Een _insert bestand: bevat de lijnen die u moet toevoegen in de databank.

Nemen we nemen als voorbeeld.

Als er in KBO een naam bijkomt, wijzigt of gewist wordt, dan:
- komt het ondernemingsnummer in denomination_delete.csv.
- komen alle namen van deze entiteit (niet de historiek) in denomination_insert.csv. Dus ook de eventuele namen van deze entiteit die niet gewijzigd zijn.

U dient dus 2 stappen te doorlopen om uw databank up-to-date te zetten (in pseudo-sql):

1. DELETE FROM mydatabase.denomination WHERE entitynumber IN (SELECT entitynumber FROM denomination_delete.csv)
2. INSERT INTO mydatabase.denomination (SELECT * FROM denomination_insert.csv)

---

## 2. BESCHRIJVING VAN DE BESTANDEN

### 2.1. META.CSV

Het bestand meta.csv bevat de volgende variabelen:

| naam | datatype | verplicht |
|------|----------|-----------|
| Variable | tekst | ja |
| Value | tekst | nee |

De metadata wordt gegeven onder de vorm van key/value-paren. Momenteel bevat het bestand volgende variabelen:

**SnapshotDate**  
Geeft de referentiedatum van de gegevens. Dit is de datum waarop (om middernacht) een snapshot werd genomen van de KBO-databank.

**ExtractTimestamp**  
Geeft het tijdstip waarop het bestand is aangemaakt.

**ExtractType**  
Geeft aan of dit een full of een updatebestand is.

**ExtractNumber**  
Geeft het volgnummer van dit bestand. Telkens een nieuw bestand wordt aangemaakt, wordt dit volgnummer met 1 verhoogd.

**Version**  
Geeft de versie van het KBO opendatabestand. Wanneer het formaat van het bestand wijzigt, dan zal het versienummer worden verhoogd. Voor een beschrijving van het formaat dient u het cookbook met overeenkomstig versienummer te raadplegen.

### 2.2. CODE.CSV

Het bestand code.csv bevat de beschrijvingen van de codes die gebruikt worden in de andere bestanden. Het bevat de volgende variabelen:

| naam | datatype | Formaat | verplicht |
|------|----------|---------|-----------|
| Category | tekst | | ja |
| Code | tekst | | ja |
| Language | tekst | {"DE","EN","FR","NL"} | ja |
| Description | tekst | | ja |

**Category**  
Geeft aan om welke "codetabel" het gaat. De waarde in category komt overeen met de waarde die in de volgende hoofdstukken wordt opgegeven in de kolom codetabel. Bijvoorbeeld: in hoofdstuk 2.3 staat dat voor de variabele 'JuridicalSituation' de codetabel 'JuridicalSituation' gebruikt wordt. De codes in de kolom 'JuridicalSituation' in het bestand enterprise.csv kan je dan in code.csv opzoeken onder category 'JuridicalSituation'. Meestal is de naam van variabele gelijk aan de naam van zijn codetabel.

**Code**  
De code waarvoor een omschrijving wordt gegeven. Een code behoort tot een bepaalde category. Het formaat is afhankelijk van de category waartoe de code behoort. Bijvoorbeeld: voor 'JuridicalSituation' is het formaat 'XXX' (tekst 3 posisties). Het gebruikte formaat kan je opzoeken in de volgende hoofdstukken bij de beschrijving van de variabelen waar deze code wordt gebruikt.

**Language**  
De taal waarin de omschrijving die volgt, is uitgedrukt. Alle codes hebben een beschrijving in het Nederlands en het Frans. Sommige codes hebben ook een beschrijving in het Duits en/of het Engels(*). De gebruikte waarden zijn:

- DE: Duits
- EN: Engels(*)
- FR: Frans
- NL: Nederlands

(*) Op dit moment zijn er nog geen omschrijvingen in het Engels beschikbaar.

**Description**  
De omschrijving van de gegeven code – behorende tot de gegeven category – in de gegeven taal.

### 2.3. ENTERPRISE.CSV

Het bestand enterprise.csv bevat 1 lijn per entiteit met enkele basisgegevens. Het bevat de volgende variabelen:

| naam | datatype | Formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| EnterpriseNumber | tekst | 9999.999.999 | | ja |
| Status | tekst | XX | Status | ja |
| JuridicalSituation | tekst | XXX | JuridicalSituation | ja |
| TypeOfEnterprise | tekst | X | TypeOfEnterprise | ja |
| JuridicalForm | tekst | XXX | JuridicalForm | nee* |
| JuridicalFormCAC | tekst | XXX | JuridicalForm | nee** |
| StartDate | datum | dd-mm-yyyy | | ja |

\* verplicht voor entiteiten rechtspersoon; komt niet voor bij entiteiten natuurlijk persoon

\*\* het bevat de rechtsvorm zoals deze gelezen/beschouwd moet worden, in afwachting van het aanpassen van de statuten conform het Wetboek van Vennootschappen en Verenigingen (WVV).

**EnterpriseNumber**  
Het ondernemingsnummer.

**Status**  
De Status van de entiteit. In dit bestand is dit steeds 'AC': actief.

**JuridicalSituation**  
De rechtstoestand van de entiteit. Zie codetabel.

**TypeOfEnterprise**  
Type entiteit: entiteit rechtspersoon¹ of entiteit natuurlijk persoon. Zie codetabel.

**JuridicalForm**  
De rechtsvorm van de entiteit, indien het een entiteit rechtspersoon betreft. Zie codetabel.

**JuridicalFormCAC**  
Bevat de de rechtsvorm zoals deze gelezen/beschouwd moet worden, in afwachting van het aanpassen van de statuten conform het Wetboek van Vennootschappen en Verenigingen (WVV).

**StartDate**  
De begindatum van de entiteit. Voor entiteiten rechtspersoon is dit de begindatum van de eerste rechtstoestand met status bekendgemaakt of actief. Voor entiteiten natuurlijk persoon is dit de begindatum van de laatste periode waarin de entiteit zich in status bekendgemaakt of actief bevindt.

---

¹ U dient het begrip "entiteit rechtspersoon" zeer ruim te interpreteren, ook organisaties zonder rechtspersoonlijkheid zijn opgenomen in het bestand.

### 2.4. ESTABLISHMENT.CSV

Het bestand establishment.csv bevat 1 lijn per vestigingseenheid met enkele basisgegevens. Het bevat de volgende variabelen:

| naam | datatype | Formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| EstablishmentNumber | tekst | 9.999.999.999 | | ja |
| StartDate | datum | dd-mm-yyyy | | ja |
| EnterpriseNumber | tekst | 9999.999.999 | | ja |

**EstablishmentNumber**  
Het nummer van de vestigingseenheid.

**StartDate**  
De begindatum van de vestigingseenheid.

**EnterpriseNumber**  
Het ondernemingsnummer van de entiteit waartoe deze vestigingseenheid behoort.

### 2.5. DENOMINATION.CSV

Het bestand denomination.csv bevat 1 lijn per naam van een entiteit, een bijkantoor of vestigingseenheid. Een entiteit, bijkantoor of vestigingseenheid kan meerdere namen hebben. Het bevat de volgende variabelen:

| naam | datatype | Formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| EntityNumber | tekst | 9999.999.999 of 9.999.999.999 | | ja |
| Language | tekst | X | Language | ja |
| TypeOfDenomination | tekst | XXX | TypeOfDenomination | ja |
| Denomination | tekst | (320)X | | ja |

**EntityNumber**  
Het vestigingseenheids- of ondernemingsnummer.

**Language**  
Taal van de naam. Zie codetabel.

**TypeOfDenomination**  
Type naam. Zie codetabel.

**Denomination**  
De naam van de entiteit, bijkantoor of vestigingseenheid.

### 2.6. ADDRESS.CSV

Het bestand address.csv bevat
- voor een geregistreerde entiteit rechtspersoon: 1 lijn per adres van een entiteit of vestigingseenheid.
- voor een geregistreerde entiteit natuurlijk persoon: 0 adressen voor de zetel van de entiteit en 1 adres voor elk van haar vestigingseenheden.
- voor een bijkantoor: 1 lijn per adres van het bijkantoor. (Een buitenlandse entiteit kan meerdere bijkantoren in België hebben).

Het bevat de volgende variabelen:

| naam | datatype | Formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| EntityNumber | tekst | 9999.999.999 of 9.999.999.999 | | ja |
| TypeOfAddress | tekst | XXXX | TypeOfAddress | ja |
| CountryNL | tekst | 100(X) | | nee* |
| CountryFR | tekst | 100(X) | | nee* |
| Zipcode | tekst | 20(X) | | nee |
| MunicipalityNL | tekst | 200(X) | | nee |
| MunicipalityFR | tekst | 200(X) | | nee |
| StreetNL | tekst | 200(X) | | nee |
| StreetFR | tekst | 200(X) | | nee |
| HouseNumber | tekst | 22(X) | | nee |
| Box | tekst | 20(X) | | nee |
| ExtraAddressInfo | tekst | 80(X) | | nee |
| DateStrikingOff | datum | dd-mm-yyyyy | | nee |

\* deze variabelen zijn leeg voor een adres in België

**EntityNumber**  
Het vestigingseenheids- of ondernemingsnummer.

**TypeOfAddress**  
Het type adres. Zie codetabel.

**CountryNL**  
Voor een adres in het buitenland: de benaming van het land in het Nederlands.

**CountryFR**  
Voor een adres in het buitenland: de benaming van het land in het Frans.

**Zipcode**  
Postcode.

**MunicipalityNL**  
De naam van de gemeente in het Nederlands.

**MunicipalityNL**  
De naam van de gemeente in het Frans.

**StreetNL**  
Straatnaam in het Nederlands.

**StreetFR**  
Straatnaam in het Frans.

**HouseNumber**  
Huisnummer (zonder busnummer)

**Box**  
Busnummer.

**ExtraAddressInfo**  
Bijkomende informatie over het adres, zoals bijvoorbeeld "City Atrium" of "North Gate II & III".

**DateStrikingOff**  
Indien het adres is doorgehaald, dan staat hier de datum vanaf wanneer het adres doorgehaald is.

### 2.7. CONTACT.CSV

Het bestand contact.csv bevat 1 lijn per contactgegeven van een entiteit of vestigingseenheid. Per entiteit of vestigingseenheid kunnen meerdere contactgegevens voorkomen (bijvoorbeeld 1 of meer telefoonnummer(s) en 1 of meer webadres(sen)). Het bevat de volgende variabelen:

| naam | datatype | Formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| EntityNumber | tekst | 9999.999.999 of 9.999.999.999 | | ja |
| EntityContact | tekst | (3)X | EntityContact | ja |
| ContactType | tekst | (5)X | ContactType | ja |
| Value | tekst | (254)X | | ja |

**EntityNumber**  
Het vestigingseenheids- of ondernemingsnummer.

**EntityContact**  
Geeft aan voor welk type entiteit dit een contactgegeven is: onderneming, bijkantoor of vestigingseenheid. Zie codetabel.

**ContactType**  
Geeft het type contactgegeven aan: telefoonnummer, e-mail of webadres. Zie codetabel.

**Value**  
Het contactgegeven: telefoonnummer, e-mail of webadres.

### 2.8. ACTIVITY.CSV

Het bestand activity.csv bevat 1 lijn per activiteit van een entiteit of vestigingseenheid. De activiteiten kunnen ingeschreven zijn op entiteits- en / of vestigingeenheidsniveau. Het bevat de volgende variabelen:

| naam | datatype | Formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| EntityNumber | tekst | 9999.999.999 of 9.999.999.999 | | ja |
| ActivityGroup | tekst | 999 | ActivityGroup | ja |
| NaceVersion | tekst | {"2003","2008","2025"} | | ja |
| NaceCode | tekst | (5)9 of (7)9 | Nace2003, nace2008, of nace2025 (afh. van NaceVersion) | ja |
| Classification | tekst | XXXX | Classification | ja |

**EntityNumber**  
Het vestigingseenheids- of ondernemingsnummer.

**ActivityGroup**  
Soort activiteit. Zie codetabel.

**NaceVersion**  
Geeft aan of de activiteit is gecodeerd in Nace versie 2003, Nace versie 2008 of Nace versie 2025.

**NaceCode**  
De activiteit van de entiteit of vestigingseenheid, gecodeerd in een Nace code (in de aangegeven versie). Zie codetabel (Nace2003, Nace2008, Nace2025).

**Classification**  
Geeft aan of dit een hoofd-, neven- of hulpactiviteit is. Zie codetabel.

### 2.9. BRANCH.CSV

Het bestand branch.csv bevat één lijn per bijkantoor van de buitenlandse entiteit (een buitenlandse entiteit kan meerdere bijkantoren hebben). Het bestand bevat de volgende variabelen:

| naam | datatype | formaat | codetabel | verplicht |
|------|----------|---------|-----------|-----------|
| Id | tekst | 9999.999.999 of 9.999.999.999 | | ja |
| StartDate | datum | dd-mm-jjjj | | ja |
| EnterpriseNumber | tekst | 9999.999.999 of 9.999.999.999 | | ja |

**Id**  
Met het id kan een bijkantoor geïdentificeerd worden.

**StartDate**  
De startdatum van het bijkantoor.

**EnterpriseNumber**  
Het ondernemingsnummer van de entiteit die verbonden is aan het bijkantoor.

---

*Cookbook KBO Open data - versie R018.00*
