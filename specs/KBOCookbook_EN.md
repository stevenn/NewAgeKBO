# Cookbook KBO Open data

**Version R018.00**

**Contact person:** Vincent Lheureux  
ICT Staff Service  
Every working day from 9 am to 4 pm. In case of unavailability during these hours, by appointment.

Koning Albert II-laan 16  
1000 Brussels

T +32 (0) 2 277 94 50  
F +32 (0) 2 277 51 80

Vincent.lheureux@economie.fgov.be  
http://economie.fgov.be

---

FOD Economy, SMEs, Self-Employed and Energy

---

## Contents

- [Introduction](#introduction)
- [1. General concepts](#1-general-concepts)
  - [1.1. Who can use the files?](#11-who-can-use-the-files)
  - [1.2. Where are the files made available?](#12-where-are-the-files-made-available)
  - [1.3. When are the files made available?](#13-when-are-the-files-made-available)
  - [1.4. What data do the files contain?](#14-what-data-do-the-files-contain)
  - [1.5. How are the files structured?](#15-how-are-the-files-structured)
    - [1.5.1. The full file](#151-the-full-file)
    - [1.5.2. The update file](#152-the-update-file)
- [2. Description of the files](#2-description-of-the-files)
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

## INTRODUCTION

This document is a technical description of the files offered under the KBO Open data license by the FOD Economy, SMEs, Energy and Self-Employed.

These files contain publicly accessible data from KBO (Crossroads Bank for Enterprises). The complete file is updated monthly, while the update file is updated daily.

---

## 1. GENERAL CONCEPTS

### 1.1. WHO CAN USE THE FILES?

The open data files are available to anyone who accepts the KBO Open data license. Via the website https://kbopub.economie.fgov.be/kbo-open-data you can start a request to gain access to the KBO Open data files.

### 1.2. WHERE ARE THE FILES MADE AVAILABLE?

The KBO Open data files can be downloaded via the website https://kbopub.economie.fgov.be/kbo-open-data or via SFTP server. For this you must register on this website.

### 1.3. WHEN ARE THE FILES MADE AVAILABLE?

Every first Sunday of the month, new files are created based on a snapshot of the KBO database taken on the preceding Friday (at midnight). These files are then made available on the website on this first Sunday of the month, or the Monday following. Each month the extract number is increased by 1. Only the last 4 extracts are kept on the server. If technical problems occur, this schedule may be deviated from.

### 1.4. WHAT DATA DO THE FILES CONTAIN?

The KBO Open data files contain active data about active registered entities and establishments as they are registered in KBO on the snapshot date.

The files do not contain history. If, for example, the address of an active entity changes, the file will only contain the new address and not the old address.

In chapter 2, all variables that appear in the KBO Open data files are described.

### 1.5. HOW ARE THE FILES STRUCTURED?

There are 2 types of files:

- A complete file – this contains all the data listed in chapter 2 of all active entities and their active establishments included in KBO Open data (hereafter referred to as "full" file)
- An update file with the mutations between the last and the penultimate full file.

The first time you load the data, you naturally use the full file. To keep your database up-to-date, you can then choose whether to reload the full file monthly or only update your database with the changes from the update file.

The files follow this naming convention:
- full file: KboOpenData_<extractnr>_<year>_<month>_Full.zip
- update file: KboOpenData_<extractnr>_<year>_<month>_Update.zip

#### 1.5.1. THE FULL FILE

The data in the full file is delivered in the form of a ZIP file containing a number of CSV files:

- **meta.csv**: contains some metadata about this full file (version number, creation time, ...).
- **code.csv**: contains the descriptions of the codes used in the other files.
- **contact.csv**: contains contact details of entities and establishments.
- **enterprise.csv**: contains 1 line per entity with some basic data.
- **establishment.csv**: contains 1 line per establishment with some basic data.
- **activity.csv**: contains 1 line per activity of an entity or establishment. An entity or establishment can have multiple activities.
- **address.csv**: contains 0, 1 or 2 lines per address of an entity or establishment. For a registered legal person entity, we provide the address of the seat, and – if applicable – the address of the branch office. For a registered natural person entity, no address is given at the level of the seat. Only the address(es) of the establishment(s) are given.
- **denomination.csv**: contains 1 line per name of an entity, establishment or branch office. An entity always has a name. In addition, a commercial name and/or abbreviation may also occur.

An establishment sometimes has a commercial name. A branch office can have a branch office name and/or an abbreviation.

- **Branch.CSV**: one line per branch office is linked to a foreign entity. **Note, the ID of a branch office is not an official number. This number can never be used for a search in other public search products.**

The data from the different files can be linked together using the enterprise number or establishment number. The files are designed so that they can easily be loaded into a relational database.

It is not necessary to load all files. If, for example, you are only interested in entities and their name and address, you do not need to load the activity.csv file.

The CSV characteristics of the files are:

- Delimiter: comma **-** ,
- Text delimiter: between double quotes – **"**
- Decimal point: period **-** .
- Date format: dd-mm-yyyy

Some values can be empty (NULL VALUE). In this case, the next delimiter immediately follows.

#### 1.5.2. THE UPDATE FILE

The data in the update file is delivered in the form of a ZIP file containing a number of CSV files. The data is structured in the same way as in the full file.

As with the full file, there is a meta.csv. The code.csv file contains the descriptions of all codes. For the codes, you therefore always receive the entire list as in the full file, and not just the changes.

For the other files from the full file – enterprise.csv, establishment.csv, ... - there are 2 types in the update file:

- A _delete file: contains the entities or establishments for which you must delete data from the database in the 1st step.
- An _insert file: contains the lines that you must add to the database.

Let's take an example.

If a name is added, changed or deleted in KBO, then:
- the enterprise number appears in denomination_delete.csv.
- all names of this entity (not the history) appear in denomination_insert.csv. So also any names of this entity that have not been changed.

You therefore need to go through 2 steps to update your database (in pseudo-sql):

1. DELETE FROM mydatabase.denomination WHERE entitynumber IN (SELECT entitynumber FROM denomination_delete.csv)
2. INSERT INTO mydatabase.denomination (SELECT * FROM denomination_insert.csv)

---

## 2. DESCRIPTION OF THE FILES

### 2.1. META.CSV

The meta.csv file contains the following variables:

| name | datatype | mandatory |
|------|----------|-----------|
| Variable | text | yes |
| Value | text | no |

The metadata is provided in the form of key/value pairs. Currently the file contains the following variables:

**SnapshotDate**  
Provides the reference date of the data. This is the date on which (at midnight) a snapshot was taken of the KBO database.

**ExtractTimestamp**  
Provides the time at which the file was created.

**ExtractType**  
Indicates whether this is a full or an update file.

**ExtractNumber**  
Provides the sequence number of this file. Each time a new file is created, this sequence number is increased by 1.

**Version**  
Provides the version of the KBO open data file. When the format of the file changes, the version number will be increased. For a description of the format, you should consult the cookbook with the corresponding version number.

### 2.2. CODE.CSV

The code.csv file contains the descriptions of the codes used in the other files. It contains the following variables:

| name | datatype | Format | mandatory |
|------|----------|--------|-----------|
| Category | text | | yes |
| Code | text | | yes |
| Language | text | {"DE","EN","FR","NL"} | yes |
| Description | text | | yes |

**Category**  
Indicates which "code table" it concerns. The value in category corresponds to the value specified in the code table column in the following chapters. For example: in chapter 2.3 it states that for the variable 'JuridicalSituation' the code table 'JuridicalSituation' is used. The codes in the 'JuridicalSituation' column in the enterprise.csv file can then be looked up in code.csv under category 'JuridicalSituation'. Usually the name of the variable is the same as the name of its code table.

**Code**  
The code for which a description is given. A code belongs to a certain category. The format depends on the category to which the code belongs. For example: for 'JuridicalSituation' the format is 'XXX' (text 3 positions). The format used can be found in the following chapters in the description of the variables where this code is used.

**Language**  
The language in which the following description is expressed. All codes have a description in Dutch and French. Some codes also have a description in German and/or English(*). The values used are:

- DE: German
- EN: English(*)
- FR: French
- NL: Dutch

(*) At this moment there are no descriptions available in English yet.

**Description**  
The description of the given code – belonging to the given category – in the given language.

### 2.3. ENTERPRISE.CSV

The enterprise.csv file contains 1 line per entity with some basic data. It contains the following variables:

| name | datatype | Format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| EnterpriseNumber | text | 9999.999.999 | | yes |
| Status | text | XX | Status | yes |
| JuridicalSituation | text | XXX | JuridicalSituation | yes |
| TypeOfEnterprise | text | X | TypeOfEnterprise | yes |
| JuridicalForm | text | XXX | JuridicalForm | no* |
| JuridicalFormCAC | text | XXX | JuridicalForm | no** |
| StartDate | date | dd-mm-yyyy | | yes |

\* mandatory for legal person entities; does not occur for natural person entities

\*\* contains the legal form as it should be read/considered, pending the adaptation of the statutes in accordance with the Code of Companies and Associations (WVV).

**EnterpriseNumber**  
The enterprise number.

**Status**  
The Status of the entity. In this file this is always 'AC': active.

**JuridicalSituation**  
The legal status of the entity. See code table.

**TypeOfEnterprise**  
Type of entity: legal person entity¹ or natural person entity. See code table.

**JuridicalForm**  
The legal form of the entity, if it is a legal person entity. See code table.

**JuridicalFormCAC**  
Contains the legal form as it should be read/considered, pending the adaptation of the statutes in accordance with the Code of Companies and Associations (WVV).

**StartDate**  
The start date of the entity. For legal person entities this is the start date of the first legal status with status announced or active. For natural person entities this is the start date of the last period in which the entity is in announced or active status.

---

¹ You should interpret the concept "legal person entity" very broadly, organizations without legal personality are also included in the file.

### 2.4. ESTABLISHMENT.CSV

The establishment.csv file contains 1 line per establishment with some basic data. It contains the following variables:

| name | datatype | Format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| EstablishmentNumber | text | 9.999.999.999 | | yes |
| StartDate | date | dd-mm-yyyy | | yes |
| EnterpriseNumber | text | 9999.999.999 | | yes |

**EstablishmentNumber**  
The establishment number.

**StartDate**  
The start date of the establishment.

**EnterpriseNumber**  
The enterprise number of the entity to which this establishment belongs.

### 2.5. DENOMINATION.CSV

The denomination.csv file contains 1 line per name of an entity, a branch office or establishment. An entity, branch office or establishment can have multiple names. It contains the following variables:

| name | datatype | Format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| EntityNumber | text | 9999.999.999 or 9.999.999.999 | | yes |
| Language | text | X | Language | yes |
| TypeOfDenomination | text | XXX | TypeOfDenomination | yes |
| Denomination | text | (320)X | | yes |

**EntityNumber**  
The establishment or enterprise number.

**Language**  
Language of the name. See code table.

**TypeOfDenomination**  
Type of name. See code table.

**Denomination**  
The name of the entity, branch office or establishment.

### 2.6. ADDRESS.CSV

The address.csv file contains
- for a registered legal person entity: 1 line per address of an entity or establishment.
- for a registered natural person entity: 0 addresses for the seat of the entity and 1 address for each of its establishments.
- for a branch office: 1 line per address of the branch office. (A foreign entity can have multiple branch offices in Belgium).

It contains the following variables:

| name | datatype | Format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| EntityNumber | text | 9999.999.999 or 9.999.999.999 | | yes |
| TypeOfAddress | text | XXXX | TypeOfAddress | yes |
| CountryNL | text | 100(X) | | no* |
| CountryFR | text | 100(X) | | no* |
| Zipcode | text | 20(X) | | no |
| MunicipalityNL | text | 200(X) | | no |
| MunicipalityFR | text | 200(X) | | no |
| StreetNL | text | 200(X) | | no |
| StreetFR | text | 200(X) | | no |
| HouseNumber | text | 22(X) | | no |
| Box | text | 20(X) | | no |
| ExtraAddressInfo | text | 80(X) | | no |
| DateStrikingOff | date | dd-mm-yyyyy | | no |

\* these variables are empty for an address in Belgium

**EntityNumber**  
The establishment or enterprise number.

**TypeOfAddress**  
The type of address. See code table.

**CountryNL**  
For an address abroad: the name of the country in Dutch.

**CountryFR**  
For an address abroad: the name of the country in French.

**Zipcode**  
Postal code.

**MunicipalityNL**  
The name of the municipality in Dutch.

**MunicipalityNL**  
The name of the municipality in French.

**StreetNL**  
Street name in Dutch.

**StreetFR**  
Street name in French.

**HouseNumber**  
House number (without box number)

**Box**  
Box number.

**ExtraAddressInfo**  
Additional information about the address, such as "City Atrium" or "North Gate II & III".

**DateStrikingOff**  
If the address has been crossed out, this shows the date from which the address was crossed out.

### 2.7. CONTACT.CSV

The contact.csv file contains 1 line per contact detail of an entity or establishment. Per entity or establishment, multiple contact details can occur (for example 1 or more telephone number(s) and 1 or more web address(es)). It contains the following variables:

| name | datatype | Format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| EntityNumber | text | 9999.999.999 or 9.999.999.999 | | yes |
| EntityContact | text | (3)X | EntityContact | yes |
| ContactType | text | (5)X | ContactType | yes |
| Value | text | (254)X | | yes |

**EntityNumber**  
The establishment or enterprise number.

**EntityContact**  
Indicates for which type of entity this is contact information: enterprise, branch office or establishment. See code table.

**ContactType**  
Indicates the type of contact detail: telephone number, e-mail or web address. See code table.

**Value**  
The contact detail: telephone number, e-mail or web address.

### 2.8. ACTIVITY.CSV

The activity.csv file contains 1 line per activity of an entity or establishment. The activities can be registered at entity and/or establishment level. It contains the following variables:

| name | datatype | Format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| EntityNumber | text | 9999.999.999 or 9.999.999.999 | | yes |
| ActivityGroup | text | 999 | ActivityGroup | yes |
| NaceVersion | text | {"2003","2008", "2025"} | | yes |
| NaceCode | text | (5)9 or (7)9 | Nace2003, nace2008, or nace2025 (dep. on NaceVersion) | yes |
| Classification | text | XXXX | Classification | yes |

**EntityNumber**  
The establishment or enterprise number.

**ActivityGroup**  
Type of activity. See code table.

**NaceVersion**  
Indicates whether the activity is coded in Nace version 2003, Nace version 2008 or Nace version 2025.

**NaceCode**  
The activity of the entity or establishment, coded in a Nace code (in the indicated version). See code table (Nace2003, Nace2008, Nace2025).

**Classification**  
Indicates whether this is a main, secondary or auxiliary activity. See code table.

### 2.9. BRANCH.CSV

The branch.csv file contains one line per branch office of the foreign entity (a foreign entity can have multiple branch offices). The file contains the following variables:

| name | datatype | format | code table | mandatory |
|------|----------|--------|-----------|-----------|
| Id | text | 9999.999.999 or 9.999.999.999 | | yes |
| StartDate | date | dd-mm-yyyy | | yes |
| EnterpriseNumber | text | 9999.999.999 or 9.999.999.999 | | yes |

**Id**  
A branch office can be identified with the id.

**StartDate**  
The start date of the branch office.

**EnterpriseNumber**  
The enterprise number of the entity associated with the branch office.

---

*Cookbook KBO Open data - version R018.00*
