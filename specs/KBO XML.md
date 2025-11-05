# KBO (Crossroads Bank for Enterprises) - Technical User Manual for Data Reuse

**Version:** 20.4  
**Last Update:** 12/03/2024

---

## Document Information

**Publisher:**  
FOD Economie, K.M.O., Middenstand en Energie  
Vooruitgangstraat 50  
1210 Brussels  
Enterprise Number: 0314.595.348

**Contact:**
- Phone: 0800 120 33 (free number)
- Website: https://economie.fgov.be

---

## Table of Contents

1. [Introduction](#introduction)
2. [Purpose and Target Audience](#purpose-and-target-audience)
3. [The Process](#the-process)
4. [The Data File](#the-data-file)
   - 4.1 [Reusable Data](#reusable-data)
   - 4.2 [Delivered Files](#delivered-files)
5. [Annex 1: XML Field Descriptions in the Data File](#annex-1-xml-field-descriptions)
6. [Annex 2: XML Field Descriptions in the Code File](#annex-2-xml-field-descriptions-code)
7. [Annex 3: Processing the Reuse File](#annex-3-processing)

---

## 1. Introduction {#introduction}

The Crossroads Bank for Enterprises (KBO) is responsible for:
- Recording
- Storing
- Managing
- Making available information concerning the identification of registered entities

The KBO management service, established within the Federal Public Service Economy, SMEs, Middle Class and Energy, is responsible for managing the Crossroads Bank for Enterprises.

The KBO contains all basic identification data of registered entities and their establishment units. It receives data from:
- Clerks of commercial courts
- Business counters
- VAT administration
- RSZ (social security)

These institutions keep the data up to date by entering, modifying, and deleting information. The Federal Public Service Economy brings all this existing data together in the Crossroads Bank for Enterprises.

This document first discusses the purpose and target audience, then details the KBO files and how they can be downloaded from the available FTP server. The final chapters and annexes cover the data model, describing in detail how the files are structured and the meaning of the various fields included in the data files.

---

## 2. Purpose and Target Audience {#purpose-and-target-audience}

This document is written to explain the reuse of public KBO data. The purpose is to describe the technical context of making data available for reuse for commercial or non-commercial purposes.

This document covers:
- How a user gains access to data for reuse (commercial or non-commercial)
- A detailed description of the fields included in both the data and code XML files

The licensee has the option to develop software themselves, based on the provided KBO file, to determine how the data is displayed and to search for relevant information based on specific criteria in the provided files. The description of this software falls outside the scope of this document.

---

## 3. The Process {#the-process}

The data file is made available on the FTP server every working day. It will remain accessible for 5 working days, after which the file is deleted and archived. In addition to the data file, the code file will also be made available in the same way.

To allow the licensee to gain better insight into the data structure of these files, the respective XML Schema files (KBO.xsd, Codes.xsd) will also be made available on the FTP server. These schema files will also allow the licensee to validate the daily available files for their technical correctness.

### 3.1 FTP Server Access

Below describes how the licensee can access the FTP server of the Federal Public Service Economy, SMEs, Middle Class and Energy.

First, the licensee needs an application that makes it possible to download files from a secure FTP server (SFTP or FTP over SSH). The download process is shown using the FileZilla application, which is freely available at: http://filezilla-project.org/

Note: This is open source software distributed under the terms of the GNU General Public License.

### 3.2 Connection Settings

```
Host: ftps.economie.fgov.be
Port: 22
Server Type: SSH File Transfer Protocol
Login Type: Normal
```

The username and related password to be filled in are those received by the licensee from the KBO management service.

During the first connection, verify that the 'fingerprint' shown in the message matches:

```
ssh-rsa 1024 f9:ab:ed:0e:c7:b4:a2:fd:61:23:2f:8a:28:d2:67:9b
```

You can only assume a connection is being established to the correct FTP server if this data matches.

Once logged in successfully, an overview of the files available for download is shown.

---

## 4. The Data File {#the-data-file}

### 4.1 Reusable Data {#reusable-data}

The following paragraphs describe which data is made available.

The KBO contains the following data elements:
- Enterprise numbers and establishment unit numbers
- Denominations: names, trade names, and abbreviations
- Addresses: registered office addresses, establishment unit addresses
- Contact information
- Legal forms and legal situations of the entity
- Functions performed within the entity, as well as entrepreneurial skills
- Activities identified via NACEBEL codes
- Financial data: capital, financial year dates, month of annual meeting, and bank account numbers
- Permissions
- Authorizations
- Working partners
- Ex officio actions
- Foreign identification data
- Data from branches registered in Belgium for foreign legal entities
- Links between entities and between an entity and its establishments
- Establishment unit data

However, not all data can be reused for commercial or non-commercial purposes and will therefore not be found in the data file. The law specifies which data from the KBO can be publicly released. Please read the data catalog for a detailed overview of reusable data.

### 4.2 Delivered Files {#delivered-files}

The data is delivered in eXtensible Markup Language (XML) format. XML is a standard for representing structured data in plain text form. This representation is both machine-readable and human-readable. The data is presented hierarchically.

#### File Types

Three XML files are created by the KBO:

1. **Full reuse file:** `Dyyyymmdd.xml` (where yyyymmdd is the date the file was created)
2. **Change reuse file:** `Dyyyymmdd.wijzig.xml` (where yyyymmdd is the date the file was created)
3. **Code file:** `Dyyyymmdd.codes.XML` (where yyyymmdd is the date the file was created)

The full or change file is combined with the code file in a compressed file: `Dyymmdd.KBO.ZIP` (yyyymmdd is the date the file was created). This file is made available on the FTP server.
- Full file location: `full` subdirectory
- Change file location: `delta` subdirectory

Additionally, XML Schema files for both codes (`Codes.xsd`) and data (`KBO.xsd`) are available on the FTP server. A new version will only be made available when changes are made by the KBO.

#### 4.2.1 The Data File

**Types of Data Files:**
1. **Full reuse file:** Contains all establishment units and entities from the KBO database
2. **Change reuse file:** Contains only entities and establishment units that were modified or created compared to the previous change reuse file

**Entities/Establishments:**

The data files provide information about:
- Entities
- Establishment units

Entities are linked to establishments via the `linkedEnterprise` data group of type '001'. The entity data group includes links to all establishment units that are or were linked to the entity. The establishment unit data group contains a link to the entities to which it belongs or belonged.

**Structure of Data Files:**

The structure of the data in change reuse files and full reuse files is identical.

Annex 1 provides an overview of all fields (XML tags) that can potentially be included in the delivered XML files. For each field, it is also indicated whether it is a mandatory field.

If data is coded, the name of the code table is shown in square brackets. This code table contains the possible values the field can assume.

**Histories:**

Both active and discontinued data will be included in the data files.

The complete history will be included for the following data groups:
- EnterpriseNumbers
- Activities
- Addresses
- ContactInformations
- Authorizations
- BankAccounts
- Denominations
- FinancialDatas
- Functions
- Professions
- WorkingPartners
- JurForms
- JurSits
- LinkedEnterprises
- Permissions
- ExOfficioExecutions
- ForeignIdentifications
- Branches

A history of a data group is built using the `ValidityPeriod`. A start date (`Begin`) and end date (`End`) indicate when data was valid.

**Canceled Entities/Establishment Units:**

When an entity/establishment unit delivered in one of the previous change reuse files is deleted from KBO, the entity/establishment unit appears in the reuse file as `CancelledEnterprises` or `CancelledBusinessUnit`.

**Size:**

The change reuse file contains an average of about 6,000 entities or establishments that have been modified.

However, occasionally large numbers may be transmitted, for example during migrations, changed legislation, or problem resolution. Therefore, account for potentially large numbers of changed data.

**Processing Files:**

Annex 3 describes a method for processing the data.

#### 4.2.2 The Code File

As mentioned above, certain data in the data file is included in code form. The meaning of these codes is described in the code XML file, also made available on the FTP server. These codes are used to limit the size of the data files. An overview of these code fields and their meanings is given in Annex 2.

There is one exception within the 'address' data group where the code is not included in a separate code file but directly in the data file itself. This concerns the XML tags 'Code' and 'NisCode' which are included in the data file in the language(s) of the municipality.

The structure of each code element is almost always identical, consisting of:
- The code itself
- A description of the code in French, Dutch, German, and English
- The validity period through a start and end date

For the following code types, additional attributes are included:

**CountryCodes:** Also includes the current ISO country code, corresponding to the country code from Internal Affairs

**FunctionCodes:** Also includes the 'type' field, indicating whether it's a legal function (value '1') or entrepreneurial skills (value '2')

**NacebelCodes:** Also includes the version of the Nacebel codes, either 2003 or 2008 or 2025

**PermissionCodes:** Also includes:
- 'type': indicates whether it's a permission (value '1') or authorization (value '2')
- 'administratiecode': indicates the administration that grants the authorization or permission

Code tables can change: new codes can be added, codes can be discontinued, and descriptions corrected. Although code tables are not as subject to changes as entity data itself, it is still essential that every change is communicated as quickly as possible. Therefore, code tables are delivered with the same frequency as entity data.

#### 4.2.2.1 Format

All XML files are encoded in UTF-8 and can contain characters of more than 1 byte.

---

## 5. Annex 1: XML Field Descriptions in the Data File {#annex-1-xml-field-descriptions}

### 5.1 CommercialisationFileType

Root element of the commercialization file.

**Elements:**

#### Header (HeaderType, required)
Contains general data about the reuse file.

- **ExecutionDate** (DateType, required): Date the file was generated
- **SequenceNumber** (long(10), required): Sequence number of the extract, incremented by 1 for each new change extract
- **ExtractVersion** (string(6), required): Version of the extract, incremented with each new release
- **ExtractType** (string(1), required): Type of extract - 'F' for full extract, 'D' for change extract
- **ExtractBegin** (TimeStampType, required): Lowest timestamp present in the extract
- **ExtractEnd** (TimeStampType, required): Highest timestamp present in the extract

#### CancelledBusinessUnits (CancelledBusinessUnitNumberType, optional)
Contains establishment units that were canceled since the previous generation.

- **CancelledBusinessUnit** (EnterpriseNumberType, 0...*): Establishment unit number of the canceled establishment

#### CancelledEnterprises (CancelledEnterprisesType, optional)
Contains entities that were canceled since the previous generation.

- **CancelledEnterpriseNumber** (EnterpriseNumberType, required): Entity number ('Technical Key') of the canceled entity

#### Enterprises (EnterprisesType, optional)
Contains entities that have been modified since the previous generation.

- **Enterprise** (EnterpriseType, 0...*): This data structure groups data about an entity. An entity means either:
  - A natural person entity (e.g., self-employed traders)
  - A legal entity or company or association without legal personality (e.g., companies, de facto associations)
  
  Each entity has a legal situation at every moment in its lifecycle and has an address/domicile address and a name at every moment.
  
  **Note:** To comply with GDPR requirements, the registered office address of a natural person entity is not provided.
  
  Each entity is uniquely identified by an entity number and has an enterprise number at every moment in its lifecycle.

#### BusinessUnits (BusinessUnitsType, optional)
Contains establishments that have been modified since the previous generation.

- **BusinessUnit** (BusinessUnitType, 0...1): The establishment unit data structure groups data about the entity's establishment units. An establishment unit in KBO is a place that can be geographically identified by an address, where at least one activity of the entity is carried out or from which the activity is carried out.
  
  Establishment units have neither a legal form nor a legal situation. Each establishment unit is uniquely identified by an establishment unit number.

#### Footer (FooterType, required)

- **NbrOfEnterprises** (long(10), required): Number of non-canceled entities included in this reuse file
- **NbrOfBusinessUnits** (long(10), required): Number of non-canceled establishment units included in the reuse file
- **NbrOfCancelledEnterprises** (long(10), required): Number of canceled entities included in this reuse file
- **NbrOfCancelledBusinessUnits** (long(10), required): Number of canceled establishment units included in this reuse file

---

### 5.2 EnterpriseType

This data structure groups data about an entity.

**Elements:**

#### Nbr (EnterpriseNumberType, required)
The identification number of the entity, called the entity number, is a unique number assigned to the entity. This number corresponds to the enterprise number at the time of entity creation ('technical key'). The entity number contains no meaningful information.

**Structure:**
- Position 1-8: Sequence number
- Position 9-10: Check digit (97 - modulo 97)

#### EnterpriseNumbers (required)
See EnterpriseNumberHistoryType

#### Type (string(1) [TypeOfEnterpriseCodes], required)
Indicates whether the entity is a natural person entity or a legal entity/entity without legal personality.

**Allowed values:**
- 1 = natural person entity
- 2 = legal entity or entity without legal personality

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Date the entity was established
- **End** (DateType, optional): Date the entity was discontinued or no longer has legal personality

#### CloseDate (DateType, optional)
Date of closing an entity (in case of duplicates)

#### Status (StatusType [StatusCodes], required)
Status of the entity within the Crossroads Bank for Enterprises

#### Capital (decimal(15) with 2 decimal places, optional)
The current capital of the entity. Initially, this is the capital stated at the entity's establishment. This may be updated later if a capital increase or decrease is executed.

#### Currency (string(3) [CurrencyCode], optional)
Currency code in which the stated capital is expressed, according to the alphanumeric ISO currency code standard

#### Duration (long(4), optional)
If the entity was established with a limited duration, this duration is recorded in years. For entities of unlimited duration, this data is not filled in.

#### Denominations (optional)
See DenominationType

#### Addresses (optional)
See AddressType

#### ContactInformations (optional)
See ContactInformationType

#### JurForms (optional)
See JurFormType

#### JurSits (optional)
See JurSitType

#### Activities (optional)
See ActivityType

#### BankAccounts (optional)
See BankAccountType

#### FinancialDatas (optional)
See FinancialDataType

#### Functions (optional)
See FunctionType

#### Professions (optional)
See ProfessionType

#### WorkingPartners (optional)
See WorkingPartnerType

#### Permissions (optional)
See PermissionType

#### Authorizations (optional)
See AuthorizationType

#### ForeignIdentifications (optional)
See ForeignIdentificationType

#### Branches (optional)
See BranchType

#### LinkedEnterprises (optional)
See LinkedEnterpriseType

#### ExOfficioExecutions (optional)
See ExOfficioExecutionType

---

### 5.3 EnterpriseNumberHistoryType

**EnterpriseNumbers** (EnterpriseNumberHistoryType, required): This data structure contains the history of enterprise numbers for the entity.

An entity can be assigned different enterprise numbers throughout its life. The following rules apply:

1. An entity always has exactly one assigned enterprise number between its start date (inclusive) and end date (inclusive) at every moment
2. An enterprise number can never be simultaneously assigned to 2 entities

**Elements:**

#### EnterpriseNumber (EnterpriseNumberType, required)
The enterprise number assigned or was assigned to this entity

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the assignment of the enterprise number to this entity
- **End** (DateType, optional): End date of the assignment of the enterprise number to this entity

---

### 5.4 DenominationType

**Denomination** (DenominationType, optional): The denomination data structure contains the denomination(s) of the entity, possibly in different languages if applicable.

An entity always has a name.

**Elements:**

#### Type (string(3) [DenominationCodes], required)
Indicates what type of name it is (e.g., name, abbreviation, trade name).

The following rules apply for certain denomination types:

1. **For natural person entities:**
   - name = surname + first name of natural person (required)
   - no abbreviation

2. **For legal entity entities:**
   - name (required)
   - abbreviation (optional)
   - trade name (optional)

3. **For branches:**
   - branch name (optional)
   - abbreviation (optional)

4. **For establishment units:**
   - trade name (optional)

#### Language (string(1) [LanguageCodes], required)
The language in which the denomination is recorded

#### Name (string(192), required)
The denomination itself

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the denomination
- **End** (DateType, optional): End date of the denomination

#### ExOfficioExecutions (optional)

Contains ex officio actions performed on the denomination and/or on specific fields of the denomination.

- **ExOfficioExecution** (ExOfficioExecutionType, required)
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the denomination, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.5 AddressType

Every entity, establishment unit, and branch has an address.

For a natural person entity, this is the domicile address/main residence. For a legal entity or entity without legal personality, this is the registered office address.

Establishment units and branches always have a Belgian address. Entities can also have foreign addresses.

Depending on the format in which the address data is encoded, certain fields will always, optionally, or never be filled in the AddressCoding:

#### Format 001: "RRN" - Belgian address with street code + NIS code

This format is used for Belgian addresses encoded with postal code + street code (as defined by municipalities and managed by Internal Affairs) and additionally a NIS code for the municipality.

**Always filled fields:**
- FormatCode: value 001
- Description: at least 1, possibly more
  - Language (only official languages of this municipality based on language regime)
  - StreetName (in the specified language)
  - CommunityName (in the specified language)
- PostCode
- CountryCode: value '150' (Belgium)
- StreetCode
- NisCode
- Validity

**Optional fields:**
- HouseNbr
- PostBox
- TechnicalCreationReason
- TechnicalStopReason

#### Format 002: Foreign

This format is only used for foreign addresses.

**Always filled fields:**
- FormatCode: value 002
- Validity

**Optional fields:**
- PostCode
- Description: max 1, without language
  - StreetName
  - CommunityName
- HouseNbr
- PostBox
- State
- CountryCode: value different from '150' (Belgium)
- TechnicalCreationReason
- TechnicalStopReason

#### Format 003: Text

This format is only used for Belgian addresses that at the start of KBO had no codes for street and/or municipality in the source file.

**Always filled fields:**
- FormatCode: value 003
- Validity

**Optional fields:**
- PostCode
- Description: 0, 1, or possibly more than 1
  - Language (only if official languages of this municipality can be determined, possibly no language)
  - StreetName (in the specified language/without language)
  - CommunityName (in the specified language/without language)
- HouseNbr
- PostBox
- StreetCode: not in combination with NisCode; only possible if PostCode is also filled
- NisCode: not in combination with StreetCode
- CountryCode
- TechnicalCreationReason
- TechnicalStopReason

**Note:** If the "streetcode" field is not filled, the street should be considered "without language". If the "niscode" field is not filled, the municipality should be considered "without language".

#### Format 004: BeSt

This format is only used for Belgian addresses encoded with a BeSt key. In addition to the key itself, the 'written form' of the address is also included.

**Always filled fields:**
- FormatCode: value 004
- Description: at least 1, possibly more
  - Language (only official languages of this municipality based on language regime)
  - StreetName (in the specified language)
  - CommunityName (in the specified language)
- HouseNbr
- PostCode
- CountryCode: value '150' (Belgium)
- BeStCode
  - Namespace
  - ObjectId
  - VersionId
- Validity

**Optional fields:**
- PostBox
- TechnicalCreationReason
- TechnicalStopReason

#### Format 005: "Anomaly" - Belgian address for which a BeSt anomaly file exists

This format is only used for Belgian addresses for which an anomaly file exists to include it in BeSt. Besides the indication that an anomaly file exists, the information is similar to format 001.

**Always filled fields:**
- FormatCode: value 005
- Description: at least 1, possibly more
  - Language (only official languages of this municipality based on language regime)
  - StreetName (in the specified language)
  - CommunityName (in the specified language)
- PostCode
- CountryCode: value '150' (Belgium)
- NisCode
- AnomalyFileIndicator: value 'true'
- Validity

**Optional fields:**
- HouseNbr
- StreetCode
- PostBox
- TechnicalCreationReason
- TechnicalStopReason

**Address Structure (AddressType):**

#### Type (string(3) [AddressTypeCodes], required)
Indicates what type of address it is.

Examples:
- Registered office address/domicile address
- Establishment unit address
- Branch address

#### AddressCoding (required)

**Elements:**

- **FormatCode** (string(3) [AddressFormatCodes], required): Indicates the format in which address details are encoded

- **Description**
  - **StreetName** (string(100), optional): Standardized street name at the entity's address. If the street code is provided, the street name is from the street code table, according to one of the specified languages possible according to Belgian language legislation. If the BeSt code of the address is provided, the street name is from BeSt, according to one of the specified languages possible according to Belgian language legislation. If no street code or BeSt code is filled in, this is the street name as received by the data supplier.
  
  - **CommunityName** (string(100), optional): Municipality name of the entity's/establishment unit's/branch's address. If the NisCode is provided, the municipality name is from the NisCode table, according to one of the specified languages possible according to Belgian language legislation. If the BeSt code of the address is provided, the municipality name is from BeSt, according to one of the specified languages possible according to Belgian language legislation. If no NisCode or BeSt code is filled in, this is the municipality name as received by the data supplier.
  
  - **Language** ([LanguageCodes], optional): The language in which the address is recorded. Note: If it's a bilingual municipality, the address is recorded in both national languages.

- **HouseNbr** (string(11), optional): House number of the entity's/establishment unit's/branch's address

- **PostBox** (string(10), optional): Box number of the entity's/establishment unit's/branch's address

- **PostCode** (string(15), optional): Postal code of the entity's/establishment unit's/branch's address

- **State** (string(3), optional): For some foreign addresses, a state must be specified (e.g., for America). The state in coded form can be specified here. This does not apply to Belgian addresses.

- **CountryCode** (string(3) [CountryCodes], optional): Country of the entity's/establishment unit's/branch's address. The country codes included here are those made available by the FPS Internal Affairs, which itself follows the standard of the FPS Foreign Affairs.

- **StreetCode** (string(4), optional): For street names of Belgian municipalities, a street code table is used to standardize street names. This data is the identification code of the street from the coded street file. The street code itself is language-independent. This is only used for streets from Belgium, not for foreign addresses. Note: The use of street codes starts at the startup of KBO. For addresses taken over from different source files at KBO startup, street codes will not yet be included unless they were also present at the source. The source of these street codes is the municipalities themselves. The practical management of these codes is housed at the FPS Internal Affairs. KBO therefore starts from the street code table as made available by the FPS Internal Affairs.

- **NisCode** (string(5), optional): NIS code of the municipality where the entity's/establishment unit's/branch's address is located (only filled for Belgian municipalities)

- **BeStCode** (optional)
  - **Namespace** (string(255), required): Part of the BeSt key of an address
  - **ObjectId** (string(255), required): Part of the BeSt key of an address
  - **VersionId** (string(255), required): Part of the BeSt key of an address

- **AnomalyFileIndicator** (boolean, required): Indicates whether an anomaly file is ongoing for this address coding

- **TechnicalCreationReason** (string(3) [TechnicalCreationReasonType], optional): Code indicates technical reason for creation

- **TechnicalStopReason** (string(3) [TechnicalStopReasonType], optional): Code indicates technical reason for discontinuation

- **Validity** (ValidityPeriod, required)
  - **Begin** (DateType, required): Start date of the address
  - **End** (DateType, optional): End date of the address

#### Details (string(40), optional)
For addresses, a supplementary description can be given to identify the address, mainly for correspondence reasons.

Examples:
- Names of buildings: Vesalius, WTC building
- Names of industrial estates: Airway park

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the address
- **End** (DateType, optional): End date of the address

#### ExOfficioExecutions (optional)

- **ExOfficioExecution** (ExOfficioExecutionType, required): This structure contains ex officio actions performed on the address and/or on specific fields of the address
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the address, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.6 ContactInformationType

**ContactInformation** (ContactInformationType, optional): The contact data structure contains the entity's contact data.

Each ContactInformation contains 1 contact detail. The type determines what kind of contact detail it is: phone number, fax number, email address, or website.

Each entity and/or establishment can always have 0, one, or multiple (overlapping) contact details, even of the same type. It's also possible that the same contact detail appears multiple times but with a different validity period.

**Elements:**

#### Type (string(3) [ContactInformationType], required)
Indicates the type of contact data.

Examples:
- Phone number
- Fax number
- Website
- Email address

#### ContactData (string(254), required)
The contact data itself

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the address
- **End** (DateType, optional): End date of the address

---

### 5.7 JurFormType

**JurForm** (JurFormType, optional): The legal form data structure indicates the type of entity that can be established.

A natural person entity has no legal form.

**Elements:**

#### Code (string(3) [JuridicalFormCodes], required)
Provides the code of the legal form applicable to the legal entity or entity without legal personality

#### JurFormCAC (JurFormCACType, optional)
Contains the legal form as it should be read/understood pending the alignment of statutes with the Companies Code.

- **Code** (string(3) [JuridicalFormCodes], required): The code for the legal form as it should be read/understood pending alignment of statutes with the Companies Code
- **Date** (DateType, required): The start date of the legal form as it should be read/understood pending alignment of statutes with the Companies Code

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the legal form for the legal entity or entity without legal personality
- **End** (DateType, optional): End date of the legal form for the legal entity or entity without legal personality

#### ExOfficioExecutions (optional)

- **ExOfficioExecution** (ExOfficioExecutionType, required): This structure contains ex officio actions performed on the legal form and/or on specific fields of the legal form
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the legal form, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.8 JurSitType

**JurSit** (JurSitType, optional): The legal situation data structure indicates the legal situation of the entity at every moment in its lifecycle. Legal situations of an entity can change over time.

**Elements:**

#### Code (string(3) [JuridicalSituationCodes], required)
The code reflecting the entity's legal situation.

Examples: legal establishment, normal situation, closure of bankruptcy, etc.

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the relevant legal situation of the entity
- **End** (DateType, optional): End date of the relevant legal situation of the entity

#### Status (StatusType, required)
The status the entity has/had during the duration of the legal situation

#### Events (optional)
History of events of the legal situation

- **Event** (EventType, optional): This structure contains the events of the legal situation
  - **Code** (string(2) [EventCodes], required): Type of event
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the event
    - **End** (DateType, optional): End date of the event

#### Counter (integer(10), required)
Sequence number of the legal situation

---

### 5.9 FinancialDataType

**FinancialData** (FinancialDataType): The financial data structure contains the entity's financial data.

**Elements:**

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the financial data
- **End** (DateType, optional): End date of the financial data

#### FiscalYearEnd (optional)

- **Day** (long(2), optional): Day of the month in which the entity's financial year ends
- **Month** (long(2), optional): Month in which the entity's financial year ends

#### MonthAnnMeeting (long(2), optional)
Indicates the month in which the annual general meeting of partners/shareholders of the entity will take place

#### ExceptFiscalYear (optional)

- **Begin** (DateType, optional): Date of the beginning of the exceptional financial year
- **End** (DateType, optional): Date of the end of the exceptional financial year

---

### 5.10 FunctionType

**Function** (FunctionType, optional): The function data structure contains the possible functions that natural persons and/or entities perform in the entity.

Natural persons in their own name OR natural persons representing an entity OR another entity itself can perform functions in an entity.

These are persons authorized to represent the entity (e.g., director, curator, etc.)

**Elements:**

#### Code (string(5) [FunctionCodes], required)
The code of the function performed

#### FunctionCAC (FunctionCACType, optional)
Contains the function as it should be read according to the law introducing the Companies Code

- **Code** (string(5) [FunctionCodes], required): The function code
- **BeginDate** (DateType, required): Contains the date from which the function should be read differently, as provided in the law introducing the Companies Code
- **EndDate** (DateType, optional): Contains the date until which the function should be read differently, as provided in the law introducing the Companies Code

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the function in the entity
- **End** (DateType, optional): End date of the function in the entity

#### Counter (string(3), required)

#### EndReason (string(3) [StopReasonFunctionCodes], optional)
Reason for discontinuation of the function

#### HeldByPerson (optional)

- **Name** (string(48), required): Surname of the person performing the function in the entity. This data is only filled if it's a natural person performing the function in the entity.
- **FirstName** (string(50), optional): First name of the person performing the function in the entity. This data is only filled if it's a natural person performing the function in the entity.
- **PropertyManagementCompany** (EnterpriseNumberType, optional): When the property manager is a natural person who performs this activity professionally, the entity number of their natural person entity. Note: A natural person is a professional property manager when they are the founder of a natural person entity that has one or more establishments with a current NACEBEL code 68321 (Management of residential real estate for a fee or on a contract basis) or 68322 (Management of non-residential real estate for a fee or on a contract basis).

#### HeldByEnterprise (optional)

- **ExecutingEnterprise** (EnterpriseNumberType, required): The identification number of the entity performing the function ('technical key'). This indicates which entity performs the function. This data is filled if it's a legal entity performing the function.

#### ExOfficioExecutions (optional)

- **ExOfficioExecution** (ExOfficioExecutionType, required): This structure contains ex officio actions performed on the function and/or on specific fields of the function
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the function, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.11 ProfessionType

**Profession** (ProfessionType, optional): The professions data structure contains the possible entrepreneurial skills that natural persons have proven for the entity, as well as practitioners of ambulant activities and fairground operators.

These are entrepreneurial skills necessary for the exercise of certain activities.

**Elements:**

#### Code (string(5) [FunctionCodes], required)
The code of the professional competence performed

#### Exempted (string(1), optional)
Flag indicating whether an exemption was obtained for the entrepreneurial skill.
- 'N' (No): no exemption was obtained
- 'Y' (Yes): an exemption was obtained

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the entrepreneurial skill, practitioner of ambulant activity, or fairground operator
- **End** (DateType, optional): End date of the entrepreneurial skill, practitioner of ambulant activity, or fairground operator

#### Counter (string(3), required)

#### EndReason (string(3) [StopReasonFunctionCodes], optional)
Reason for discontinuation of the entrepreneurial skill, practitioner of ambulant activity, or fairground operator

#### HeldByPerson (required)

- **Name** (string(48), optional): Surname of the person who has proven the entrepreneurial skill, of the practitioner of ambulant activity, or fairground operator
- **FirstName** (string(50), optional): First name of the person who has proven the entrepreneurial skill, of the practitioner of ambulant activity, or fairground operator

#### ExOfficioExecutions (optional)

- **ExOfficioExecution** (ExOfficioExecutionType, required): This structure contains ex officio actions performed on the entrepreneurial skill and/or on specific fields thereof
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the entrepreneurial skill, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.12 WorkingPartnerType

**WorkingPartner** (WorkingPartnerType, optional): The WorkingPartner data structure contains the entity's working partners. These are partners who personally perform a real and regular activity within the entity without being in a subordinate relationship to that entity and with the aim of making capital that fully or partially belongs to them productive.

**Elements:**

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the working partner
- **End** (DateType, optional): End date of the working partner

#### Counter (string(3), required)

#### EndReason (string(3) [StopReasonFunctionCodes], optional)
Reason for discontinuation of the working partner

#### HeldByPerson (required)

- **Name** (string(48), optional): Surname of the working partner
- **FirstName** (string(50), optional): First name of the working partner

#### ExOfficioExecutions (optional)

- **ExOfficioExecution** (ExOfficioExecutionType, required): This structure contains ex officio actions performed on the working partner and/or on specific fields thereof
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the working partner, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.13 ActivityType

**Activity** (ActivityType, optional): The activity data structure contains an overview of the entity's activities (NACE codes).

Since there is no uniformity in the use of activity codes at the federal government level, activity codes are split per instrumenting administration. This means that different instrumenting administrations can use their specific activity codes within KBO.

**Elements:**

#### ParentEnterprise (EnterpriseNumberType, optional)
In case an establishment's activity is described, this contains the entity number ('technical key') of the entity to which the establishment unit belongs

#### Nacebel (string(7) [NacebelCodes], required)
The NACEBEL code of the activity

#### Type (string(1) [TypeOfActivityCodes], required)
Indicates whether the activity is a main activity, secondary activity, or auxiliary activity of the entity.

Possible values:
- 0 = secondary activity
- 1 = main activity
- 2 = auxiliary activity

#### Version (string(4), required)
Version of the NACE nomenclature in which the activity is coded: '2003' or '2008'

#### ActivityGroup (string(20) [ActivityGroupCodes], required)
Type of activity code

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the activity
- **End** (DateType, optional): End date of the activity

#### ExOfficioExecutions (optional)

- **ExOfficioExecution** (ExOfficioExecutionType, required): This structure contains ex officio actions performed on the activity and/or on specific fields of the activity
  - **Field** (string(3) [FieldCodes], optional): If an ex officio action relates to a field of the activity, this refers to the field
  - **Action** (string(3) [ExOfficioActionCodes], required): Indicates whether it's an ex officio deletion, registration, or modification
  - **Reason** (string(3) [ReasonExOfficioActionCodes], required): Indicates the reason for registering the ex officio action
  - **Validity** (ValidityPeriod, required)
    - **Begin** (DateType, required): Start date of the ex officio action
    - **End** (DateType, optional): End date of the ex officio action

---

### 5.14 BankAccountType

**BankAccount** (BankAccountType, optional): The bank account data structure contains the entity's bank account numbers.

Foreign account numbers are also stored. The regular format for Belgian bank account numbers as well as the IBAN format for Belgian and foreign account numbers (used by banks for international payments) are maintained.

**Elements:**

#### UsagePurposeCode (string(3) [BankAccountCodes], required)
The bank account number is used for specific purposes. This is indicated by the usage purpose.

#### Counter (string(6), required)

#### Number (string(12), optional)
The Belgian bank account number of the entity. For domestic payments, the local account number should always be used.

#### Iban (string(34), optional)
The IBAN (International Bank Account Number) format of the bank account number. This format is used by banks for cross-border payments within the European Union.

#### Bic (string(11), optional)
Bank Identifier Code. This is a code used to identify a bank for cross-border payments.

#### NonSepaAccount (string(34), optional)
Bank account from outside the SEPA zone (Single European Payment Area)

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the bank account
- **End** (DateType, optional): End date of the bank account

---

### 5.15 PermissionType

**Permission** (PermissionType, optional): The permission data structure contains the various permissions granted to the entity.

By permissions, we mean recognitions, licenses, etc., that can be issued with the intention of being allowed to perform certain activities.

**Elements:**

#### ParentEnterprise (EnterpriseNumberType, optional)
In case a permission of an establishment is described, this contains the entity number ('technical key') of the entity to which the establishment unit belongs

#### Code (string(5) [PermissionCodes], required)
Identification code of the permission applicable to the entity

#### PhaseCode (string(3) [PermissionPhaseCodes], required)
Code of the phase in which the permission is located: specifies in which phase the file is with the instrumenting authority

#### Duration (decimal(3) with 1 decimal place, optional)
Indicates how long the permission is valid if the permission has a limited duration. For permissions with a permanent character, this data is not filled in. The duration can be specified in years and half years.

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the permission effectively acquired by / assigned to the entity. When at the time of registration it is not known when the permission is effectively granted to the entity, this data equals '01.01.0001'.
- **End** (DateType, optional): End date of the permission

#### EndReason (string(3) [StopReasonPermissionCodes], optional)
Reason for discontinuation of the permission

---

### 5.16 AuthorizationType

**Authorization** (AuthorizationType, optional): The authorization data structure contains the various authorizations granted by the administration to the entity.

These are authorizations under which the entity is known, such as 'VAT liable', 'Employer'.

The phase indicates the stage of the authorization, such as 'in application', 'refused', 'granted', etc.

**Elements:**

#### Code (string(5) [PermissionCodes], required)
Identification code of the authorization applicable to the entity

#### PhaseCode (string(3) [PermissionPhaseCodes], required)
Code of the phase in which the authorization is located: specifies in which phase the file is with the instrumenting authority

#### Duration (decimal(3) with 1 decimal place, optional)
Indicates how long the authorization is valid if it has a limited duration. For authorizations with a permanent character, this data is not filled in. The duration can be specified in years and half years.

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the authorization effectively acquired by / assigned to the entity. When at the time of registration it is not known when the authorization is effectively granted to the entity, this data equals '01.01.0001'.
- **End** (DateType, optional): End date of the authorization

#### EndReason (string(3) [StopReasonPermissionCodes], optional)
Reason for discontinuation of the authorization

---

### 5.17 ForeignIdentificationType

**ForeignIdentification** (ForeignIdentificationType, optional): The foreign identification data structure contains information about the foreign entity.

**Elements:**

#### CountryCode (String, required)
ISO country code of the foreign register

#### RegistryCode (String, required)
Foreign register code

#### RegistryName (String, required)
Name of foreign register

#### RegistryEntityNumber (String, required)
Identification in the foreign register

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the foreign identification
- **End** (DateType, optional): End date of the foreign identification

---

### 5.18 BranchType

**Branch** (BranchType, optional): This structure contains the data of a branch.

**Elements:**

#### Id (required)
Identification number of the branch

#### Addresses (required)
See AddressType

#### Denominations (optional)
See DenominationType

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the branch
- **End** (DateType, optional): End date of the branch

#### EndReason (string(3) [StopReasonEnterpriseCodes], optional)
Reason for discontinuation of the branch

---

### 5.19 LinkedEnterpriseType

**LinkedEnterprise** (LinkedEnterpriseType, optional): This structure contains the relationships between the entity and other entities and between the entity and its establishment units.

For the different types of links, it must be agreed who takes on the 'parent' role and who takes on the 'child' role.

General principle:
- The 'parent' is the one who is the cause of the action
- The 'child' is the result of the action

Examples:

1. **Link natural/legal entity - establishment unit:**
   - Parent relationship to child: natural/legal entity establishes establishment units
   - Child relationship: establishment units are established by natural/legal entities

2. **Link absorption:**
   - Parent relationship to child: legal entity being absorbed
   - Child relationship to parent: legal entity absorbing

**Elements:**

#### Child (EnterpriseNumberType, required)
The entity number ('Technical Key') or establishment unit number that fulfills the CHILD role in the link between entities/establishment units

#### LinkType (string(3) [LinkEnterpriseCodes], required)
Code of the relationship between two entities or between an entity and an establishment unit

#### Parent (EnterpriseNumberType, required)
The entity number ('Technical Key') or establishment unit number that fulfills the PARENT role in the link between entities/establishment units

#### EndReason (string(3) [StopReasonLinkedEnterpriseCodes], optional)
Reason for discontinuation of the relationship between two entities or between an entity and an establishment unit

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the relationship between the entities/establishment units
- **End** (DateType, optional): End date of the link between the entities/establishment units

---

### 5.20 ExOfficioExecutionType

**ExOfficioExecution** (ExOfficioExecutionType, optional): This structure contains ex officio actions performed on the entity/establishment unit.

**Elements:**

#### Field (string(3) [FieldCodes], optional)
If an ex officio action relates to a field, this refers to the field

#### Action (string(3) [ExOfficioActionCodes], required)
Indicates whether it's an ex officio deletion, registration, or modification

#### Reason (string(3) [ReasonExOfficioActionCodes], required)
Indicates the reason for registering the ex officio action

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the ex officio action
- **End** (DateType, optional): End date of the ex officio action

---

### 5.21 BusinessUnitType

**BusinessUnit** (BusinessUnitType): The establishment unit data structure groups data concerning the entity's establishment units. An establishment unit in KBO is a geographical place where or from which at least one activity is performed by entities.

These geographical places are identifiable by an address, located in Belgium.

Establishment units have neither a legal form nor a legal situation.

Each establishment unit is uniquely identified by an establishment unit number.

**Elements:**

#### Number (EnterpriseNumberType, required)
This identification number, called the establishment unit number, is a unique number assigned to an establishment unit. This identification is a mere sequence number containing no meaningful information.

**Internal storage structure:**
- Position 1-8: Sequence number
- Position 9-10: Check digit (97 - modulo 97)

**Presentation format on user interfaces and documents:** 9.999.999.999

#### Validity (ValidityPeriod, required)

- **Begin** (DateType, required): Start date of the establishment unit
- **End** (DateType, optional): End date of the establishment unit, i.e., the date on which all activities of an establishment unit have been discontinued

#### EndReason (string(3) [StopReasonEnterpriseCodes], optional)
Reason for discontinuation of the establishment unit

#### Status (StatusType, required)
Indicates at what stage in its lifecycle the establishment unit is.

Possible statuses:
- Active (AC)
- Discontinued (ST)
- Canceled (AN)

#### Denominations (required)
See DenominationType

#### Addresses (optional)
See AddressType

#### ContactInformations (optional)
See ContactInformationType

#### Activities (optional)
See ActivityType

#### Functions (optional)
See FunctionType

#### Permissions (optional)
See PermissionType

#### LinkedEnterprises (optional)
See LinkedEnterpriseType

#### ExOfficioExecutions (optional)
See ExOfficioExecutionType

---

## 6. Annex 2: XML Field Descriptions in the Code File {#annex-2-xml-field-descriptions-code}

| Code Table | Description |
|-----------|-------------|
| ActivityGroupCodes | Coding for the type of activity |
| AddressTypeCodes | Coding for the type of address (registered office address, branch, establishment unit address) |
| AddressFormatCodes | Coding of the format of address details |
| AddressStrikeCodes | Coding for the type of address deletion |
| BankAccountCodes | Coding for the usage purpose of a bank account |
| ContactInformationType | Coding of the type of contact information (phone number, fax number, website, email address) |
| CountryCodes | Coding for countries: the code indicates the country of an address |
| CurrencyCodes | Coding for currency in which the capital is expressed |
| DenominationCodes | Coding for type of denomination (name, abbreviation, trade name) |
| EventCodes | Coding for the type of event registered on a legal situation |
| ExemptedCodes | Coding for the type of exemption |
| ExOfficioActionCodes | Coding for the type of ex officio action |
| FieldCodes | Coding for the field of the ex officio action |
| FunctionCodes | Coding for functions of an entity: two types of functions are included here: legal functions and entrepreneurial skills, practitioner of ambulant activity, fairground operator |
| JuridicalFormCodes | Coding of the legal form of a legal entity / entity without legal personality. These legal forms are used for both legal entities with legal personality and those without |
| JuridicalSituationCodes | Coding for the legal situations of an entity |
| LanguageCodes | Coding of the languages used in KBO for denominations and addresses |
| LinkedEnterpriseCodes | Coding of types of links between entities and links between entities and establishment units |
| NacebelCodes | Activity nomenclature according to the standard of NACEBEL codes (a classification system for economic activities) |
| PermissionCodes | Coding for permissions and authorizations |
| PermissionPhaseCodes | Coding of the phase in which a permission or authorization is located |
| ReasonExOfficioActionCodes | Coding for the reason for the ex officio action |
| StatusCodes | Coding for the status of an entity or establishment unit. This status indicates at what stage in its lifecycle an entity or establishment unit is located |
| StopReasonEnterpriseCodes | Coding for the type of discontinuation of an establishment unit |
| StopReasonFunctionCodes | Coding for the type of discontinuation of a function or entrepreneurial skills, practitioner of ambulant activity, fairground operator |
| StopReasonPermissionCodes | Coding for the type of discontinuation of a permission or authorization |
| StopReasonLinkedEnterpriseCodes | Coding for the type of discontinuation of a relationship between two entities |
| TechnicalCreationReasonCode | Coding for the technical creation of address coding |
| TechnicalStopReasonCode | Coding for the technical discontinuation of address coding |
| TypeOfActivityCodes | Coding for the type of activity (main activity, secondary activity, auxiliary activity) |
| TypeOfEnterpriseCodes | Coding for the type of entity (natural person, legal entity) |

---

## 7. Annex 3: Processing the Reuse File {#annex-3-processing}

This annex suggests a method for processing the reuse files. First, all entities and establishments from the full reuse file are loaded into a database. The database structure is a reflection of the structure of the data in the reuse file. Then it describes how the database can be maintained using the change reuse files in a JAVA environment.

### 7.1 Initial Load {#initial-load}

This is done using the full reuse file.

#### 7.1.1 Process

```
Create DB → Bulk Insert Data → Add Indexes → Add Foreign Keys
```

#### 7.1.2 Create DB

- Create only the tables you need. The less data, the faster the loading will be.
- Don't use foreign keys at this point. We're going to bulk load the data and it will only be referentially correct at the end of loading.
- Don't create indexes yet. These must be continuously recalculated while loading occurs.

#### 7.1.3 Bulk Insert Data

Loading the data in the full reuse file must be done in bulk. This means we're going to bundle insert statements and send them as 1 transaction to the database.

Bulk insert statements can be easily built as follows:

```sql
INSERT INTO [table]
( [field1], [field2], [field3] ) VALUES
( '[value1.1]', '[value1.2]', '[value1.3]' ),
( '[value2.1]', '[value2.2]', '[value2.3]' ),
( '[value3.1]', '[value3.2]', '[value3.3]' ),
etc.
```

**Architecture:**

```
ZIP File → Parser → Queues (multiple) → Database
```

The parser is a small application that can read the delivered Full Zip as a Stream. Reading the zip directly saves the extra step of unzipping. Since the load is mainly database-bounded, the overhead is negligible.

*Java provides this with the ZipInputStream*

Since the file is far too large to take into memory (a full reuse file is approximately 28 gigabytes), use of an XML Stream reader is required to process this. Streamers only take a small portion of the file at a time, so they don't need much memory.

*Java provides this with the standard (since java 1.6) Stax libraries*

The parser generates entities and sends them to the different queues per entity. Behind each queue is a worker that, for example, bundles 1000 insert statements and only then sends them to the database.

Be careful: The Parser must not work too fast, so that the queues cannot keep up. It must throttle as soon as some queues cannot keep up (and are already 100,000 entities behind, for example).

*Java makes this very easy to build through the java.util.concurrent standard classes*

#### 7.1.4 Add Indexes

Indexes must at least be added on the ENTERPRISE.NR column and all FK_ENT_NR (foreign key) columns.

This is to avoid full table scans when foreign key constraints are added.

Since there are tables with many rows, it is recommended to place the necessary indexes on all necessary tables.

Calculating the indexes can take a while.

#### 7.1.5 Add Foreign Keys

After all data is loaded and indexes are added, foreign keys can be added.

### 7.2 Maintaining Data in the Database

This is done using the change reuse file.

```
Step 1: Canceled Establishment Units → 
Step 2: Canceled Entities → 
Step 3: New and Modified Entities → 
Step 4: New and Modified Establishment Units
```

#### Step 1: Process Canceled Establishment Units

In the change reuse file, establishment units deleted from KBO can be found under the `CancelledBusinessUnits` tag. The establishment units listed here must be removed from the database, along with all their data groups.

#### Step 2: Process Canceled Entities

In the change reuse file, entities deleted from KBO can be found under the `CancelledEnterprises` tag. The entities listed here must be removed from the database, along with all their data groups.

#### Step 3: New and Modified Entities

Under the `Enterprises` tags, modified and new entities can be found. In this case, first check whether the entity is already included in the database. If the entity is found, it's a modified entity; otherwise, it's a new one.

Entities in which something has been changed at the entity level or somewhere in their data groups will be present in their entirety in the change extract. This means that users can take over the entity in its entirety by:
- First deleting the existing data and then inserting the new data, OR
- Comparing and updating the data. Data that is no longer present, e.g., a missing address, has been canceled in KBO and must therefore be removed from the database.

New entities should be inserted into the database.

#### Step 4: New and Modified Establishment Units

Under the `BusinessUnits` tags, modified and new establishment units can be found. In this case, first check whether the establishment unit is already included in the database. If the establishment unit is found, it's a modified establishment unit; otherwise, it's a new one.

Establishment units in which something has been changed at the establishment level or somewhere in their data groups will be present in their entirety in the change extract.

Users should take over the establishment unit in its entirety by:
- First deleting the existing data and then inserting the new data, OR
- Comparing and updating the data. Data that is no longer present, e.g., a missing address, has been canceled in KBO and must therefore be removed from the database.

New establishment units should be added to the database.

---

**End of Document**