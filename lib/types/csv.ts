/**
 * CSV file structure types (raw data from KBO Open Data)
 */

// Raw CSV types (before transformation)

export interface RawEnterprise {
  EnterpriseNumber: string
  Status: string
  JuridicalSituation: string
  TypeOfEnterprise: string
  JuridicalForm: string
  JuridicalFormCAC: string
  StartDate: string // dd-mm-yyyy format
}

export interface RawEstablishment {
  EstablishmentNumber: string
  StartDate: string
  EnterpriseNumber: string
}

export interface RawDenomination {
  EntityNumber: string
  Language: string // 0-4
  TypeOfDenomination: string // 001-004
  Denomination: string
}

export interface RawAddress {
  EntityNumber: string
  TypeOfAddress: string // REGO, BAET, ABBR, OBAD
  CountryNL: string
  CountryFR: string
  Zipcode: string
  MunicipalityNL: string
  MunicipalityFR: string
  StreetNL: string
  StreetFR: string
  HouseNumber: string
  Box: string
  ExtraAddressInfo: string
  DateStrikingOff: string
}

export interface RawActivity {
  EntityNumber: string
  ActivityGroup: string
  NaceVersion: string
  NaceCode: string
  Classification: string
}

export interface RawContact {
  EntityNumber: string
  ContactType: string
  ContactValue: string
}

export interface RawBranch {
  Id: string
  StartDate: string
  EnterpriseNumber: string
  Denomination: string
  StreetNL: string
  StreetFR: string
  HouseNumber: string
  Box: string
  Zipcode: string
  MunicipalityNL: string
  MunicipalityFR: string
}

export interface RawCode {
  Category: string
  Code: string
  Language: string
  Description: string
}

export interface RawMeta {
  SnapshotDate: string // dd-mm-yyyy
  ExtractTimestamp: string // dd-mm-yyyy HH:MM:SS
  ExtractNumber: string // numeric
  ExtractType: string // 'Full' or 'Update'
  Version: string
}
