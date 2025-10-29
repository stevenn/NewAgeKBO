"use strict";
/**
 * Shared column mapping utilities
 * Maps between KBO CSV column names (PascalCase) and database column names (snake_case)
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.CSV_TO_DB_TABLE_NAMES = void 0;
exports.csvColumnToDbColumn = csvColumnToDbColumn;
exports.csvTableToDbTable = csvTableToDbTable;
exports.computeEntityType = computeEntityType;
exports.convertKboDateFormat = convertKboDateFormat;
exports.isKboDateFormat = isKboDateFormat;
/**
 * Special case column mappings where simple snake_case conversion doesn't work
 */
var SPECIAL_COLUMN_MAPPINGS = {
    // Address columns with language suffixes
    'CountryNL': 'country_nl',
    'CountryFR': 'country_fr',
    'MunicipalityNL': 'municipality_nl',
    'MunicipalityFR': 'municipality_fr',
    'StreetNL': 'street_nl',
    'StreetFR': 'street_fr',
    // Denomination columns
    'TypeOfDenomination': 'denomination_type',
    'Denomination': 'denomination',
    // Enterprise/Establishment columns
    'JuridicalFormCAC': 'juridical_form_cac',
    // Contact columns
    'EntityContact': 'entity_contact',
    'ContactType': 'contact_type',
    'Value': 'contact_value',
    // Common columns
    'EntityNumber': 'entity_number',
    'EnterpriseNumber': 'enterprise_number',
    'EstablishmentNumber': 'establishment_number',
    'ActivityGroup': 'activity_group',
    'NaceVersion': 'nace_version',
    'NaceCode': 'nace_code',
    'Classification': 'classification',
    'TypeOfAddress': 'type_of_address',
    'StartDate': 'start_date',
    'Status': 'status',
    'JuridicalSituation': 'juridical_situation',
    'TypeOfEnterprise': 'type_of_enterprise',
    'JuridicalForm': 'juridical_form',
    'HouseNumber': 'house_number',
    'Box': 'box',
    'ExtraAddressInfo': 'extra_address_info',
    'DateStrikingOff': 'date_striking_off',
    'Zipcode': 'zipcode',
    'Language': 'language',
    'Id': 'id'
};
/**
 * Table name mappings: CSV file names (singular) to database table names (plural)
 */
exports.CSV_TO_DB_TABLE_NAMES = {
    'activity': 'activities',
    'address': 'addresses',
    'contact': 'contacts',
    'denomination': 'denominations',
    'enterprise': 'enterprises',
    'establishment': 'establishments',
    'branch': 'branches',
    'code': 'codes'
};
/**
 * Convert PascalCase column name to snake_case database column name
 */
function csvColumnToDbColumn(csvColumn) {
    // Check special cases first
    if (SPECIAL_COLUMN_MAPPINGS[csvColumn]) {
        return SPECIAL_COLUMN_MAPPINGS[csvColumn];
    }
    // Default: simple snake_case conversion
    return csvColumn
        .replace(/([A-Z])/g, '_$1')
        .toLowerCase()
        .replace(/^_/, '');
}
/**
 * Convert CSV table name to database table name
 */
function csvTableToDbTable(csvTable) {
    return exports.CSV_TO_DB_TABLE_NAMES[csvTable] || csvTable;
}
/**
 * Compute entity_type from entity number
 * Establishments start with "2.", everything else is enterprise
 */
function computeEntityType(entityNumber) {
    return entityNumber.startsWith('2.') ? 'establishment' : 'enterprise';
}
/**
 * Convert DD-MM-YYYY date format to YYYY-MM-DD
 */
function convertKboDateFormat(ddmmyyyy) {
    var _a = ddmmyyyy.split('-'), day = _a[0], month = _a[1], year = _a[2];
    return "".concat(year, "-").concat(month, "-").concat(day);
}
/**
 * Check if a value looks like a KBO date (DD-MM-YYYY format)
 */
function isKboDateFormat(value) {
    return /^\d{2}-\d{2}-\d{4}$/.test(value);
}
