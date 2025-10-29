#!/usr/bin/env tsx
"use strict";
/**
 * Apply daily KBO update from ZIP file
 * Purpose: Process incremental updates using delete-then-insert pattern
 *
 * Strategy:
 * - Process ZIP files directly (no extraction)
 * - DELETE operations: Mark records as _is_current = false (preserve history)
 * - INSERT operations: Add new records with _is_current = true
 * - Update _extract_number and _snapshot_date from meta.csv
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
var dotenv_1 = require("dotenv");
(0, dotenv_1.config)({ path: ['.env.local', '.env'] });
var node_stream_zip_1 = require("node-stream-zip");
var sync_1 = require("csv-parse/sync");
var path = require("path");
var crypto_1 = require("crypto");
var motherduck_1 = require("../lib/motherduck");
var column_mapping_1 = require("../lib/utils/column-mapping");
/**
 * Generate a short hash for a string (8 characters)
 * Used to create unique IDs for denominations
 */
function shortHash(text) {
    return (0, crypto_1.createHash)('sha256').update(text).digest('hex').substring(0, 8);
}
/**
 * Parse metadata from meta.csv in ZIP
 */
function parseMetadata(zip) {
    return __awaiter(this, void 0, void 0, function () {
        var metaContent, metaRecords;
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k;
        return __generator(this, function (_l) {
            switch (_l.label) {
                case 0: return [4 /*yield*/, zip.entryData('meta.csv')];
                case 1:
                    metaContent = _l.sent();
                    metaRecords = (0, sync_1.parse)(metaContent.toString(), {
                        columns: true,
                        skip_empty_lines: true
                    });
                    return [2 /*return*/, {
                            SnapshotDate: ((_a = metaRecords.find(function (r) { return r.Variable === 'SnapshotDate'; })) === null || _a === void 0 ? void 0 : _a.Value) || ((_b = metaRecords[0]) === null || _b === void 0 ? void 0 : _b.Value) || '',
                            ExtractTimestamp: ((_c = metaRecords.find(function (r) { return r.Variable === 'ExtractTimestamp'; })) === null || _c === void 0 ? void 0 : _c.Value) || ((_d = metaRecords[1]) === null || _d === void 0 ? void 0 : _d.Value) || '',
                            ExtractType: ((_e = metaRecords.find(function (r) { return r.Variable === 'ExtractType'; })) === null || _e === void 0 ? void 0 : _e.Value) || ((_f = metaRecords[2]) === null || _f === void 0 ? void 0 : _f.Value) || '',
                            ExtractNumber: ((_g = metaRecords.find(function (r) { return r.Variable === 'ExtractNumber'; })) === null || _g === void 0 ? void 0 : _g.Value) || ((_h = metaRecords[3]) === null || _h === void 0 ? void 0 : _h.Value) || '',
                            Version: ((_j = metaRecords.find(function (r) { return r.Variable === 'Version'; })) === null || _j === void 0 ? void 0 : _j.Value) || ((_k = metaRecords[4]) === null || _k === void 0 ? void 0 : _k.Value) || ''
                        }];
            }
        });
    });
}
// Removed - now using shared library from lib/utils/column-mapping
/**
 * Apply delete operations (mark as historical, don't actually delete)
 */
function applyDeletes(db, zip, csvTableName, dbTableName, metadata) {
    return __awaiter(this, void 0, void 0, function () {
        var fileName, content, records, csvPkColumn_1, dbPkColumn, entityNumbers, extractNumber, sql, error_1;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    fileName = "".concat(csvTableName, "_delete.csv");
                    _b.label = 1;
                case 1:
                    _b.trys.push([1, 4, , 5]);
                    return [4 /*yield*/, zip.entryData(fileName)];
                case 2:
                    content = _b.sent();
                    records = (0, sync_1.parse)(content.toString(), {
                        columns: true,
                        skip_empty_lines: true
                    });
                    if (records.length === 0) {
                        console.log("   \u2139\uFE0F  ".concat(dbTableName, ": No deletes"));
                        return [2 /*return*/, 0];
                    }
                    csvPkColumn_1 = Object.keys(records[0])[0];
                    dbPkColumn = (0, column_mapping_1.csvColumnToDbColumn)(csvPkColumn_1);
                    entityNumbers = records.map(function (r) { return "'".concat(r[csvPkColumn_1], "'"); }).join(',');
                    extractNumber = parseInt(metadata.ExtractNumber);
                    sql = "\n      UPDATE ".concat(dbTableName, "\n      SET _is_current = false,\n          _deleted_at_extract = ").concat(extractNumber, "\n      WHERE ").concat(dbPkColumn, " IN (").concat(entityNumbers, ")\n        AND _is_current = true\n    ");
                    return [4 /*yield*/, (0, motherduck_1.executeStatement)(db, sql)];
                case 3:
                    _b.sent();
                    console.log("   \u2713 ".concat(dbTableName, ": Marked ").concat(records.length, " records as historical"));
                    return [2 /*return*/, records.length];
                case 4:
                    error_1 = _b.sent();
                    if ((_a = error_1.message) === null || _a === void 0 ? void 0 : _a.includes('Entry not found')) {
                        console.log("   \u2139\uFE0F  ".concat(dbTableName, ": No delete file"));
                        return [2 /*return*/, 0];
                    }
                    throw error_1;
                case 5: return [2 /*return*/];
            }
        });
    });
}
/**
 * Apply insert operations
 */
function applyInserts(db, zip, csvTableName, dbTableName, metadata) {
    return __awaiter(this, void 0, void 0, function () {
        var fileName, content, records, uniqueRecords, seen, _i, records_1, record, key, enterpriseNames_1, enterpriseNumbers, existingRecords, _a, existingRecords_1, rec, snapshotDate_1, extractNumber_1, csvColumns_1, dbColumns, needsEntityType_1, needsComputedId_1, allColumns, values, sql, error_2;
        var _b;
        return __generator(this, function (_c) {
            switch (_c.label) {
                case 0:
                    fileName = "".concat(csvTableName, "_insert.csv");
                    _c.label = 1;
                case 1:
                    _c.trys.push([1, 6, , 7]);
                    return [4 /*yield*/, zip.entryData(fileName)];
                case 2:
                    content = _c.sent();
                    records = (0, sync_1.parse)(content.toString(), {
                        columns: true,
                        skip_empty_lines: true
                    });
                    if (records.length === 0) {
                        console.log("   \u2139\uFE0F  ".concat(dbTableName, ": No inserts"));
                        return [2 /*return*/, 0];
                    }
                    uniqueRecords = [];
                    seen = new Set();
                    for (_i = 0, records_1 = records; _i < records_1.length; _i++) {
                        record = records_1[_i];
                        key = JSON.stringify(record);
                        if (!seen.has(key)) {
                            seen.add(key);
                            uniqueRecords.push(record);
                        }
                    }
                    if (uniqueRecords.length < records.length) {
                        console.log("   \u26A0\uFE0F  ".concat(dbTableName, ": Removed ").concat(records.length - uniqueRecords.length, " duplicate records from insert file"));
                    }
                    enterpriseNames_1 = new Map();
                    if (!(dbTableName === 'enterprises')) return [3 /*break*/, 4];
                    enterpriseNumbers = uniqueRecords.map(function (r) { return "'".concat(r['EnterpriseNumber'], "'"); }).join(',');
                    return [4 /*yield*/, (0, motherduck_1.executeQuery)(db, "\n        SELECT enterprise_number, primary_name, primary_name_language,\n               primary_name_nl, primary_name_fr, primary_name_de\n        FROM enterprises\n        WHERE enterprise_number IN (".concat(enterpriseNumbers, ")\n          AND _is_current = false\n        ORDER BY _snapshot_date DESC, _extract_number DESC\n      "))];
                case 3:
                    existingRecords = _c.sent();
                    for (_a = 0, existingRecords_1 = existingRecords; _a < existingRecords_1.length; _a++) {
                        rec = existingRecords_1[_a];
                        if (!enterpriseNames_1.has(rec.enterprise_number)) {
                            enterpriseNames_1.set(rec.enterprise_number, rec);
                        }
                    }
                    _c.label = 4;
                case 4:
                    snapshotDate_1 = (0, column_mapping_1.convertKboDateFormat)(metadata.SnapshotDate);
                    extractNumber_1 = parseInt(metadata.ExtractNumber);
                    csvColumns_1 = Object.keys(uniqueRecords[0]);
                    dbColumns = csvColumns_1.map(function (col) { return (0, column_mapping_1.csvColumnToDbColumn)(col); });
                    needsEntityType_1 = ['activities', 'addresses', 'contacts', 'denominations'].includes(dbTableName);
                    needsComputedId_1 = ['activities', 'addresses', 'contacts', 'denominations'].includes(dbTableName);
                    allColumns = void 0;
                    if (needsComputedId_1) {
                        // For tables with computed IDs, we need: id, _snapshot_date, _extract_number, then all CSV columns, entity_type, _is_current
                        allColumns = __spreadArray(__spreadArray(['id', '_snapshot_date', '_extract_number'], dbColumns, true), ['entity_type', '_is_current'], false);
                    }
                    else if (needsEntityType_1) {
                        allColumns = __spreadArray(__spreadArray([], dbColumns, true), ['entity_type', '_snapshot_date', '_extract_number', '_is_current'], false);
                    }
                    else {
                        allColumns = __spreadArray(__spreadArray([], dbColumns, true), ['_snapshot_date', '_extract_number', '_is_current'], false);
                    }
                    values = uniqueRecords.map(function (record) {
                        // Compute ID for tables that need it
                        var computedId = null;
                        if (needsComputedId_1) {
                            var entityNumber = record['EntityNumber'] || record[csvColumns_1[0]];
                            if (dbTableName === 'activities') {
                                // id: entity_number_group_version_code_classification
                                computedId = "".concat(entityNumber, "_").concat(record['ActivityGroup'], "_").concat(record['NaceVersion'], "_").concat(record['NaceCode'], "_").concat(record['Classification']);
                            }
                            else if (dbTableName === 'addresses') {
                                // id: entity_number_type_of_address
                                computedId = "".concat(entityNumber, "_").concat(record['TypeOfAddress']);
                            }
                            else if (dbTableName === 'contacts') {
                                // id: entity_number_entity_contact_contact_type_value
                                computedId = "".concat(entityNumber, "_").concat(record['EntityContact'], "_").concat(record['ContactType'], "_").concat(record['Value']);
                            }
                            else if (dbTableName === 'denominations') {
                                // id: entity_number_type_language_hash(denomination)
                                // Hash ensures uniqueness even if multiple denominations exist for same entity+type+language
                                var denominationHash = shortHash(record['Denomination'] || '');
                                computedId = "".concat(entityNumber, "_").concat(record['TypeOfDenomination'], "_").concat(record['Language'], "_").concat(denominationHash);
                            }
                        }
                        var recordValues = csvColumns_1.map(function (col) {
                            var val = record[col];
                            if (val === '' || val === null) {
                                return 'NULL';
                            }
                            // Check if this looks like a date (DD-MM-YYYY format)
                            if (col.toLowerCase().includes('date') && (0, column_mapping_1.isKboDateFormat)(val)) {
                                var converted = (0, column_mapping_1.convertKboDateFormat)(val);
                                return "'".concat(converted, "'");
                            }
                            // Escape single quotes
                            return "'".concat(val.replace(/'/g, "''"), "'");
                        });
                        // Build values string
                        if (needsComputedId_1) {
                            var entityNumber = record['EntityNumber'] || record[csvColumns_1[0]];
                            var entityType = (0, column_mapping_1.computeEntityType)(entityNumber);
                            // Order: id, _snapshot_date, _extract_number, ...all CSV columns..., entity_type, _is_current
                            return "('".concat(computedId, "', '").concat(snapshotDate_1, "', ").concat(extractNumber_1, ", ").concat(recordValues.join(','), ", '").concat(entityType, "', true)");
                        }
                        else if (needsEntityType_1) {
                            var entityNumber = record['EntityNumber'] || record[csvColumns_1[0]];
                            var entityType = (0, column_mapping_1.computeEntityType)(entityNumber);
                            return "(".concat(recordValues.join(','), ", '").concat(entityType, "', '").concat(snapshotDate_1, "', ").concat(extractNumber_1, ", true)");
                        }
                        // For enterprises, add primary_name fields from existing record
                        if (dbTableName === 'enterprises') {
                            var enterpriseNumber = record['EnterpriseNumber'];
                            var existing = enterpriseNames_1.get(enterpriseNumber);
                            if (existing) {
                                var primaryName = existing.primary_name ? "'".concat(existing.primary_name.replace(/'/g, "''"), "'") : "'".concat(enterpriseNumber, "'");
                                var primaryNameLang = existing.primary_name_language ? "'".concat(existing.primary_name_language, "'") : 'NULL';
                                var primaryNameNl = existing.primary_name_nl ? "'".concat(existing.primary_name_nl.replace(/'/g, "''"), "'") : 'NULL';
                                var primaryNameFr = existing.primary_name_fr ? "'".concat(existing.primary_name_fr.replace(/'/g, "''"), "'") : 'NULL';
                                var primaryNameDe = existing.primary_name_de ? "'".concat(existing.primary_name_de.replace(/'/g, "''"), "'") : 'NULL';
                                return "(".concat(recordValues.join(','), ", ").concat(primaryName, ", ").concat(primaryNameLang, ", ").concat(primaryNameNl, ", ").concat(primaryNameFr, ", ").concat(primaryNameDe, ", '").concat(snapshotDate_1, "', ").concat(extractNumber_1, ", true)");
                            }
                            else {
                                // New enterprise - use enterprise number as primary name
                                return "(".concat(recordValues.join(','), ", '").concat(enterpriseNumber, "', NULL, NULL, NULL, NULL, '").concat(snapshotDate_1, "', ").concat(extractNumber_1, ", true)");
                            }
                        }
                        return "(".concat(recordValues.join(','), ", '").concat(snapshotDate_1, "', ").concat(extractNumber_1, ", true)");
                    }).join(',\n      ');
                    // Update column list for enterprises to include primary_name fields
                    if (dbTableName === 'enterprises') {
                        allColumns = __spreadArray(__spreadArray([], dbColumns, true), ['primary_name', 'primary_name_language', 'primary_name_nl', 'primary_name_fr', 'primary_name_de', '_snapshot_date', '_extract_number', '_is_current'], false);
                    }
                    sql = "\n      INSERT INTO ".concat(dbTableName, " (").concat(allColumns.join(', '), ")\n      VALUES\n      ").concat(values, "\n    ");
                    return [4 /*yield*/, (0, motherduck_1.executeStatement)(db, sql)];
                case 5:
                    _c.sent();
                    console.log("   \u2713 ".concat(dbTableName, ": Inserted ").concat(uniqueRecords.length, " records"));
                    return [2 /*return*/, uniqueRecords.length];
                case 6:
                    error_2 = _c.sent();
                    if ((_b = error_2.message) === null || _b === void 0 ? void 0 : _b.includes('Entry not found')) {
                        console.log("   \u2139\uFE0F  ".concat(dbTableName, ": No insert file"));
                        return [2 /*return*/, 0];
                    }
                    throw error_2;
                case 7: return [2 /*return*/];
            }
        });
    });
}
/**
 * Resolve primary names for enterprises
 * Updates enterprises where primary_name is the enterprise number (temporary placeholder)
 * to use actual names from the denominations table
 */
function resolvePrimaryNames(db, metadata) {
    return __awaiter(this, void 0, void 0, function () {
        var snapshotDate, extractNumber, sql, result;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    snapshotDate = (0, column_mapping_1.convertKboDateFormat)(metadata.SnapshotDate);
                    extractNumber = parseInt(metadata.ExtractNumber);
                    sql = "\n    UPDATE enterprises e\n    SET\n      primary_name = COALESCE(\n        d.denomination_nl,\n        d.denomination_fr,\n        d.denomination_unknown,\n        d.denomination_de,\n        d.denomination_en,\n        e.enterprise_number\n      ),\n      primary_name_language = COALESCE(\n        CASE WHEN d.denomination_nl IS NOT NULL THEN '2' END,\n        CASE WHEN d.denomination_fr IS NOT NULL THEN '1' END,\n        CASE WHEN d.denomination_unknown IS NOT NULL THEN '0' END,\n        CASE WHEN d.denomination_de IS NOT NULL THEN '3' END,\n        CASE WHEN d.denomination_en IS NOT NULL THEN '4' END,\n        NULL\n      ),\n      primary_name_nl = d.denomination_nl,\n      primary_name_fr = d.denomination_fr,\n      primary_name_de = d.denomination_de\n    FROM (\n      SELECT\n        entity_number,\n        MAX(CASE WHEN language = '2' AND denomination_type = '001' THEN denomination END) as denomination_nl,\n        MAX(CASE WHEN language = '1' AND denomination_type = '001' THEN denomination END) as denomination_fr,\n        MAX(CASE WHEN language = '0' AND denomination_type = '001' THEN denomination END) as denomination_unknown,\n        MAX(CASE WHEN language = '3' AND denomination_type = '001' THEN denomination END) as denomination_de,\n        MAX(CASE WHEN language = '4' AND denomination_type = '001' THEN denomination END) as denomination_en\n      FROM denominations\n      WHERE _is_current = true\n        AND entity_type = 'enterprise'\n        AND denomination_type = '001'\n      GROUP BY entity_number\n    ) d\n    WHERE e.enterprise_number = d.entity_number\n      AND e._snapshot_date = '".concat(snapshotDate, "'\n      AND e._extract_number = ").concat(extractNumber, "\n      AND e._is_current = true\n      AND e.primary_name = e.enterprise_number\n  ");
                    return [4 /*yield*/, (0, motherduck_1.executeStatement)(db, sql)
                        // Count how many were updated
                    ];
                case 1:
                    _b.sent();
                    return [4 /*yield*/, (0, motherduck_1.executeQuery)(db, "\n    SELECT COUNT(*) as count\n    FROM enterprises\n    WHERE _snapshot_date = '".concat(snapshotDate, "'\n      AND _extract_number = ").concat(extractNumber, "\n      AND _is_current = true\n      AND primary_name != enterprise_number\n  "))];
                case 2:
                    result = _b.sent();
                    return [2 /*return*/, ((_a = result[0]) === null || _a === void 0 ? void 0 : _a.count) || 0];
            }
        });
    });
}
/**
 * Process daily update ZIP file
 */
function processDailyUpdate(zipPath) {
    return __awaiter(this, void 0, void 0, function () {
        var stats, zip, db, jobId, dbName, _a, jobStartTime, entries, tables, _i, _b, name_1, tableList, _c, tableList_1, csvTableName, dbTableName, deletes, inserts, error_3, errorMsg, resolved, error_4, errorMsg, totalRecordsProcessed, jobStatus, errorMessage, error_5, errorMessage, updateError_1;
        return __generator(this, function (_d) {
            switch (_d.label) {
                case 0:
                    console.log("\n\uD83D\uDCE6 Processing daily update: ".concat(path.basename(zipPath), "\n"));
                    stats = {
                        metadata: {},
                        tablesProcessed: [],
                        deletesApplied: 0,
                        insertsApplied: 0,
                        errors: []
                    };
                    zip = new node_stream_zip_1.default.async({ file: zipPath });
                    return [4 /*yield*/, (0, motherduck_1.connectMotherduck)()];
                case 1:
                    db = _d.sent();
                    _d.label = 2;
                case 2:
                    _d.trys.push([2, 20, 25, 28]);
                    dbName = process.env.MOTHERDUCK_DATABASE;
                    if (!dbName) {
                        throw new Error('MOTHERDUCK_DATABASE not set in environment');
                    }
                    return [4 /*yield*/, (0, motherduck_1.executeQuery)(db, "USE ".concat(dbName))
                        // Step 1: Parse metadata
                    ];
                case 3:
                    _d.sent();
                    // Step 1: Parse metadata
                    console.log('📋 Reading metadata...');
                    _a = stats;
                    return [4 /*yield*/, parseMetadata(zip)];
                case 4:
                    _a.metadata = _d.sent();
                    console.log("   \u2713 Snapshot Date: ".concat(stats.metadata.SnapshotDate));
                    console.log("   \u2713 Extract Number: ".concat(stats.metadata.ExtractNumber));
                    console.log("   \u2713 Extract Type: ".concat(stats.metadata.ExtractType));
                    if (stats.metadata.ExtractType !== 'update') {
                        throw new Error("Expected 'update' extract type, got '".concat(stats.metadata.ExtractType, "'"));
                    }
                    // Step 2: Create import job record
                    console.log('\n📝 Creating import job record...');
                    jobId = (0, crypto_1.randomUUID)();
                    jobStartTime = new Date().toISOString();
                    return [4 /*yield*/, (0, motherduck_1.executeQuery)(db, "INSERT INTO import_jobs (\n        id, extract_number, extract_type, snapshot_date, extract_timestamp,\n        status, started_at, worker_type\n      ) VALUES (\n        '".concat(jobId, "',\n        ").concat(stats.metadata.ExtractNumber, ",\n        'update',\n        '").concat(stats.metadata.SnapshotDate, "',\n        '").concat(stats.metadata.ExtractTimestamp, "',\n        'running',\n        '").concat(jobStartTime, "',\n        'local'\n      )"))];
                case 5:
                    _d.sent();
                    console.log("   \u2713 Job ID: ".concat(jobId));
                    return [4 /*yield*/, zip.entries()];
                case 6:
                    entries = _d.sent();
                    tables = new Set();
                    for (_i = 0, _b = Object.keys(entries); _i < _b.length; _i++) {
                        name_1 = _b[_i];
                        if (name_1.endsWith('_delete.csv')) {
                            tables.add(name_1.replace('_delete.csv', ''));
                        }
                        else if (name_1.endsWith('_insert.csv')) {
                            tables.add(name_1.replace('_insert.csv', ''));
                        }
                    }
                    tableList = Array.from(tables).sort();
                    console.log("\n\uD83D\uDCCA Tables to process: ".concat(tableList.join(', '), "\n"));
                    _c = 0, tableList_1 = tableList;
                    _d.label = 7;
                case 7:
                    if (!(_c < tableList_1.length)) return [3 /*break*/, 13];
                    csvTableName = tableList_1[_c];
                    dbTableName = (0, column_mapping_1.csvTableToDbTable)(csvTableName);
                    _d.label = 8;
                case 8:
                    _d.trys.push([8, 11, , 12]);
                    console.log("\uD83D\uDD04 Processing ".concat(dbTableName, "..."));
                    return [4 /*yield*/, applyDeletes(db, zip, csvTableName, dbTableName, stats.metadata)];
                case 9:
                    deletes = _d.sent();
                    stats.deletesApplied += deletes;
                    return [4 /*yield*/, applyInserts(db, zip, csvTableName, dbTableName, stats.metadata)];
                case 10:
                    inserts = _d.sent();
                    stats.insertsApplied += inserts;
                    stats.tablesProcessed.push(dbTableName);
                    return [3 /*break*/, 12];
                case 11:
                    error_3 = _d.sent();
                    errorMsg = "".concat(dbTableName, ": ").concat(error_3.message);
                    stats.errors.push(errorMsg);
                    console.error("   \u274C ".concat(errorMsg));
                    return [3 /*break*/, 12];
                case 12:
                    _c++;
                    return [3 /*break*/, 7];
                case 13:
                    if (!(stats.tablesProcessed.includes('enterprises') || stats.tablesProcessed.includes('denominations'))) return [3 /*break*/, 17];
                    console.log('\n🔄 Resolving primary names for new enterprises...');
                    _d.label = 14;
                case 14:
                    _d.trys.push([14, 16, , 17]);
                    return [4 /*yield*/, resolvePrimaryNames(db, stats.metadata)];
                case 15:
                    resolved = _d.sent();
                    if (resolved > 0) {
                        console.log("   \u2713 Resolved primary names for ".concat(resolved, " enterprises"));
                    }
                    else {
                        console.log("   \u2139\uFE0F  No new enterprises requiring name resolution");
                    }
                    return [3 /*break*/, 17];
                case 16:
                    error_4 = _d.sent();
                    errorMsg = "Primary name resolution: ".concat(error_4.message);
                    stats.errors.push(errorMsg);
                    console.error("   \u274C ".concat(errorMsg));
                    return [3 /*break*/, 17];
                case 17:
                    if (!jobId) return [3 /*break*/, 19];
                    totalRecordsProcessed = stats.deletesApplied + stats.insertsApplied;
                    jobStatus = stats.errors.length > 0 ? 'failed' : 'completed';
                    errorMessage = stats.errors.length > 0 ? stats.errors.join('; ') : null;
                    return [4 /*yield*/, (0, motherduck_1.executeQuery)(db, "UPDATE import_jobs SET\n          status = '".concat(jobStatus, "',\n          completed_at = '").concat(new Date().toISOString(), "',\n          records_processed = ").concat(totalRecordsProcessed, ",\n          records_inserted = ").concat(stats.insertsApplied, ",\n          records_updated = 0,\n          records_deleted = ").concat(stats.deletesApplied, "\n          ").concat(errorMessage ? ", error_message = '".concat(errorMessage.replace(/'/g, "''"), "'") : '', "\n        WHERE id = '").concat(jobId, "'"))];
                case 18:
                    _d.sent();
                    console.log("\n   \u2713 Job ".concat(jobStatus, ": ").concat(jobId));
                    _d.label = 19;
                case 19: return [3 /*break*/, 28];
                case 20:
                    error_5 = _d.sent();
                    if (!jobId) return [3 /*break*/, 24];
                    _d.label = 21;
                case 21:
                    _d.trys.push([21, 23, , 24]);
                    errorMessage = error_5.message || 'Unknown error';
                    return [4 /*yield*/, (0, motherduck_1.executeQuery)(db, "UPDATE import_jobs SET\n            status = 'failed',\n            completed_at = '".concat(new Date().toISOString(), "',\n            error_message = '").concat(errorMessage.replace(/'/g, "''"), "'\n          WHERE id = '").concat(jobId, "'"))];
                case 22:
                    _d.sent();
                    return [3 /*break*/, 24];
                case 23:
                    updateError_1 = _d.sent();
                    console.error('Failed to update job status:', updateError_1);
                    return [3 /*break*/, 24];
                case 24: throw error_5;
                case 25: return [4 /*yield*/, zip.close()];
                case 26:
                    _d.sent();
                    return [4 /*yield*/, (0, motherduck_1.closeMotherduck)(db)];
                case 27:
                    _d.sent();
                    return [7 /*endfinally*/];
                case 28: return [2 /*return*/, stats];
            }
        });
    });
}
/**
 * Main execution
 */
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var args, zipPath, stats, error_6;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    args = process.argv.slice(2);
                    if (args.length === 0) {
                        console.error('Usage: npx tsx scripts/apply-daily-update.ts <path-to-update.zip>');
                        console.error('\nExample:');
                        console.error('  npx tsx scripts/apply-daily-update.ts sampledata/KboOpenData_0141_2025_10_06_Update.zip');
                        process.exit(1);
                    }
                    zipPath = args[0];
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, processDailyUpdate(zipPath)
                        // Summary
                    ];
                case 2:
                    stats = _a.sent();
                    // Summary
                    console.log('\n' + '='.repeat(60));
                    console.log('📊 DAILY UPDATE SUMMARY');
                    console.log('='.repeat(60));
                    console.log("Extract Number: ".concat(stats.metadata.ExtractNumber));
                    console.log("Snapshot Date: ".concat(stats.metadata.SnapshotDate));
                    console.log("\nTables Processed: ".concat(stats.tablesProcessed.length));
                    console.log("Records Marked Historical: ".concat(stats.deletesApplied));
                    console.log("Records Inserted: ".concat(stats.insertsApplied));
                    console.log("Total Changes: ".concat(stats.deletesApplied + stats.insertsApplied));
                    if (stats.errors.length > 0) {
                        console.log("\n\u26A0\uFE0F  Errors: ".concat(stats.errors.length));
                        stats.errors.forEach(function (err) { return console.log("   \u2022 ".concat(err)); });
                    }
                    console.log('\n' + '='.repeat(60));
                    console.log(stats.errors.length > 0 ? '⚠️  Completed with errors' : '✅ Update applied successfully');
                    console.log('='.repeat(60) + '\n');
                    process.exit(stats.errors.length > 0 ? 1 : 0);
                    return [3 /*break*/, 4];
                case 3:
                    error_6 = _a.sent();
                    console.error('\n❌ Update failed:', error_6);
                    process.exit(1);
                    return [3 /*break*/, 4];
                case 4: return [2 /*return*/];
            }
        });
    });
}
main();
