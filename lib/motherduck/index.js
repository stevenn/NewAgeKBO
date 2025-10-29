"use strict";
/**
 * Motherduck connection utilities
 * Uses @duckdb/node-api for serverless compatibility
 *
 * Note: Uses @duckdb/node-api which works in Vercel serverless functions
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.getMotherduckConfig = getMotherduckConfig;
exports.connectMotherduck = connectMotherduck;
exports.executeQuery = executeQuery;
exports.executeStatement = executeStatement;
exports.executeTransaction = executeTransaction;
exports.closeMotherduck = closeMotherduck;
exports.databaseExists = databaseExists;
exports.ensureDatabase = ensureDatabase;
exports.tableExists = tableExists;
exports.getTableCount = getTableCount;
exports.getDatabaseStats = getDatabaseStats;
var node_api_1 = require("@duckdb/node-api");
var errors_1 = require("@/lib/errors");
/**
 * Get Motherduck configuration from environment
 */
function getMotherduckConfig() {
    var token = process.env.MOTHERDUCK_TOKEN;
    if (!token) {
        throw new errors_1.MotherduckError('MOTHERDUCK_TOKEN environment variable is not set. Please add it to .env.local');
    }
    return {
        token: token,
        database: process.env.MOTHERDUCK_DATABASE || 'kbo',
    };
}
/**
 * Connect to Motherduck
 * Returns a Promise that resolves to a DuckDB connection
 *
 * For serverless environments (Vercel), we use a special connection sequence:
 * 1. Create in-memory DuckDB instance (no filesystem access needed)
 * 2. Set all directory configs to /tmp (required before Motherduck extension loads)
 * 3. Attach to Motherduck database
 * 4. Switch to using the Motherduck database
 *
 * This approach avoids "home directory not found" errors in serverless environments
 * where the filesystem is read-only except for /tmp.
 */
function connectMotherduck(config) {
    return __awaiter(this, void 0, void 0, function () {
        var mdConfig, instance, connection, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    mdConfig = config || getMotherduckConfig();
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 9, , 10]);
                    return [4 /*yield*/, node_api_1.DuckDBInstance.create(':memory:')];
                case 2:
                    instance = _a.sent();
                    return [4 /*yield*/, instance.connect()
                        // CRITICAL: Set all directory configurations BEFORE attaching to Motherduck
                        // The Motherduck extension checks home_directory during motherduck_init()
                    ];
                case 3:
                    connection = _a.sent();
                    // CRITICAL: Set all directory configurations BEFORE attaching to Motherduck
                    // The Motherduck extension checks home_directory during motherduck_init()
                    return [4 /*yield*/, connection.run("SET home_directory='/tmp'")];
                case 4:
                    // CRITICAL: Set all directory configurations BEFORE attaching to Motherduck
                    // The Motherduck extension checks home_directory during motherduck_init()
                    _a.sent();
                    return [4 /*yield*/, connection.run("SET extension_directory='/tmp/.duckdb/extensions'")];
                case 5:
                    _a.sent();
                    return [4 /*yield*/, connection.run("SET temp_directory='/tmp'")
                        // Set Motherduck token as environment variable (DuckDB will pick it up automatically)
                    ];
                case 6:
                    _a.sent();
                    // Set Motherduck token as environment variable (DuckDB will pick it up automatically)
                    process.env.motherduck_token = mdConfig.token;
                    // Attach to Motherduck database (extension will be auto-installed to /tmp)
                    return [4 /*yield*/, connection.run("ATTACH 'md:".concat(mdConfig.database, "' AS md"))
                        // Switch to using the Motherduck database
                    ];
                case 7:
                    // Attach to Motherduck database (extension will be auto-installed to /tmp)
                    _a.sent();
                    // Switch to using the Motherduck database
                    return [4 /*yield*/, connection.run("USE md")];
                case 8:
                    // Switch to using the Motherduck database
                    _a.sent();
                    return [2 /*return*/, connection];
                case 9:
                    error_1 = _a.sent();
                    throw new errors_1.MotherduckError("Failed to connect to Motherduck: ".concat(error_1 instanceof Error ? error_1.message : String(error_1)), error_1 instanceof Error ? error_1 : undefined);
                case 10: return [2 /*return*/];
            }
        });
    });
}
/**
 * Execute a SQL query
 */
function executeQuery(connection, sql) {
    return __awaiter(this, void 0, void 0, function () {
        var result, chunks, columnNames, rows, _i, chunks_1, chunk, rowArrays, _loop_1, _a, rowArrays_1, rowArray, error_2;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 3, , 4]);
                    return [4 /*yield*/, connection.run(sql)];
                case 1:
                    result = _b.sent();
                    return [4 /*yield*/, result.fetchAllChunks()
                        // Get column names from the result
                    ];
                case 2:
                    chunks = _b.sent();
                    columnNames = result.columnNames();
                    rows = [];
                    for (_i = 0, chunks_1 = chunks; _i < chunks_1.length; _i++) {
                        chunk = chunks_1[_i];
                        rowArrays = chunk.getRows();
                        _loop_1 = function (rowArray) {
                            var rowObject = {};
                            columnNames.forEach(function (colName, idx) {
                                rowObject[colName] = rowArray[idx];
                            });
                            rows.push(rowObject);
                        };
                        // Convert to objects using column names
                        for (_a = 0, rowArrays_1 = rowArrays; _a < rowArrays_1.length; _a++) {
                            rowArray = rowArrays_1[_a];
                            _loop_1(rowArray);
                        }
                    }
                    return [2 /*return*/, rows];
                case 3:
                    error_2 = _b.sent();
                    throw new errors_1.MotherduckError("Query execution failed: ".concat(error_2 instanceof Error ? error_2.message : String(error_2)), error_2 instanceof Error ? error_2 : undefined);
                case 4: return [2 /*return*/];
            }
        });
    });
}
/**
 * Execute a SQL statement (no results expected)
 */
function executeStatement(connection, sql) {
    return __awaiter(this, void 0, void 0, function () {
        var error_3;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 2, , 3]);
                    return [4 /*yield*/, connection.run(sql)];
                case 1:
                    _a.sent();
                    return [3 /*break*/, 3];
                case 2:
                    error_3 = _a.sent();
                    throw new errors_1.MotherduckError("Statement execution failed: ".concat(error_3 instanceof Error ? error_3.message : String(error_3)), error_3 instanceof Error ? error_3 : undefined);
                case 3: return [2 /*return*/];
            }
        });
    });
}
/**
 * Execute multiple SQL statements in a transaction
 */
function executeTransaction(connection, statements) {
    return __awaiter(this, void 0, void 0, function () {
        var _i, statements_1, sql, error_4;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, executeStatement(connection, 'BEGIN TRANSACTION')];
                case 1:
                    _a.sent();
                    _a.label = 2;
                case 2:
                    _a.trys.push([2, 8, , 10]);
                    _i = 0, statements_1 = statements;
                    _a.label = 3;
                case 3:
                    if (!(_i < statements_1.length)) return [3 /*break*/, 6];
                    sql = statements_1[_i];
                    return [4 /*yield*/, executeStatement(connection, sql)];
                case 4:
                    _a.sent();
                    _a.label = 5;
                case 5:
                    _i++;
                    return [3 /*break*/, 3];
                case 6: return [4 /*yield*/, executeStatement(connection, 'COMMIT')];
                case 7:
                    _a.sent();
                    return [3 /*break*/, 10];
                case 8:
                    error_4 = _a.sent();
                    return [4 /*yield*/, executeStatement(connection, 'ROLLBACK')];
                case 9:
                    _a.sent();
                    throw error_4;
                case 10: return [2 /*return*/];
            }
        });
    });
}
/**
 * Close database connection
 */
function closeMotherduck(connection) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            try {
                connection.closeSync();
            }
            catch (error) {
                throw new errors_1.MotherduckError("Failed to close connection: ".concat(error instanceof Error ? error.message : String(error)), error instanceof Error ? error : undefined);
            }
            return [2 /*return*/];
        });
    });
}
/**
 * Check if database exists
 */
function databaseExists(connection, dbName) {
    return __awaiter(this, void 0, void 0, function () {
        var result;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, executeQuery(connection, 'SHOW DATABASES')];
                case 1:
                    result = _a.sent();
                    return [2 /*return*/, result.some(function (row) { return row.database_name === dbName; })];
            }
        });
    });
}
/**
 * Create database if it doesn't exist
 */
function ensureDatabase(connection, dbName) {
    return __awaiter(this, void 0, void 0, function () {
        var exists;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, databaseExists(connection, dbName)];
                case 1:
                    exists = _a.sent();
                    if (!!exists) return [3 /*break*/, 3];
                    return [4 /*yield*/, executeStatement(connection, "CREATE DATABASE IF NOT EXISTS ".concat(dbName))];
                case 2:
                    _a.sent();
                    console.log("Created database: ".concat(dbName));
                    _a.label = 3;
                case 3: return [2 /*return*/];
            }
        });
    });
}
/**
 * Check if table exists
 */
function tableExists(connection, tableName) {
    return __awaiter(this, void 0, void 0, function () {
        var result;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, executeQuery(connection, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")];
                case 1:
                    result = _a.sent();
                    return [2 /*return*/, result.some(function (row) { return row.table_name === tableName; })];
            }
        });
    });
}
/**
 * Get table row count
 */
function getTableCount(connection, tableName) {
    return __awaiter(this, void 0, void 0, function () {
        var result;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, executeQuery(connection, "SELECT COUNT(*) as count FROM ".concat(tableName))];
                case 1:
                    result = _b.sent();
                    return [2 /*return*/, ((_a = result[0]) === null || _a === void 0 ? void 0 : _a.count) || 0];
            }
        });
    });
}
/**
 * Get database statistics
 */
function getDatabaseStats(connection) {
    return __awaiter(this, void 0, void 0, function () {
        var tables, stats, _i, tables_1, table_name, count, _a, _b;
        return __generator(this, function (_c) {
            switch (_c.label) {
                case 0:
                    _c.trys.push([0, 8, , 9]);
                    return [4 /*yield*/, executeQuery(connection, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")];
                case 1:
                    tables = _c.sent();
                    stats = [];
                    _i = 0, tables_1 = tables;
                    _c.label = 2;
                case 2:
                    if (!(_i < tables_1.length)) return [3 /*break*/, 7];
                    table_name = tables_1[_i].table_name;
                    // Skip system tables that start with underscore
                    if (table_name.startsWith('_')) {
                        return [3 /*break*/, 6];
                    }
                    _c.label = 3;
                case 3:
                    _c.trys.push([3, 5, , 6]);
                    return [4 /*yield*/, getTableCount(connection, table_name)];
                case 4:
                    count = _c.sent();
                    stats.push({ table_name: table_name, row_count: count });
                    return [3 /*break*/, 6];
                case 5:
                    _a = _c.sent();
                    // Skip tables that can't be counted
                    console.warn("Warning: Could not count rows in table ".concat(table_name));
                    return [3 /*break*/, 6];
                case 6:
                    _i++;
                    return [3 /*break*/, 2];
                case 7: return [2 /*return*/, stats];
                case 8:
                    _b = _c.sent();
                    // If we can't query information_schema, return empty array
                    return [2 /*return*/, []];
                case 9: return [2 /*return*/];
            }
        });
    });
}
