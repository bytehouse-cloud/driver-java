/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance.bytehouse.jdbc;

import com.bytedance.bytehouse.data.DataTypeFactory;
import com.bytedance.bytehouse.data.IDataType;
import com.bytedance.bytehouse.jdbc.wrapper.BHDatabaseMetadata;
import com.bytedance.bytehouse.jdbc.wrapper.SQLHelper;
import com.bytedance.bytehouse.log.Logger;
import com.bytedance.bytehouse.log.LoggerFactory;
import com.bytedance.bytehouse.settings.BHConstants;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public final class ByteHouseDatabaseMetadata implements BHDatabaseMetadata, SQLHelper {

    public static final String TABLE_SCHEM = "TABLE_SCHEM";

    private static final String TABLE_NAME = "TABLE_NAME";

    private static final Logger LOG = LoggerFactory.getLogger(ByteHouseDatabaseMetadata.class);

    private final String url;

    private final ByteHouseConnection connection;

    /**
     * constructor.
     * <br><br>
     * this class is not the resource creator of the {@link ByteHouseConnection}.
     * Hence it's not this class's job to close the connection.
     */
    public ByteHouseDatabaseMetadata(
            final String url,
            final ByteHouseConnection connection
    ) {
        this.url = url;
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.cfg().user();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "ByteHouse";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.serverContext().version();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "com.bytedance.bytehouse.jdbc.ByteHouseDriver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return String.valueOf(BHConstants.CLIENT_REVISION);
    }

    @Override
    public int getDriverMajorVersion() {
        return BHConstants.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return BHConstants.MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "GLOBAL,ARRAY";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /**
     * Bytehouse does not support any stored procedure.
     */
    @Override
    public ResultSet getProcedures(
            final String catalog,
            final String schemaPattern,
            final String procedureNamePattern
    ) throws SQLException {

        return ByteHouseResultSetBuilder
                .builder(9, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "PROCEDURE_CAT",
                        "PROCEDURE_SCHEM",
                        "PROCEDURE_NAME",
                        "RES_1",
                        "RES_2",
                        "RES_3",
                        "REMARKS",
                        "PROCEDURE_TYPE",
                        "SPECIFIC_NAME")
                .columnTypes(
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "UInt8",
                        "String")
                .build();
    }

    /**
     * Bytehouse does not support any stored procedure.
     */
    @Override
    public ResultSet getProcedureColumns(
            final String catalog,
            final String schemaPattern,
            final String procedureNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        return ByteHouseResultSetBuilder
                .builder(20, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "1", "2", "3", "4", "5",
                        "6", "7", "8", "9", "10",
                        "11", "12", "13", "14", "15",
                        "16", "17", "18", "19", "20")
                .columnTypes(
                        "UInt32", "UInt32", "UInt32", "UInt32", "UInt32",
                        "UInt32", "UInt32", "UInt32", "UInt32", "UInt32",
                        "UInt32", "UInt32", "UInt32", "UInt32", "UInt32",
                        "UInt32", "UInt32", "UInt32", "UInt32", "UInt32")
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTables(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String[] types
    ) throws SQLException {
        /*
         TABLE_CAT                 String => table catalog (may be null)
         TABLE_SCHEM               String => table schema (may be null)
         TABLE_NAME                String => table name
         TABLE_TYPE                String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
                                             "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
         REMARKS                   String => explanatory comment on the table
         TYPE_CAT                  String => the types catalog (may be null)
         TYPE_SCHEM                String => the types schema (may be null)
         TYPE_NAME                 String => type name (may be null)
         SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
         REF_GENERATION            String => specifies how values in SELF_REFERENCING_COL_NAME are created.
                                             Values are "SYSTEM", "USER", "DERIVED". (may be null)
         */

        if (this.connection.cfg().isCnch()) {
            return getTablesForCnch(schemaPattern, tableNamePattern, types);
        } else {
            return getTablesForBytehouse(schemaPattern, tableNamePattern, types);
        }
    }

    private ByteHouseResultSet getTablesForBytehouse(
            final String schemaPattern,
            final String tableNamePattern,
            final String[] types
    ) throws SQLException {

        final List<String> matchingSchemas;
        try (ResultSet rs = getSchemasForBytehouse("", schemaPattern)) {
            matchingSchemas = rsStringToList(TABLE_SCHEM, rs);
        }

        final ByteHouseResultSetBuilder builder = ByteHouseResultSetBuilder
                .builder(10, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TABLE_CAT",
                        TABLE_SCHEM,
                        TABLE_NAME,
                        "TABLE_TYPE",
                        "REMARKS",
                        "TYPE_CAT",
                        "TYPE_SCHEM",
                        "TYPE_NAME",
                        "SELF_REFERENCING_COL_NAME",
                        "REF_GENERATION"
                ).columnTypes(
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String",
                        "String"
                );
        // "null" is different from "empty". "null" means "ALL". "empty" means "NONE".
        // see {@link DatabaseMetaData}#getTables documentation.
        final Set<String> filterSet = types == null ? null :
                Arrays.stream(types)
                        .map(s -> s.toUpperCase(Locale.ROOT))
                        .collect(Collectors.toSet());

        for (final String matchingSchema : matchingSchemas) {
            final String sql = String.format("SHOW TABLES FROM `%s`", matchingSchema);
            try (ResultSet rs = request(sql)) {
                while (rs.next()) {
                    // filter 1
                    final String type = rs.getString("Type").toUpperCase(Locale.ROOT);
                    if (filterSet != null && !filterSet.contains(type)) {
                        continue;
                    }

                    // filter 2
                    final String tableName = rs.getString("Name");
                    if (tableNamePattern != null && !sqlLike(tableName, tableNamePattern)) {
                        continue;
                    }

                    builder.addRow(
                            type, //TABLE_CAT
                            matchingSchema, //TABLE_SCHEM
                            tableName, //TABLE_NAME
                            type, // TABLE_TYPE
                            rs.getString("Comments"), //REMARKS
                            null, // TYPE_CAT
                            null, // TYPE_SCHEM
                            null, // TYPE_NAME
                            null, // SELF_REFERENCING_COL_NAME
                            null // REF_GENERATION
                    );
                }
            }
        }
        return builder.build();
    }

    @Deprecated
    private ByteHouseResultSet getTablesForCnch(
            final String schemaPattern,
            final String tableNamePattern,
            final String[] types
    ) throws SQLException {
        String sql = "select database, name from system.cnch_tables where 1=1";
        if (schemaPattern != null) {
            sql += " and database like '" + schemaPattern + "'";
        }
        if (tableNamePattern != null) {
            sql += " and name like '" + tableNamePattern + "'";
        }
        sql += " order by database, name";
        final ByteHouseResultSetBuilder builder;
        try (ResultSet result = request(sql)) {

            builder = ByteHouseResultSetBuilder
                    .builder(10, connection.serverContext())
                    .cfg(connection.cfg())
                    .columnNames(
                            "TABLE_CAT",
                            TABLE_SCHEM,
                            TABLE_NAME,
                            "TABLE_TYPE",
                            "REMARKS",
                            "TYPE_CAT",
                            "TYPE_SCHEM",
                            "TYPE_NAME",
                            "SELF_REFERENCING_COL_NAME",
                            "REF_GENERATION")
                    .columnTypes(
                            "String",
                            "String",
                            "String",
                            "String",
                            "String",
                            "String",
                            "String",
                            "String",
                            "String",
                            "String");

            final List<String> typeList = types != null ? Arrays.asList(types) : null;
            while (result.next()) {
                final List<String> row = new ArrayList<>();
                row.add(BHConstants.DEFAULT_CATALOG);
                row.add(result.getString(1));
                row.add(result.getString(2));
                String type = "TABLE";
                row.add(type);
                for (int i = 3; i < 9; i++) {
                    row.add(null);
                }
                if (typeList == null || typeList.contains(type)) {
                    builder.addRow(row);
                }
            }
        }
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getSchemas(
            final String catalog,
            final String schemaPattern
    ) throws SQLException {
        if (this.connection.cfg().isCnch()) {
            return getSchemasForCnch(catalog, schemaPattern);
        } else {
            return getSchemasForBytehouse(catalog, schemaPattern);
        }
    }

    private ResultSet getSchemasForBytehouse(
            final String catalog,
            final String schemaPattern) throws SQLException {
        final ByteHouseResultSetBuilder builder = ByteHouseResultSetBuilder
                .builder(2, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(TABLE_SCHEM, "TABLE_CATALOG")
                .columnTypes("String", "String");

        final String sql = "SHOW DATABASES";
        try (ResultSet rs = request(sql)) {
            while (rs.next()) {
                final String name = rs.getString("Name").toUpperCase(Locale.ROOT);
                if (schemaPattern != null && !sqlLike(name, schemaPattern)) {
                    continue;
                }
                builder.addRow(
                        name,
                        BHConstants.DEFAULT_CATALOG
                );
            }
        }
        return builder.build();
    }

    @Deprecated
    private ResultSet getSchemasForCnch(
            final String catalog,
            final String schemaPattern) throws SQLException {
        String sql = String.format("SELECT "
                        + " name as TABLE_SCHEM, "
                        + " '%s' as TABLE_CATALOG "
                        + " from system.cnch_databases ",
                BHConstants.DEFAULT_CATALOG
        );
        if (catalog != null) {
            sql += String.format(" where TABLE_CATALOG = '%s'", catalog);
        }
        if (schemaPattern != null) {
            if (catalog != null) {
                sql += " and ";
            } else {
                sql += " where ";
            }
            sql += "name LIKE '" + schemaPattern + '\'';
        }
        return request(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        return ByteHouseResultSetBuilder
                .builder(1, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames("TABLE_CAT")
                .columnTypes("String")
                .addRow(BHConstants.DEFAULT_CATALOG).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return ByteHouseResultSetBuilder
                .builder(1, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames("TABLE_TYPE")
                .columnTypes("String")
                .addRow("TABLE")
                .addRow("VIEW")
                .addRow("OTHER").build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getColumns(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        if (this.connection.cfg().isCnch()) {
            return getColumnsForCnch(schemaPattern, tableNamePattern, columnNamePattern);
        } else {
            return getColumnsForBytehouse(schemaPattern, tableNamePattern, columnNamePattern);
        }
    }

    private ByteHouseResultSet getColumnsForBytehouse(
            final String schemaPattern,
            final String tableNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        final List<String> matchingTables = new ArrayList<>();
        final List<String> matchingSchemas = new ArrayList<>(); // same order as the table
        try (ByteHouseResultSet rs = getTablesForBytehouse(
                schemaPattern,
                tableNamePattern,
                null
        )) {
            while (rs.next()) {
                matchingSchemas.add(rs.getString(TABLE_SCHEM));
                matchingTables.add(rs.getString(TABLE_NAME));
            }
        }

        final ByteHouseResultSetBuilder builder = ByteHouseResultSetBuilder
                .builder(24, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TABLE_CAT",
                        TABLE_SCHEM,
                        TABLE_NAME,
                        "COLUMN_NAME",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "COLUMN_SIZE",
                        "BUFFER_LENGTH",
                        "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "COLUMN_DEF",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SCOPE_CATALOG",
                        "SCOPE_SCHEMA",
                        "SCOPE_TABLE",
                        "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT",
                        "IS_GENERATEDCOLUMN")
                .columnTypes(
                        "String",
                        "String",
                        "String",
                        "String",
                        "Int32",
                        "String",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "String",
                        "String",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "String",
                        "String",
                        "String",
                        "String",
                        "Int32",
                        "String",
                        "String");

        for (int i = 0; i < matchingTables.size(); i++) {
            final String matchingTable = matchingTables.get(i);
            final String matchingSchema = matchingSchemas.get(i);

            final String sql = String.format("DESCRIBE TABLE `%s`.`%s`", matchingSchema, matchingTable);
            logger().info(sql);
            try (ResultSet descTable = request(sql)) {
                int colNum = 1;
                while (descTable.next()) {
                    final String columnName = descTable.getString("Name");
                    if (columnNamePattern != null && !sqlLike(columnName, columnNamePattern)) {
                        continue;
                    }

                    final List<Object> row = new ArrayList<>();
                    //catalog name
                    row.add(BHConstants.DEFAULT_CATALOG);

                    //database name
                    row.add(matchingSchema);

                    //table name
                    row.add(matchingTable);

                    //column name
                    final IDataType<?, ?> dataType = DataTypeFactory.get(
                            descTable.getString("Type"),
                            connection.serverContext()
                    );
                    row.add(columnName);

                    //data type
                    row.add(dataType.sqlTypeId());
                    //type name
                    row.add(dataType.name());
                    // column size / precision
                    row.add(dataType.getPrecision());
                    //buffer length
                    row.add(0);
                    // decimal digits
                    row.add(dataType.getScale());
                    // radix
                    row.add(10);
                    // nullable
                    row.add(dataType.nullable() ? columnNullable : columnNoNulls);
                    //remarks
                    final String comment = descTable.getString("Comment");
                    row.add(comment == null || comment.isEmpty() ? null : comment);

                    // COLUMN_DEF
                    final String defaultType = descTable.getString("DefaultType");
                    final String defaultExpression = descTable.getString("DefaultExpression");
                    if (defaultType != null && !defaultType.isEmpty()) {
                        row.add(defaultType);
                    } else if (defaultExpression != null && !defaultExpression.isEmpty()) {
                        row.add(defaultExpression);
                    } else {
                        row.add(null);
                    }

                    //"SQL_DATA_TYPE", unused per JavaDoc
                    row.add(null);
                    //"SQL_DATETIME_SUB", unused per JavaDoc
                    row.add(null);

                    // char octet length
                    row.add(0);
                    // ordinal
                    row.add(colNum++);

                    //IS_NULLABLE
                    row.add(dataType.nullable() ? "YES" : "NO");
                    //"SCOPE_CATALOG",
                    row.add(null);
                    //"SCOPE_SCHEMA",
                    row.add(null);
                    //"SCOPE_TABLE",
                    row.add(null);
                    //"SOURCE_DATA_TYPE",
                    row.add(null);
                    //"IS_AUTOINCREMENT"
                    row.add("NO");
                    //"IS_GENERATEDCOLUMN"
                    row.add("NO");

                    builder.addRow(row);
                }
            }
        }
        return builder.build();
    }

    @Deprecated
    private ByteHouseResultSet getColumnsForCnch(
            final String schemaPattern,
            final String tableNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        StringBuilder query;
        if (connection.serverContext().version().compareTo("1.1.54237") > 0) {
            query = new StringBuilder(
                    "SELECT "
                            + " database, "
                            + " table, "
                            + " name, "
                            + " type, "
                            + " default_kind as default_type, "
                            + " default_expression "
            );
        } else {
            query = new StringBuilder(
                    "SELECT database, table, name, type, default_type, default_expression ");
        }
        query.append("FROM system.cnch_columns ");
        final List<String> predicates = new ArrayList<>();
        if (schemaPattern != null) {
            predicates.add("database LIKE '" + schemaPattern + "' ");
        }
        if (tableNamePattern != null) {
            predicates.add("table LIKE '" + tableNamePattern + "' ");
        }
        if (columnNamePattern != null) {
            predicates.add("name LIKE '" + columnNamePattern + "' ");
        }
        if (!predicates.isEmpty()) {
            query.append(" WHERE ");
            buildAndCondition(query, predicates);
        }
        ByteHouseResultSetBuilder builder = ByteHouseResultSetBuilder
                .builder(24, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TABLE_CAT",
                        TABLE_SCHEM,
                        TABLE_NAME,
                        "COLUMN_NAME",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "COLUMN_SIZE",
                        "BUFFER_LENGTH",
                        "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "COLUMN_DEF",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SCOPE_CATALOG",
                        "SCOPE_SCHEMA",
                        "SCOPE_TABLE",
                        "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT",
                        "IS_GENERATEDCOLUMN")
                .columnTypes(
                        "String",
                        "String",
                        "String",
                        "String",
                        "Int32",
                        "String",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "String",
                        "String",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "String",
                        "String",
                        "String",
                        "String",
                        "Int32",
                        "String",
                        "String");
        try (ResultSet descTable = request(query.toString())) {
            int colNum = 1;
            while (descTable.next()) {
                final List<Object> row = new ArrayList<>();
                //catalog name
                row.add(BHConstants.DEFAULT_CATALOG);
                //database name
                row.add(descTable.getString("database"));
                //table name
                row.add(descTable.getString("table"));
                //column name
                IDataType dataType = DataTypeFactory.get(
                        descTable.getString("type"),
                        connection.serverContext()
                );
                row.add(descTable.getString("name"));
                //data type
                row.add(dataType.sqlTypeId());
                //type name
                row.add(descTable.getString("name"));
                // column size / precision
                row.add(dataType.getPrecision());
                //buffer length
                row.add(0);
                // decimal digits
                row.add(dataType.getScale());
                // radix
                row.add(10);
                // nullable
                row.add(dataType.nullable() ? columnNullable : columnNoNulls);
                //remarks
                row.add(null);

                // COLUMN_DEF
                if ("DEFAULT".equals(descTable.getString("default_type"))) {
                    row.add(descTable.getString("default_expression"));
                } else {
                    row.add(null);
                }

                //"SQL_DATA_TYPE", unused per JavaDoc
                row.add(null);
                //"SQL_DATETIME_SUB", unused per JavaDoc
                row.add(null);

                // char octet length
                row.add(0);
                // ordinal
                row.add(colNum);
                colNum += 1;

                //IS_NULLABLE
                row.add(dataType.nullable() ? "YES" : "NO");
                //"SCOPE_CATALOG",
                row.add(null);
                //"SCOPE_SCHEMA",
                row.add(null);
                //"SCOPE_TABLE",
                row.add(null);
                //"SOURCE_DATA_TYPE",
                row.add(null);
                //"IS_AUTOINCREMENT"
                row.add("NO");
                //"IS_GENERATEDCOLUMN"
                row.add("NO");

                builder.addRow(row);
            }
        }
        return builder.build();
    }

    @Override
    public ResultSet getColumnPrivileges(
            final String catalog,
            final String schema,
            final String table,
            final String columnNamePattern
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(
            final String catalog,
            final String schema,
            final String table,
            final int scope,
            final boolean nullable
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getVersionColumns(
            final String catalog,
            final String schema,
            final String table
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getPrimaryKeys(
            final String catalog,
            final String schema,
            final String table
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getImportedKeys(
            final String catalog,
            final String schema,
            final String table
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getExportedKeys(
            final String catalog,
            final String schema,
            final String table
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getCrossReference(
            final String parentCatalog,
            final String parentSchema,
            final String parentTable,
            final String foreignCatalog,
            final String foreignSchema,
            final String foreignTable
    ) throws SQLException {
        // FIXME: 17/8/21 check this for gateway
        return getEmptyResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        final ByteHouseResultSetBuilder builder = ByteHouseResultSetBuilder
                .builder(18, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames(
                        "TYPE_NAME",
                        "DATA_TYPE",
                        "PRECISION",
                        "LITERAL_PREFIX",
                        "LITERAL_SUFFIX",
                        "CREATE_PARAMS",
                        "NULLABLE",
                        "CASE_SENSITIVE",
                        "SEARCHABLE",
                        "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE",
                        "AUTO_INCREMENT",
                        "LOCAL_TYPE_NAME",
                        "MINIMUM_SCALE",
                        "MAXIMUM_SCALE",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "NUM_PREC_RADIX")
                .columnTypes(
                        "String",
                        "Int32",
                        "Int32",
                        "String",
                        "String",
                        "String",
                        "Int32",
                        "Int8",
                        "Int32",
                        "Int8",
                        "Int8",
                        "Int8",
                        "String",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32",
                        "Int32")
                .addRow(
                        "String", Types.VARCHAR,
                        null,       // precision - todo
                        '\'', '\'', null,
                        typeNoNulls, true, typeSearchable,
                        true,       // unsigned
                        true,       // fixed precision (money)
                        false,      //auto-incr
                        null,
                        null, null, // scale - should be fixed
                        null, null,
                        10
                );
        final int[] sizes = {8, 16, 32, 64};
        final boolean[] signed = {true, false};
        for (final int size : sizes) {
            for (boolean b : signed) {
                String name = (b ? "" : "U") + "Int" + size;
                builder.addRow(
                        name, (size <= 16 ? Types.INTEGER : Types.BIGINT),
                        null,       // precision - todo
                        null, null, null,
                        typeNoNulls, true, typePredBasic,
                        !b,         // unsigned
                        true,       // fixed precision (money)
                        false,      //auto-incr
                        null,
                        null, null, // scale - should be fixed
                        null, null,
                        10
                );
            }
        }
        final int[] floatSizes = {32, 64};
        for (final int floatSize : floatSizes) {
            final String name = "Float" + floatSize;
            builder.addRow(
                    name, Types.FLOAT,
                    null,       // precision - todo
                    null, null, null,
                    typeNoNulls, true, typePredBasic,
                    false,      // unsigned
                    true,       // fixed precision (money)
                    false,      //auto-incr
                    null,
                    null, null, // scale - should be fixed
                    null, null,
                    10);
        }
        builder.addRow(
                "Date", Types.DATE,
                null, // precision - todo
                null, null, null,
                typeNoNulls, true, typePredBasic,
                false, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10);
        builder.addRow(
                "DateTime", Types.TIMESTAMP,
                null, // precision - todo
                null, null, null,
                typeNoNulls, true, typePredBasic,
                false, // unsigned
                true, // fixed precision (money)
                false, //auto-incr
                null,
                null, null, // scale - should be fixed
                null, null,
                10);
        return builder.build();
    }

    @Override
    public ResultSet getIndexInfo(
            final String catalog,
            final String schema,
            final String table,
            final boolean unique,
            final boolean approximate
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetType(final int type) {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(final int type, final int concurrency) {
        return concurrency <= 1;
    }

    @Override
    public boolean ownUpdatesAreVisible(final int type) {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(final int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(final int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(final int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(final int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(final int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(final int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(final int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(final int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(
            final String catalog,
            final String schemaPattern,
            final String typeNamePattern,
            final int[] types
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(
            final String catalog,
            final String schemaPattern,
            final String typeNamePattern
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getAttributes(
            final String catalog,
            final String schemaPattern,
            final String typeNamePattern,
            final String attributeNamePattern
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return (int) connection.serverContext().majorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return (int) connection.serverContext().minorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return BHConstants.MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return BHConstants.MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctions(
            final String catalog,
            final String schemaPattern,
            final String functionNamePattern
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(
            final String catalog,
            final String schemaPattern,
            final String functionNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getPseudoColumns(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public Logger logger() {
        return ByteHouseDatabaseMetadata.LOG;
    }

    private ResultSet request(final String sql) throws SQLException {
        final Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    private ResultSet getEmptyResultSet() throws SQLException {
        return ByteHouseResultSetBuilder
                .builder(1, connection.serverContext())
                .cfg(connection.cfg())
                .columnNames("some")
                .columnTypes("String")
                .build();
    }

    private void buildAndCondition(
            final StringBuilder dest,
            final List<String> conditions) {
        dest.append(String.join(" AND ", conditions));
    }
}
