package org.embulk.output.cdata;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class CDataPageOutputForManualUpsert extends CDataPageOutputForUpsertBase {
    private final String INSERT_TEMP_TABLE = super.getTask().getTable() + "#TEMP";
    private final String UPDATE_TEMP_TABLE = super.getTask().getTable() + "#TEMP";

    private static final Logger logger = LoggerFactory.getLogger(CDataPageOutputForManualUpsert.class);

    public CDataPageOutputForManualUpsert(PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
        super(reader, conn, task);
    }

    protected List<String> createColumns() {
        return getPageReader().getSchema().getColumns().stream()
                .map(Column::getName).collect(Collectors.toCollection(ArrayList::new));
    }

    protected List<String> createPlaceHolders() {
        return getPageReader().getSchema().getColumns().stream()
                .map(it -> "?").collect(Collectors.toCollection(ArrayList::new));
    }

    protected ExecutedInsertResult executeInsert(List<String> columnNames, List<String> preparedValues) {
        String externalIdColumn = getTask().getExternalIdColumn();
        String primaryKeyColumn = getTask().getDefaultPrimaryKey();
        ExecutedInsertResult result = new ExecutedInsertResult();

        try {
            // split insert to InsertTemp#TEMP and UpdateTemp#TEMP
            List<String> extractColumnNames = new ArrayList<>(Arrays.asList(externalIdColumn, primaryKeyColumn));
            if (Objects.equals(externalIdColumn, primaryKeyColumn)) {
                extractColumnNames = new ArrayList<>(Collections.singletonList(externalIdColumn));
            }

            ResultSet resultSet = selectRecordAll(getTask().getTable(), extractColumnNames);
            Map<String, String> externalIdValueAndPrimaryKeyValueMap = toIds(
                    externalIdColumn,
                    primaryKeyColumn,
                    resultSet);

            while (getPageReader().nextRecord()) {

                int externalIdColumnIndex = columnNames.indexOf(externalIdColumn);
                if (externalIdColumnIndex == -1) {
                    throw new RuntimeException("ExternalIdColumn is not found.");
                }

                String externalIdValue = getPageReader().getString(externalIdColumnIndex);
                if (externalIdValueAndPrimaryKeyValueMap.containsKey(externalIdValue)) {
                    result.selectUpdateRecordCount++;
                    String PrimaryKeyColumnValue = externalIdValueAndPrimaryKeyValueMap.get(externalIdValue);

                    List<String> updateColumnNames = new ArrayList<>(columnNames);
                    List<String> updatePreparedValues = new ArrayList<>(preparedValues);

                    boolean mustBeAddPrimaryKey = !Objects.equals(getTask().getDefaultPrimaryKey(), getTask().getExternalIdColumn());
                    if (mustBeAddPrimaryKey) {
                        updateColumnNames.add(getTask().getDefaultPrimaryKey());
                        updatePreparedValues.add(PrimaryKeyColumnValue);
                    }

                    String updateStatement = createInsertQuery(UPDATE_TEMP_TABLE, updateColumnNames, updatePreparedValues);

                    // already record, insert to UpdateTemp#TEMP
                    try (PreparedStatement updatePreparedStatement = getConnection().prepareStatement(updateStatement, Statement.RETURN_GENERATED_KEYS)) {
                        getPageReader().getSchema().visitColumns(createColumnVisitor(updatePreparedStatement));
                        updatePreparedStatement.executeUpdate();
                    }

                    logger.info("inserted to " + UPDATE_TEMP_TABLE);

                } else {
                    result.selectInsertRecordCount++;
                    // new record, insert to InsertTemp#TEMP
                    String insertStatement = createInsertQuery(INSERT_TEMP_TABLE, columnNames, preparedValues);
                    try (PreparedStatement insertPreparedStatement = getConnection().prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS)) {
                        getPageReader().getSchema().visitColumns(createColumnVisitor(insertPreparedStatement));
                        insertPreparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                    logger.info("inserted to " + INSERT_TEMP_TABLE);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    protected String createInsertIntoSelectQuery(String tableName, List<String> columnNames) {
        return "INSERT INTO " + tableName + " (" +
                String.join(", ", columnNames) +
                ") SELECT " +
                String.join(", ", columnNames) +
                " FROM " + INSERT_TEMP_TABLE;
    }

    public String createUpdateIntoSelectQuery(String tableName, List<String> columnNames) {
        return "UPDATE " + tableName + " (" +
                String.join(", ", columnNames) +
                ") SELECT " +
                String.join(", ", columnNames) +
                " FROM " + UPDATE_TEMP_TABLE;
    }

    protected String executeUpsert(String tableName, List<String> columnNames, ExecutedInsertResult result) throws SQLException {

        String insertIntoSelectQuery = createInsertIntoSelectQuery(tableName, columnNames);

        boolean mustBeAddPrimaryKey = !Objects.equals(getTask().getDefaultPrimaryKey(), getTask().getExternalIdColumn());
        if (mustBeAddPrimaryKey) {
            columnNames.add(getTask().getDefaultPrimaryKey());
        }
        String updateIntoSelectQuery = createUpdateIntoSelectQuery(tableName, columnNames);

        if (result.selectInsertRecordCount > 0) {
            getConnection()
                    .createStatement()
                    .executeUpdate(insertIntoSelectQuery, Statement.RETURN_GENERATED_KEYS);
        }

        getConnection()
                .createStatement()
                .executeUpdate(updateIntoSelectQuery, Statement.RETURN_GENERATED_KEYS);

        return insertIntoSelectQuery + " , and,  " + updateIntoSelectQuery;
    }

    protected ResultSet selectRecordAll(String tableName, List<String> selectColumns) {
        try {
            Statement selectStatement = getConnection().createStatement();
            return selectStatement.executeQuery("SELECT " + String.join(", ", selectColumns) + " FROM " + tableName);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected Map<String, String> toIds(String externalIdColumnName, String defaultPrimaryKey, ResultSet resultSet) throws SQLException {
        Map<String, String> externalIdMap = new HashMap<>();
        while (resultSet.next()) {
            externalIdMap.put(resultSet.getString(externalIdColumnName), resultSet.getString(defaultPrimaryKey));
        }

        return externalIdMap;
    }

}
