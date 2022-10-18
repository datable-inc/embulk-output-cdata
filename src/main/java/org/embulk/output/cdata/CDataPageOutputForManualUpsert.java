package org.embulk.output.cdata;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class CDataPageOutputForManualUpsert extends CDataPageOutputForUpsertBase {

    private final String TEMP_ALL_RECORDS_TABLE = "TempAllRecords#TEMP";
    private final String INSERT_TEMP_TABLE = "InsertTemp#TEMP";
    private final String UPDATE_TEMP_TABLE = "UpdateTemp#TEMP";

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

    protected void executeInsert(List<String> columnNames, List<String> preparedValues) {
        String tempInsertStatement = createInsertQuery(TEMP_ALL_RECORDS_TABLE, columnNames, preparedValues);
        String externalIdColumn = getTask().getExternalIdColumn();

        // insert to TempAllRecords#TEMP
        while (getPageReader().nextRecord()) {
            try {
                try (PreparedStatement tempInsertPreparedStatement = getConnection().prepareStatement(tempInsertStatement, Statement.RETURN_GENERATED_KEYS)) {
                    getPageReader().getSchema().visitColumns(createColumnVisitor(tempInsertPreparedStatement));
                    tempInsertPreparedStatement.executeUpdate();
                }

                logger.info("inserted to Temp#TEMP");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            // split insert to InsertTemp#TEMP and UpdateTemp#TEMP
            ResultSet resultSet = selectRecordAll(getTask().getTable());
            Map<String, String> externalIdList = toIds(externalIdColumn, getTask().getDefaultPrimaryKey(), resultSet);

            ResultSet tempAllRecordsResultSet = selectRecordAll(TEMP_ALL_RECORDS_TABLE);

            while (tempAllRecordsResultSet.next()) {
                if (externalIdList.containsKey(tempAllRecordsResultSet.getString(externalIdColumn))) {
                    String PrimaryKeyColumnValue = externalIdList.get(tempAllRecordsResultSet.getString(externalIdColumn));

                    List<String> updateColumnNames = new ArrayList<>(columnNames);
                    updateColumnNames.add(getTask().getDefaultPrimaryKey());

                    List<String> updatePreparedValues = new ArrayList<>(preparedValues);
                    updatePreparedValues.add(PrimaryKeyColumnValue);

                    String updateStatement = createInsertQuery(UPDATE_TEMP_TABLE, updateColumnNames, updatePreparedValues);

                    // already record, insert to UpdateTemp#TEMP
                    try (PreparedStatement updatePreparedStatement = getConnection().prepareStatement(updateStatement, Statement.RETURN_GENERATED_KEYS)) {
                        int index = 1;
                        for (String columnName : columnNames) {
                            updatePreparedStatement.setObject(index, tempAllRecordsResultSet.getObject(index));
                            index++;
                        }
                        updatePreparedStatement.executeUpdate();
                    }

                } else {
                    // new record, insert to InsertTemp#TEMP
                    String insertStatement = createInsertQuery(INSERT_TEMP_TABLE, columnNames, preparedValues);
                    try (PreparedStatement insertPreparedStatement = getConnection().prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS)) {
                        int index = 1;
                        for (String columnName : columnNames) {
                            insertPreparedStatement.setObject(index, tempAllRecordsResultSet.getObject(index));
                            index++;
                        }
                        insertPreparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    protected String executeUpsert(String tableName, List<String> columnNames) throws SQLException {

        String insertIntoSelectQuery = createInsertIntoSelectQuery(tableName, columnNames);
        String updateIntoSelectQuery = createUpdateIntoSelectQuery(tableName, columnNames);

        getConnection()
                .createStatement()
                .executeUpdate(insertIntoSelectQuery, Statement.RETURN_GENERATED_KEYS);

        getConnection()
                .createStatement()
                .executeUpdate(updateIntoSelectQuery, Statement.RETURN_GENERATED_KEYS);

        return insertIntoSelectQuery + " , and,  " + updateIntoSelectQuery;
    }

    protected ResultSet selectRecordAll(String tableName) {
        try {
            Statement selectStatement = getConnection().createStatement();
            return selectStatement.executeQuery("SELECT * FROM " + tableName);

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
