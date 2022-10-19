package org.embulk.output.cdata;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class CDataPageOutputForManualUpsert extends CDataPageOutputForUpsertBase {
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
        String externalIdColumn = getTask().getExternalIdColumn();

        try {
            // split insert to InsertTemp#TEMP and UpdateTemp#TEMP
            ResultSet resultSet = selectRecordAll(getTask().getTable());
            Map<String, String> externalIdValueAndPrimaryKeyValueMap = toIds(
                    externalIdColumn,
                    getTask().getDefaultPrimaryKey(),
                    resultSet);

            while (getPageReader().nextRecord()) {

                int externalIdColumnIndex = columnNames.indexOf(externalIdColumn);
                if (externalIdColumnIndex == -1) {
                    throw new RuntimeException("ExternalIdColumn is not found.");
                }

                String externalIdValue = getPageReader().getString(externalIdColumnIndex);
                if (externalIdValueAndPrimaryKeyValueMap.containsKey(externalIdValue)) {
                    String PrimaryKeyColumnValue = externalIdValueAndPrimaryKeyValueMap.get(externalIdValue);

                    List<String> updateColumnNames = new ArrayList<>(columnNames);
                    updateColumnNames.add(getTask().getDefaultPrimaryKey());

                    List<String> updatePreparedValues = new ArrayList<>(preparedValues);
                    updatePreparedValues.add(PrimaryKeyColumnValue);

                    String updateStatement = createInsertQuery(UPDATE_TEMP_TABLE, updateColumnNames, updatePreparedValues);

                    // already record, insert to UpdateTemp#TEMP
                    try (PreparedStatement updatePreparedStatement = getConnection().prepareStatement(updateStatement, Statement.RETURN_GENERATED_KEYS)) {
                        getPageReader().getSchema().visitColumns(createColumnVisitor(updatePreparedStatement));
                        updatePreparedStatement.executeUpdate();
                    }

                    logger.info("inserted to " + UPDATE_TEMP_TABLE);

                } else {
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
