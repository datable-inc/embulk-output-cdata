package org.embulk.output.cdata;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CDataPageOutputForUpsert extends CDataPageOutputForUpsertBase {

    private final String INSERT_TEMP_TABLE = super.getTask().getTable() + "#TEMP";

    private static final Logger logger = LoggerFactory.getLogger(CDataPageOutputForUpsert.class);

    public CDataPageOutputForUpsert(PageReader reader, Connection conn, CDataOutputPlugin.PluginTask task) {
        super(reader, conn, task);
    }

    protected List<String> createColumns() {
        List<String> columnNames = getPageReader().getSchema().getColumns().stream()
                .map(Column::getName).collect(Collectors.toCollection(ArrayList::new));

        return columnNames;
    }

    protected List<String> createPlaceHolders() {
        List<String> preparedValues = getPageReader().getSchema().getColumns().stream()
                .map(it -> "?").collect(Collectors.toCollection(ArrayList::new));
        return preparedValues;
    }

    protected String createUpsertQuery(String tableName, List<String> columnNames) {
        return "UPSERT INTO `" + tableName + "` (" +
                columnNames.stream().collect(Collectors.joining("`, `", "`", "`")) +
                ") SELECT " +
                columnNames.stream().collect(Collectors.joining("`, `", "`", "`")) +
                " FROM `" + INSERT_TEMP_TABLE + "`";
    }

    protected ExecutedInsertResult executeInsert(List<String> columnNames, List<String> preparedValues) throws SQLException {
        String insertStatement = createInsertQuery(INSERT_TEMP_TABLE, columnNames, preparedValues);
        logger.info(insertStatement);

        PageReader pageReader = getPageReader();
        Connection conn = getConnection();
        CDataOutputPlugin.PluginTask task = getTask();
        logger.info("ExternalIdColumn:" + task.getExternalIdColumn());

        ExecutedInsertResult result = new ExecutedInsertResult();

        while (pageReader.nextRecord()) {
            try {
                result.selectUpsertRecordCount++;
                PreparedStatement preparedStatement = conn.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);

                pageReader.getSchema().visitColumns(createColumnVisitor(preparedStatement));
                preparedStatement.setString(preparedValues.size(), task.getExternalIdColumn());
                preparedStatement.executeUpdate();

                logger.info("inserted to " + INSERT_TEMP_TABLE);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    protected String executeUpsert(String tableName, List<String> columnNames, ExecutedInsertResult result) throws SQLException {
        String upsertStatement = createUpsertQuery(tableName, columnNames);
        getConnection().createStatement().executeUpdate(upsertStatement, Statement.RETURN_GENERATED_KEYS);
        return upsertStatement;
    }
}
