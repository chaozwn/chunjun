/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.SqlUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.metrics.BigIntegerAccumulator;
import com.dtstack.chunjun.metrics.StringAccumulator;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ColumnBuildUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.StringUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.dtstack.chunjun.enums.ColumnType.TIMESTAMPTZ;

/**
 * InputFormat for reading data from a database and generate Rows.
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcInputFormat extends BaseRichInputFormat {

    public static final long serialVersionUID = 1L;
    protected static final int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    protected static int resultSetType = ResultSet.TYPE_FORWARD_ONLY;

    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;

    protected transient Connection dbConn;
    protected transient Statement statement;
    protected transient PreparedStatement ps;
    protected transient ResultSet resultSet;
    protected boolean hasNext;

    protected int columnCount;
    protected boolean needUpdateEndLocation;
    protected Object state = null;

    protected StringAccumulator maxValueAccumulator;
    protected BigIntegerAccumulator endLocationAccumulator;
    protected BigIntegerAccumulator startLocationAccumulator;

    // 轮询增量标识字段类型
    protected ColumnType type;

    protected JdbcInputSplit currentJdbcInputSplit;

    @Override
    public void openInternal(InputSplit inputSplit) {
        this.currentJdbcInputSplit = (JdbcInputSplit) inputSplit;
        initMetric(currentJdbcInputSplit);
        if (!canReadData(currentJdbcInputSplit)) {
            LOG.warn(
                    "Not read data when the start location are equal to end location, start = {}, end = {}",
                    currentJdbcInputSplit.getStartLocation(),
                    currentJdbcInputSplit.getEndLocation());
            hasNext = false;
            return;
        }

        String querySQL = null;
        try {
            dbConn = getConnection();
            dbConn.setAutoCommit(false);

            Pair<List<String>, List<String>> pair = null;
            List<String> fullColumnList = new LinkedList<>();
            List<String> fullColumnTypeList = new LinkedList<>();
            if (StringUtils.isBlank(jdbcConf.getCustomSql())) {
                pair = getTableMetaData();
                fullColumnList = pair.getLeft();
                fullColumnTypeList = pair.getRight();
            }
            Pair<List<String>, List<String>> columnPair =
                    ColumnBuildUtil.handleColumnList(
                            jdbcConf.getColumn(), fullColumnList, fullColumnTypeList);
            columnNameList = columnPair.getLeft();
            columnTypeList = columnPair.getRight();

            querySQL = buildQuerySql(currentJdbcInputSplit);
            jdbcConf.setQuerySql(querySQL);
            executeQuery(currentJdbcInputSplit.getStartLocation());
            if (!resultSet.isClosed()) {
                columnCount = resultSet.getMetaData().getColumnCount();
            }
            // 增量任务
            needUpdateEndLocation =
                    jdbcConf.isIncrement() && !jdbcConf.isPolling() && !jdbcConf.isUseMaxFunc();
            RowType rowType =
                    TableUtil.createRowType(
                            columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
            setRowConverter(
                    rowConverter == null
                            ? jdbcDialect.getColumnConverter(rowType, jdbcConf)
                            : rowConverter);
        } catch (SQLException se) {
            String expMsg = se.getMessage();
            expMsg = querySQL == null ? expMsg : expMsg + "\n querySQL: " + querySQL;
            throw new IllegalArgumentException("open() failed." + expMsg, se);
        }
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        if (minNumSplits != jdbcConf.getParallelism()) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "numTaskVertices is [%s], but parallelism in jdbcConf is [%s]",
                            minNumSplits, jdbcConf.getParallelism()));
        }

        if (jdbcConf.getParallelism() > 1
                && StringUtils.equalsIgnoreCase("range", jdbcConf.getSplitStrategy())) {
            // splitStrategy = range
            return createSplitsInternalBySplitRange(minNumSplits);
        } else {
            // default,splitStrategy = mod
            return createSplitsInternalBySplitMod(minNumSplits, jdbcConf.getStartLocation());
        }
    }

    /** Create split for modSplitStrategy */
    public JdbcInputSplit[] createSplitsInternalBySplitMod(int minNumSplits, String startLocation) {
        JdbcInputSplit[] splits = new JdbcInputSplit[minNumSplits];
        if (StringUtils.isNotBlank(startLocation)) {
            String[] startLocations = startLocation.split(ConstantValue.COMMA_SYMBOL);
            if (startLocations.length == 1) {
                for (int i = 0; i < minNumSplits; i++) {
                    splits[i] =
                            new JdbcInputSplit(
                                    i,
                                    minNumSplits,
                                    i,
                                    startLocations[0],
                                    null,
                                    null,
                                    null,
                                    "mod",
                                    jdbcConf.isPolling());
                }
            } else if (startLocations.length != jdbcConf.getParallelism()) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "startLocations is %s, but parallelism in jdbcConf is [%s]",
                                Arrays.toString(startLocations), jdbcConf.getParallelism()));
            } else {
                for (int i = 0; i < minNumSplits; i++) {
                    splits[i] =
                            new JdbcInputSplit(
                                    i,
                                    minNumSplits,
                                    i,
                                    startLocations[i],
                                    null,
                                    null,
                                    null,
                                    "mod",
                                    jdbcConf.isPolling());
                }
            }
        } else {
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] =
                        new JdbcInputSplit(
                                i,
                                minNumSplits,
                                i,
                                null,
                                null,
                                null,
                                null,
                                "mod",
                                jdbcConf.isPolling());
            }
        }

        LOG.info("createInputSplitsInternal success, splits is {}", GsonUtil.GSON.toJson(splits));
        return splits;
    }

    @Override
    public boolean reachedEnd() {
        if (hasNext) {
            return false;
        } else {
            if (currentJdbcInputSplit.isPolling()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(jdbcConf.getPollingInterval());
                    // 间隔轮询检测数据库连接是否断开，超时时间三秒，断开后自动重连
                    if (!dbConn.isValid(3)) {
                        dbConn = getConnection();
                        // 重新连接后还是不可用则认为数据库异常，任务失败
                        if (!dbConn.isValid(3)) {
                            String message =
                                    String.format(
                                            "cannot connect to %s, username = %s, please check %s is available.",
                                            jdbcConf.getJdbcUrl(),
                                            jdbcConf.getUsername(),
                                            jdbcDialect.dialectName());
                            throw new ChunJunRuntimeException(message);
                        }
                    }
                    dbConn.setAutoCommit(true);
                    JdbcUtil.closeDbResources(resultSet, null, null, false);
                    queryForPolling(String.valueOf(state));
                    return false;
                } catch (InterruptedException e) {
                    LOG.warn(
                            "interrupted while waiting for polling, e = {}",
                            ExceptionUtil.getErrorMessage(e));
                } catch (SQLException e) {
                    JdbcUtil.closeDbResources(resultSet, ps, null, false);
                    String message =
                            String.format(
                                    "error to execute sql = %s, startLocation = %s, e = %s",
                                    jdbcConf.getQuerySql(),
                                    state,
                                    ExceptionUtil.getErrorMessage(e));
                    throw new ChunJunRuntimeException(message, e);
                }
            }
            return true;
        }
    }

    @Override
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        if (!hasNext) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            RowData finalRowData = rowConverter.toInternal(resultSet);
            if (needUpdateEndLocation) {
                Object obj;
                switch (type) {
                    case DATETIME:
                    case TIMESTAMP:
                    case TIMESTAMPTZ:
                    case DATE:
                        obj = resultSet.getTimestamp(jdbcConf.getIncreColumn()).getTime();
                        break;
                    default:
                        obj = resultSet.getObject(jdbcConf.getIncreColumn());
                }
                String location = String.valueOf(obj);
                endLocationAccumulator.add(new BigInteger(location));
                LOG.debug("update endLocationAccumulator, current Location = {}", location);
            }
            if (jdbcConf.getRestoreColumnIndex() > -1) {
                state = resultSet.getObject(jdbcConf.getRestoreColumnIndex() + 1);
            }
            return finalRowData;
        } catch (Exception se) {
            throw new ReadRecordException("", se, 0, rowData);
        } finally {
            try {
                hasNext = resultSet.next();
            } catch (SQLException e) {
                LOG.error("can not read next record", e);
                hasNext = false;
            }
        }
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        formatState.setState(state);
        return formatState;
    }

    @Override
    public void closeInternal() {
        JdbcUtil.closeDbResources(resultSet, statement, dbConn, true);
    }

    /**
     * 初始化增量或或间隔轮询任务累加器
     *
     * @param inputSplit 数据分片
     */
    protected void initMetric(InputSplit inputSplit) {
        if (!jdbcConf.isIncrement()) {
            return;
        }
        // 初始化增量、轮询字段类型
        type = ColumnType.fromString(jdbcConf.getIncreColumnType());
        startLocationAccumulator = new BigIntegerAccumulator();
        endLocationAccumulator = new BigIntegerAccumulator();
        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;
        String startLocation =
                StringUtil.stringToTimestampStr(jdbcInputSplit.getStartLocation(), type);

        if (StringUtils.isNotBlank(startLocation)) {
            startLocationAccumulator.add(new BigInteger(startLocation));
        }

        // 如果是增量同步
        if (!jdbcConf.isPolling()) {
            // 若useMaxFunc设置为true，endLocation设置为数据库中查询的最大值
            if (jdbcConf.isUseMaxFunc()) {
                getMaxValue(inputSplit);
                endLocationAccumulator.add(
                        new BigInteger(
                                StringUtil.stringToTimestampStr(
                                        jdbcInputSplit.getEndLocation(), type)));
            } else {
                // useMaxFunc设置为false，如果startLocation不为空，则将endLocation初始值设置为startLocation的值，防止数据库无增量数据时下次获取到的startLocation为空
                if (StringUtils.isNotEmpty(startLocation)) {
                    endLocationAccumulator.add(new BigInteger(startLocation));
                }
            }
        }

        // 将累加器信息添加至prometheus
        String start = Metrics.START_LOCATION;
        String end = Metrics.END_LOCATION;
        if (jdbcConf.getParallelism() > 1) {
            start = Metrics.START_LOCATION + "_" + inputSplit.getSplitNumber();
            end = Metrics.END_LOCATION + "_" + inputSplit.getSplitNumber();
        }

        // 将累加器信息添加至prometheus
        if (useCustomReporter()) {
            customReporter.registerMetric(startLocationAccumulator, Metrics.START_LOCATION);
            customReporter.registerMetric(endLocationAccumulator, Metrics.END_LOCATION);
        }
        if (getRuntimeContext().getAccumulator(Metrics.START_LOCATION) == null) {
            getRuntimeContext().addAccumulator(Metrics.START_LOCATION, startLocationAccumulator);
        }
        if (getRuntimeContext().getAccumulator(Metrics.END_LOCATION) == null) {
            getRuntimeContext().addAccumulator(Metrics.END_LOCATION, endLocationAccumulator);
        }
    }

    /**
     * 将增量任务的数据最大值设置到累加器中
     *
     * @param inputSplit 数据分片
     */
    protected void getMaxValue(InputSplit inputSplit) {
        String maxValue;
        if (inputSplit.getSplitNumber() == 0) {
            maxValue = getMaxValueFromDb();
            // 将累加器信息上传至flink，供其他通道通过flink rest api获取该最大值
            maxValueAccumulator = new StringAccumulator();
            maxValueAccumulator.add(maxValue);
            getRuntimeContext().addAccumulator(Metrics.MAX_VALUE, maxValueAccumulator);
        } else {
            maxValue =
                    String.valueOf(
                            accumulatorCollector.getAccumulatorValue(Metrics.MAX_VALUE, true));
        }

        ((JdbcInputSplit) inputSplit).setEndLocation(maxValue);
    }

    /**
     * 从数据库中查询增量字段的最大值
     *
     * @return
     */
    private String getMaxValueFromDb() {
        String maxValue = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            long startTime = System.currentTimeMillis();

            String queryMaxValueSql;
            if (StringUtils.isNotEmpty(jdbcConf.getCustomSql())) {
                queryMaxValueSql =
                        String.format(
                                "select max(%s.%s) as max_value from ( %s ) %s",
                                JdbcUtil.TEMPORARY_TABLE_NAME,
                                jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                                jdbcConf.getCustomSql(),
                                JdbcUtil.TEMPORARY_TABLE_NAME);
            } else {
                queryMaxValueSql =
                        String.format(
                                "select max(%s) as max_value from %s",
                                jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                                jdbcDialect.quoteIdentifier(jdbcConf.getTable()));
            }

            String startSql =
                    buildStartLocationSql(
                            jdbcConf.getIncreColumnType(),
                            jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                            jdbcConf.getStartLocation(),
                            jdbcConf.isUseMaxFunc(),
                            jdbcConf.isPolling());
            if (StringUtils.isNotEmpty(startSql)) {
                queryMaxValueSql += " where " + startSql;
            }

            LOG.info(String.format("Query max value sql is '%s'", queryMaxValueSql));

            conn = getConnection();
            st = conn.createStatement(resultSetType, resultSetConcurrency);
            st.setQueryTimeout(jdbcConf.getQueryTimeOut());
            rs = st.executeQuery(queryMaxValueSql);
            if (rs.next()) {
                switch (type) {
                    case TIMESTAMP:
                        maxValue = String.valueOf(rs.getTimestamp("max_value").getTime());
                        break;
                    case DATE:
                        maxValue = String.valueOf(rs.getDate("max_value").getTime());
                        break;
                    default:
                        maxValue =
                                StringUtil.stringToTimestampStr(
                                        String.valueOf(rs.getObject("max_value")), type);
                }
            }

            LOG.info(
                    String.format(
                            "Takes [%s] milliseconds to get the maximum value [%s]",
                            System.currentTimeMillis() - startTime, maxValue));

            return maxValue;
        } catch (Throwable e) {
            throw new RuntimeException("Get max value from " + jdbcConf.getTable() + " error", e);
        } finally {
            JdbcUtil.closeDbResources(rs, st, conn, false);
        }
    }

    /**
     * 从数据库中查询切割键的最大 最小值
     *
     * @return
     */
    private Pair<String, String> getSplitRangeFromDb() {
        Pair<String, String> splitPkRange = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            long startTime = System.currentTimeMillis();

            String querySplitRangeSql = SqlUtil.buildQuerySplitRangeSql(jdbcConf, jdbcDialect);
            LOG.info(String.format("Query SplitRange sql is '%s'", querySplitRangeSql));

            conn = getConnection();
            st = conn.createStatement(resultSetType, resultSetConcurrency);
            st.setQueryTimeout(jdbcConf.getQueryTimeOut());
            rs = st.executeQuery(querySplitRangeSql);
            if (rs.next()) {
                splitPkRange =
                        Pair.of(
                                String.valueOf(rs.getObject("min_value")),
                                String.valueOf(rs.getObject("max_value")));
            }

            LOG.info(
                    String.format(
                            "Takes [%s] milliseconds to get the SplitRange value [%s]",
                            System.currentTimeMillis() - startTime, splitPkRange));

            return splitPkRange;
        } catch (Throwable e) {
            throw new ChunJunRuntimeException(
                    "Get SplitRange value from " + jdbcConf.getTable() + " error", e);
        } finally {
            JdbcUtil.closeDbResources(rs, st, conn, false);
        }
    }

    /**
     * 判断增量任务是否还能继续读取数据 增量任务，startLocation = endLocation且两者都不为null，返回false，其余情况返回true
     *
     * @param jdbcInputSplit 数据分片
     * @return
     */
    protected boolean canReadData(JdbcInputSplit jdbcInputSplit) {
        // 只排除增量同步
        if (!jdbcConf.isIncrement() || currentJdbcInputSplit.isPolling()) {
            return true;
        }

        if (jdbcInputSplit.getStartLocation() == null && jdbcInputSplit.getEndLocation() == null) {
            return true;
        }

        return !StringUtils.equals(
                jdbcInputSplit.getStartLocation(), jdbcInputSplit.getEndLocation());
    }

    /**
     * 构造查询sql
     *
     * @param inputSplit 数据切片
     * @return 构建的sql字符串
     */
    protected String buildQuerySql(InputSplit inputSplit) {
        List<String> whereList = new ArrayList<>();

        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;
        buildLocationFilter(jdbcInputSplit, whereList);

        if (StringUtils.isNotBlank(jdbcConf.getWhere())) {
            whereList.add(jdbcConf.getWhere());
        }
        String querySql;

        querySql = buildQuerySqlBySplit(jdbcInputSplit, whereList);

        if (!jdbcConf.isPolling()) {
            querySql = querySql + SqlUtil.buildOrderSql(jdbcConf, jdbcDialect, "ASC");
        }
        LOG.info("Executing sql is: '{}'", querySql);
        return querySql;
    }

    /**
     * 构建起始位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol 增量字段名称
     * @param startLocation 开始位置
     * @param useMaxFunc 是否保存结束位置数据
     * @return
     */
    public String buildStartLocationSql(
            String incrementColType,
            String incrementCol,
            String startLocation,
            boolean useMaxFunc,
            boolean isPolling) {
        if (org.apache.commons.lang.StringUtils.isEmpty(startLocation)
                || JdbcUtil.NULL_STRING.equalsIgnoreCase(startLocation)) {
            return null;
        }

        String operator = useMaxFunc ? " >= " : " > ";

        // 增量轮询，startLocation使用占位符代替
        if (isPolling) {
            return incrementCol + operator + "?";
        }

        return getLocationSql(incrementColType, incrementCol, startLocation, operator);
    }

    /**
     * 构建边界位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol 增量字段名称
     * @param location 边界位置(起始/结束)
     * @param operator 判断符( >, >=, <)
     * @return
     */
    protected String getLocationSql(
            String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;
        ColumnType type = ColumnType.fromString(incrementColType);
        if (type == TIMESTAMPTZ) {
            return String.valueOf(Timestamp.valueOf(location).getTime());
        }
        if (ColumnType.isTimeType(incrementColType)) {
            endTimeStr = getTimeStr(Long.parseLong(location));
            endLocationSql = incrementCol + operator + endTimeStr;
        } else if (ColumnType.isNumberType(incrementColType)) {
            endLocationSql = incrementCol + operator + location;
        } else {
            endTimeStr = String.format("'%s'", location);
            endLocationSql = incrementCol + operator + endTimeStr;
        }

        return endLocationSql;
    }

    /**
     * 构建时间边界字符串
     *
     * @param location 边界位置(起始/结束)
     * @return
     */
    protected String getTimeStr(Long location) {
        String timeStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(location));
        ts.setNanos(JdbcUtil.getNanos(location));
        timeStr = JdbcUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 26);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }

    /**
     * 增量轮询查询
     *
     * @param startLocation
     * @throws SQLException
     */
    protected void queryForPolling(String startLocation) throws SQLException {
        // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
        if ((System.currentTimeMillis() - startTime) % 300000 <= jdbcConf.getPollingInterval()) {
            LOG.info("polling startLocation = {}", startLocation);
        } else {
            LOG.debug("polling startLocation = {}", startLocation);
        }

        boolean isNumber = StringUtils.isNumeric(startLocation);
        switch (type) {
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case DATETIME:
                Timestamp ts =
                        isNumber
                                ? new Timestamp(Long.parseLong(startLocation))
                                : Timestamp.valueOf(startLocation);
                ps.setTimestamp(1, ts);
                break;
            case DATE:
                Date date =
                        isNumber
                                ? new Date(Long.parseLong(startLocation))
                                : Date.valueOf(startLocation);
                ps.setDate(1, date);
                break;
            default:
                if (isNumber) {
                    ps.setLong(1, Long.parseLong(startLocation));
                } else {
                    ps.setString(1, startLocation);
                }
        }
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
    }

    /** 构建基于startLocation&endLocation的过滤条件 * */
    protected void buildLocationFilter(JdbcInputSplit jdbcInputSplit, List<String> whereList) {
        String startLocation = jdbcInputSplit.getStartLocation();
        if (formatState.getState() != null && StringUtils.isNotBlank(jdbcConf.getRestoreColumn())) {
            startLocation = String.valueOf(formatState.getState());
            if (StringUtils.isNotBlank(startLocation)) {
                LOG.info(
                        "restore from checkpoint, update startLocation, before = {}, after = {}",
                        jdbcInputSplit.getStartLocation(),
                        startLocation);
                jdbcInputSplit.setStartLocation(startLocation);
                whereList.add(
                        buildFilterSql(
                                jdbcConf.getCustomSql(),
                                ">",
                                startLocation,
                                jdbcDialect.quoteIdentifier(jdbcConf.getRestoreColumn()),
                                jdbcConf.getRestoreColumnType(),
                                jdbcInputSplit.isPolling(),
                                this::getTimeStr));
            }
        } else if (jdbcConf.isIncrement()) {
            if (StringUtils.isNotBlank(startLocation)) {
                String operator = jdbcConf.isUseMaxFunc() ? " >= " : " > ";
                whereList.add(
                        buildFilterSql(
                                jdbcConf.getCustomSql(),
                                operator,
                                startLocation,
                                jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                                jdbcConf.getIncreColumnType(),
                                jdbcInputSplit.isPolling(),
                                this::getTimeStr));
            }
            if (StringUtils.isNotBlank(jdbcInputSplit.getEndLocation())) {
                whereList.add(
                        buildFilterSql(
                                jdbcConf.getCustomSql(),
                                "<",
                                jdbcInputSplit.getEndLocation(),
                                jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()),
                                jdbcConf.getIncreColumnType(),
                                false,
                                this::getTimeStr));
            }
        }
    }

    /** create querySql for inputSplit * */
    protected String buildQuerySqlBySplit(JdbcInputSplit jdbcInputSplit, List<String> whereList) {
        return SqlUtil.buildQuerySqlBySplit(
                jdbcConf, jdbcDialect, whereList, columnNameList, jdbcInputSplit);
    }

    /** create split for rangeSplitStrategy */
    protected JdbcInputSplit[] createSplitsInternalBySplitRange(int minNumSplits) {
        List<JdbcInputSplit> splits = new ArrayList<>();
        Pair<String, String> splitRangeFromDb = getSplitRangeFromDb();
        BigDecimal left, right = null;
        if (StringUtils.isNotBlank(splitRangeFromDb.getLeft())
                && !"null".equalsIgnoreCase(splitRangeFromDb.getLeft())) {
            left = new BigDecimal(splitRangeFromDb.getLeft());
            right = new BigDecimal(splitRangeFromDb.getRight());
            splits.addAll(createRangeSplits(left, right, minNumSplits));
            if (jdbcConf.isPolling()) {
                // rangeSplit in polling mode,range first then mod.we need to change the last range
                // shard here to <= endLocationOfSplit
                splits.get(splits.size() - 1).setRangeEndLocationOperator(" <= ");
            }
        }
        // create modSplit for polling
        if (jdbcConf.isPolling()) {
            splits.addAll(
                    Arrays.asList(
                            createSplitsInternalBySplitMod(
                                    jdbcConf.getParallelism(),
                                    right == null
                                            ? jdbcConf.getStartLocation()
                                            : String.valueOf(right))));
        }
        // Reverse,when pollingMode configures rangeStrategy, be sure to do rangeSplit first, then
        // modSplit
        Collections.reverse(splits);
        return splits.toArray(new JdbcInputSplit[0]);
    }

    protected List<JdbcInputSplit> createRangeSplits(
            BigDecimal left, BigDecimal right, int minNumSplits) {
        BigDecimal endAndStartGap = right.subtract(left);
        if (endAndStartGap.compareTo(BigDecimal.ZERO) < 0) return new ArrayList<>();
        JdbcInputSplit[] splits;
        LOG.info("create splitsInternal,the splitKey range is {} --> {}", left, right);
        BigDecimal remainder = endAndStartGap.remainder(new BigDecimal(minNumSplits));
        endAndStartGap = endAndStartGap.subtract(remainder);
        BigDecimal step = endAndStartGap.divide(new BigDecimal(minNumSplits));

        if (step.compareTo(BigDecimal.ZERO) == 0) {
            // if left = right，step and remainder is 0
            if (remainder.compareTo(BigDecimal.ZERO) == 0) {
                minNumSplits = 1;
            } else {
                minNumSplits = remainder.intValue();
            }
        }

        splits = new JdbcInputSplit[minNumSplits];
        BigDecimal start;
        BigDecimal end = left;
        for (int i = 0; i < minNumSplits; i++) {
            start = end;
            end = start.add(step);
            if (remainder.compareTo(BigDecimal.ZERO) > 0) {
                end = end.add(BigDecimal.ONE);
                remainder = remainder.subtract(BigDecimal.ONE);
            }
            // incrementalMode,The final rangeSplit scope is splitPk >= start
            // pollingMode,The final rangeSplit scope is splitPk >= start and splitPk < =end
            if (i == minNumSplits - 1) {
                if (jdbcConf.isPolling()) {
                    end = right;
                } else {
                    end = null;
                }
            }
            splits[i] =
                    new JdbcInputSplit(
                            i,
                            minNumSplits,
                            i,
                            jdbcConf.getStartLocation(),
                            null,
                            start.toString(),
                            Objects.isNull(end) ? null : end.toString(),
                            "range",
                            false);
        }

        return Arrays.asList(splits);
    }

    /**
     * 执行查询
     *
     * @param startLocation
     * @throws SQLException
     */
    protected void executeQuery(String startLocation) throws SQLException {
        if (currentJdbcInputSplit.isPolling()) {
            if (StringUtils.isBlank(startLocation)) {
                queryPollingWithOutStartLocation();
                // Concatenated sql statement for next polling query
                StringBuilder builder = new StringBuilder(128);
                builder.append(jdbcConf.getQuerySql());
                if (jdbcConf.getQuerySql().contains("WHERE")) {
                    builder.append(" AND ");
                } else {
                    builder.append(" WHERE ");
                }
                builder.append(jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()))
                        .append(" > ?")
                        .append(SqlUtil.buildOrderSql(jdbcConf, jdbcDialect, "ASC"));
                jdbcConf.setQuerySql(builder.toString());
                initPrepareStatement(jdbcConf.getQuerySql());
                LOG.info("update querySql, sql = {}", jdbcConf.getQuerySql());
            } else {
                // if the job have startLocation
                // sql will be like "select ... from ... where increColumn > ?"
                jdbcConf.setQuerySql(
                        jdbcConf.getQuerySql()
                                + SqlUtil.buildOrderSql(jdbcConf, jdbcDialect, "ASC"));
                initPrepareStatement(jdbcConf.getQuerySql());
                queryForPolling(startLocation);
                state = startLocation;
            }
        } else {
            statement = dbConn.createStatement(resultSetType, resultSetConcurrency);
            statement.setFetchSize(jdbcConf.getFetchSize());
            statement.setQueryTimeout(jdbcConf.getQueryTimeOut());
            resultSet = statement.executeQuery(jdbcConf.getQuerySql());
            hasNext = resultSet.next();
        }
    }

    /**
     * for override. because some databases have case-sensitive metadata。
     *
     * @return
     */
    protected Pair<List<String>, List<String>> getTableMetaData() {
        return JdbcUtil.getTableMetaData(null, jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
    }
    /** init prepareStatement */
    public void initPrepareStatement(String querySql) throws SQLException {
        ps = dbConn.prepareStatement(querySql, resultSetType, resultSetConcurrency);
        ps.setFetchSize(jdbcConf.getFetchSize());
        ps.setQueryTimeout(jdbcConf.getQueryTimeOut());
    }
    /**
     * polling mode first query when startLocation is not set
     *
     * @throws SQLException
     */
    protected void queryPollingWithOutStartLocation() throws SQLException {
        // add order by to query SQL avoid duplicate data
        initPrepareStatement(
                jdbcConf.getQuerySql() + SqlUtil.buildOrderSql(jdbcConf, jdbcDialect, "ASC"));
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();

        try {
            // 间隔轮询一直循环，直到查询到数据库中的数据为止
            while (!hasNext) {
                TimeUnit.MILLISECONDS.sleep(jdbcConf.getPollingInterval());
                resultSet.close();
                // 如果事务不提交 就会导致数据库即使插入数据 也无法读到数据
                dbConn.commit();
                resultSet = ps.executeQuery();
                hasNext = resultSet.next();
                // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
                if ((System.currentTimeMillis() - startTime) % 300000
                        <= jdbcConf.getPollingInterval()) {
                    LOG.info(
                            "no record matched condition in database, execute query sql = {}, startLocation = {}",
                            jdbcConf.getQuerySql(),
                            endLocationAccumulator.getLocalValue());
                }
            }
        } catch (InterruptedException e) {
            LOG.warn(
                    "interrupted while waiting for polling, e = {}",
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 构造过滤条件SQL
     *
     * @param operator 比较符
     * @param location 比较的值
     * @param columnName 字段名称
     * @param columnType 字段类型
     * @param isPolling 是否是轮询任务
     * @return
     */
    public String buildFilterSql(
            String customSql,
            String operator,
            String location,
            String columnName,
            String columnType,
            boolean isPolling,
            Function<Long, String> function) {
        StringBuilder sql = new StringBuilder(64);
        if (StringUtils.isNotEmpty(customSql)) {
            sql.append(JdbcUtil.TEMPORARY_TABLE_NAME).append(".");
        }
        sql.append(columnName).append(" ").append(operator).append(" ");
        if (isPolling) {
            // 轮询任务使用占位符
            sql.append("?");
        } else {
            sql.append(buildLocation(columnType, location, function));
        }

        return sql.toString();
    }

    /**
     * buildLocation
     *
     * @param columnType
     * @param location
     * @return
     */
    public String buildLocation(
            String columnType, String location, Function<Long, String> function) {
        if (ColumnType.isTimeType(columnType)) {
            return function.apply(Long.parseLong(location));
        } else if (ColumnType.isNumberType(columnType)) {
            return location;
        } else {
            return "'" + location + "'";
        }
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    protected Connection getConnection() throws SQLException {
        return JdbcUtil.getConnection(jdbcConf, jdbcDialect);
    }

    /** 使用自定义的指标输出器把增量指标打到普罗米修斯 */
    @Override
    protected boolean useCustomReporter() {
        return jdbcConf.isIncrement() && jdbcConf.getInitReporter();
    }

    /** 为了保证增量数据的准确性，指标输出失败时使任务失败 */
    @Override
    protected boolean makeTaskFailedWhenReportFailed() {
        return true;
    }

    public JdbcConf getJdbcConf() {
        return jdbcConf;
    }

    public void setJdbcConf(JdbcConf jdbcConf) {
        this.jdbcConf = jdbcConf;
    }

    public JdbcDialect getJdbcDialect() {
        return jdbcDialect;
    }

    public void setJdbcDialect(JdbcDialect jdbcDialect) {
        this.jdbcDialect = jdbcDialect;
    }
}
