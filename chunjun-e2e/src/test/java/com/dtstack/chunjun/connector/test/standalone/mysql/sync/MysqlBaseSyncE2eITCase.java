package com.dtstack.chunjun.connector.test.standalone.mysql.sync;

import com.dtstack.chunjun.connector.ChunjunBaseE2eTest;
import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class MysqlBaseSyncE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlBaseSyncE2eITCase.class);

    protected static final String MYSQL_HOST = "chunjun-e2e-mysql";

    public void testMysqlToMysql() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunBaseE2eTest.CHUNJUN_HOME
                        + "/chunjun-examples/json/mysql/mysql_mysql_batch.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));

        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 30);
    }
}
