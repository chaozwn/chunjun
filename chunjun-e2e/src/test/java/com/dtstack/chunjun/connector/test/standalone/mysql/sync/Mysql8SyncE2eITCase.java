package com.dtstack.chunjun.connector.test.standalone.mysql.sync;

import com.dtstack.chunjun.connector.containers.mysql.Mysql8Container;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URISyntaxException;
import java.util.stream.Stream;

public class Mysql8SyncE2eITCase extends MysqlBaseSyncE2eITCase {
    private static final Logger LOG = LoggerFactory.getLogger(Mysql8SyncE2eITCase.class);

    private static final String MYSQL8_HOST = "chunjun-e2e-mysql8";

    private static final int MYSQL_PORT = 3306;

    protected Mysql8Container mysql8Container;

    @Before
    public void before() throws URISyntaxException, InterruptedException {
        super.before();
        LOG.info("Starting mysql8 containers...");
        mysql8Container =
                new Mysql8Container(MYSQL8_HOST)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .dependsOn(flinkStandaloneContainer);
        Startables.deepStart(Stream.of(mysql8Container)).join();
        Thread.sleep(5000);
        LOG.info("Mysql8 Containers are started.");
    }

    @After
    public void after() {
        super.after();
        if (mysql8Container != null) {
            mysql8Container.stop();
        }
    }

    @Test
    public void testMysqlToMysql() throws Exception {
        super.testMysqlToMysql();
    }
}
