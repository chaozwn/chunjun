package com.dtstack.chunjun.connector.test.standalone.mysql.sync;

import com.dtstack.chunjun.connector.containers.mysql.Mysql5Container;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URISyntaxException;
import java.util.stream.Stream;

public class Mysql5SyncE2eITCase extends MysqlBaseSyncE2eITCase {
    private static final Logger LOG = LoggerFactory.getLogger(Mysql5SyncE2eITCase.class);

    private static final String MYSQL5_HOST = "chunjun-e2e-mysql5";

    private static final int MYSQL_PORT = 3306;

    protected Mysql5Container mysql5Container;

    @Before
    public void before() throws URISyntaxException, InterruptedException {
        super.before();
        LOG.info("Starting mysql5 containers...");
        mysql5Container =
                new Mysql5Container(MYSQL5_HOST)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .dependsOn(flinkStandaloneContainer);
        Startables.deepStart(Stream.of(mysql5Container)).join();
        Thread.sleep(5000);
        LOG.info("Mysql5 Containers are started.");
    }

    @After
    public void after() {
        super.after();
        if (mysql5Container != null) {
            mysql5Container.stop();
        }
    }

    @Test
    public void testMysqlToMysql() throws Exception {
        super.testMysqlToMysql();
    }
}
