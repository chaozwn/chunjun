package com.dtstack.chunjun.connector.containers.mysql;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class Mysql5Container extends MysqlBaseContainer {
    private static final URL MYSQL5_DOCKERFILE =
        MysqlBaseContainer.class.getClassLoader().getResource("docker/mysql/Mysql5Dockerfile");

    private static final String MYSQL5_HOST = "chunjun-e2e-mysql5";

    public Mysql5Container() throws URISyntaxException {
        super(MYSQL5_HOST, Paths.get(MYSQL5_DOCKERFILE.toURI()));
    }
}
