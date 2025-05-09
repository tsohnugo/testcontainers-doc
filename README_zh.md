[English Version](README.md)

# 使用Testcontainers进行Flink集成GaussDB 测试。。
## 引言
Testcontainers是一个非常实用的工具，它可以帮助我们在测试环境中轻松地创建和管理各种容器化的服务。目前，Flink支持的数据库连接器有很多，例如 MySQL的连接器(flink-connector-jdbc-mysql)、PostgreSQL的连接器(flink-connector-jdbc-postgre)等，PostgreSQL、Oracle、MySQL等主流数据库厂商已经实现在Testcontainers测试框架内的测试，但GaussDB还未实现；我们在向Flink提供支持GaussDB的数据库连接器(flink-connector-jdbc-gaussdb)时需要按照社区规范使用Testcontainers测试框架同步实现对GaussDB的测试，这样一来GaussDB的数据库连接器(flink-connector-jdbc-gaussdb)才能顺利合入到开源社区。
## 一、Testcontainers简介
Testcontainers是一个开源库，用于提供数据库、消息代理、web浏览器或几乎任何可以在Docker容器中运行的东西的一次性轻量级实例。不再需要模拟或复杂的环境配置。将测试依赖关系定义为代码，创建容器，然后简单地运行测试，最后清理容器。Testcontainers支持多种语言和测试框架，只需要安装Docker。  
## 二、Testcontainers实现支持GaussDB测试

1. **准备工作**：需要安装docker。安装教程地址：[10分钟学会Docker的安装和使用](https://blog.csdn.net/baidu_36511315/article/details/108117826)
2. **添加依赖**：首先，在我们的项目中添加 Testcontainers 和 GaussDB 相关的依赖。如果是 Maven 项目，在 `pom.xml` 中添加以下依赖：
```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.21.0</version>
</dependency>
<dependency>
    <groupId>com.huaweicloud.gaussdb</groupId>
    <artifactId>gaussdbjdbc</artifactId>
    <version>506.0.0.b058</version>
</dependency>
```
3. **创建 GaussDBContainer 容器类**：
```
1.参考PostgreSQLContainer结构，定义GaussDBContainer类，继承自GenericContainer。
2.根据需求，设置GaussDB的Docker镜像名称、暴露的端口、等待策略、用户名密码等信息。
3.提供了获取JDBC URL、用户名和密码的方法。
```
代码地址：[GaussDBContainer](https://github.com/tsohnugo/testcontainers-java/blob/main/modules/gaussdb/src/main/java/org/testcontainers/containers/GaussDBContainer.java)

## 三、使用创建的GaussDBContainer进行Flink测试
### 3.1 **测试流程**
![image](image/image-1.png)

### 3.2 **编写 Flink 测试代码**  
#### 3.2.1 `GaussdbDatabase`类
基于Docker容器创建了一个可用于测试的GaussDB数据库实例，包含了于测试数据库连接参数等，并且在获取时会检查数据库容器的运行状态。代码如下：
```java
package org.apache.flink.connector.jdbc.gaussdb.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.utility.DockerImageName;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A Gaussdb database for testing.
 *
 * <p>Notes: The source code is based on PostgresDatabase.
 */
public class GaussdbDatabase extends DatabaseExtension implements GaussdbImages {

    private static final GaussDBContainer<?> CONTAINER =
            new GaussdbXaContainer(IMAGE).withMaxConnections(10).withMaxTransactions(50);

    private static GaussdbMetadata metadata;

    public static GaussdbMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new GaussdbMetadata(CONTAINER, true);
        }
        return metadata;
    }

    protected DatabaseMetadata getMetadataDB() {
        return getMetadata();
    }

    @Override
    protected DatabaseResource getResource() {
        return new DockerResource(CONTAINER);
    }

    /** {@link GaussDBContainer} with XA enabled (by setting max_prepared_transactions). */
    public static class GaussdbXaContainer extends GaussDBContainer<GaussdbXaContainer> {
        private static final int SUPERUSER_RESERVED_CONNECTIONS = 1;
        private int maxConnections = SUPERUSER_RESERVED_CONNECTIONS + 1;
        private int maxTransactions = 1;

        public GaussdbXaContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        public GaussdbXaContainer withMaxConnections(int maxConnections) {
            checkArgument(
                    maxConnections > SUPERUSER_RESERVED_CONNECTIONS,
                    "maxConnections should be greater than superuser_reserved_connections");
            this.maxConnections = maxConnections;
            return this.self();
        }

        public GaussdbXaContainer withMaxTransactions(int maxTransactions) {
            checkArgument(maxTransactions > 1, "maxTransactions should be greater 1");
            this.maxTransactions = maxTransactions;
            return this.self();
        }

        @Override
        public void start() {
            super.start();
        }
    }
}
```

#### 3.2.2 `GaussdbFinkTest`类
主要是搭建了测试环境，初始化一系列用于测试相关的配置和资源，创建测试所需要的资源，该类以创建数据库并验证是否存在测试进行举例。代码如下：
```java
package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.connector.jdbc.gaussdb.GaussdbTestBase;
import org.apache.flink.connector.jdbc.gaussdb.testutils.GaussdbDatabase;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class GaussdbFlinkTest implements JdbcITCaseBase, GaussdbTestBase {

    private static DatabaseMetadata getStaticMetadata() {
        return GaussdbDatabase.getMetadata();
    }

    protected static GaussdbCatalog catalog;
    protected static final String TEST_CATALOG_NAME = "mypg";
    protected static final String TEST_DB = "test";
    protected static final String TEST_SCHEMA = "test_schema";
    protected static String baseUrl;
    protected static final String TEST_USERNAME = getStaticMetadata().getUsername();
    protected static final String TEST_PWD = getStaticMetadata().getPassword();

    @BeforeAll
    static void init() throws SQLException {
        // jdbc:postgresql://localhost:50807/gaussdb?user=gaussdb
        String jdbcUrl = getStaticMetadata().getJdbcUrl();
        // jdbc:postgresql://localhost:50807/
        baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));
        catalog =
                new GaussdbCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        GaussdbCatalog.DEFAULT_DATABASE,
                        TEST_USERNAME,
                        TEST_PWD,
                        baseUrl);
        // create test database and schema
        createSchema(TEST_DB, TEST_SCHEMA);
    }

    public static void executeSQL(String db, String sql) throws SQLException {
        try (Connection conn =
                     DriverManager.getConnection(
                             String.format("%s/%s", baseUrl, db), 
                             TEST_USERNAME, TEST_PWD);
             Statement statement = conn.createStatement()) {
                statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw e;
        }
    }

    public static void createSchema(String db, String schema) throws SQLException {
        executeSQL(db, String.format("CREATE SCHEMA %s", schema));
    }

    @Test
    void testDbExists() {
        assertThat(catalog.databaseExists("nonexistent")).isFalse();

        assertThat(catalog.databaseExists(GaussdbCatalog.DEFAULT_DATABASE))
        .isTrue();
    }

}
```
flink-connector-jdbc-gaussdb源码地址：[flink-connector-jdbc-gaussdb](https://github.com/Tyhoning/flink-connector-jdbc/tree/main/flink-connector-jdbc-gaussdb)

## 四、总结
通过使用Testcontainers工具，我们可以更加方便地搭建测试环境，提高测试的效率和可靠性。这种方式不仅可以在本地进行测试，还可以在持续集成和持续交付（CI/CD）流程中使用，确保我们的Flink应用在与 GaussDB 交互时的稳定性和正确性。希望本文对大家在进行Flink和GaussDB相关开发和测试时使用Testcontainers工具有所帮助。

上述代码中的一些配置信息（如 Docker 镜像名称、用户名、密码等）需要根据实际情况进行调整。

