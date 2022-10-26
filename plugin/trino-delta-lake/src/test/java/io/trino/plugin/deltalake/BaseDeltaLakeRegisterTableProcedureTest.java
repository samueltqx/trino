/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake;

import io.trino.Session;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseDeltaLakeRegisterTableProcedureTest
        extends AbstractTestQueryFramework
{
    protected static final String CATALOG_NAME = "delta_lake";
    protected static final String SCHEMA = "test_delta_lake_register_table_" + randomNameSuffix();

    private String dataDirectory;
    protected HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString();
        this.metastore = createTestMetastore(dataDirectory);

        queryRunner.installPlugin(new TestingDeltaLakePlugin());

        queryRunner.createCatalog(CATALOG_NAME, CONNECTOR_NAME, getConnectorProperties(dataDirectory));
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    protected abstract Map<String, String> getConnectorProperties(String dataDirectory);

    protected abstract HiveMetastore createTestMetastore(String dataDirectory);

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(Path.of(dataDirectory), ALLOW_INSECURE);
        }
    }

    @Test
    public void testRegisterTableWithTableLocation()
    {
        String tableName = "test_register_table_with_table_location_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        String showCreateTableOld = (String) computeScalar("SHOW CREATE TABLE " + tableName);

        // Drop table from metastore and use the same table name to register again
        dropTableFromMetastore(tableName);

        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation));
        String showCreateTableNew = (String) computeScalar("SHOW CREATE TABLE " + tableName);

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithComments()
    {
        String tableName = "test_register_table_with_comments_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " (a, b, c) COMMENT 'my-table-comment' AS VALUES (1, 'INDIA', true)");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        // Drop table from metastore and use the same table name to register again
        dropTableFromMetastore(tableName);

        assertQuerySucceeds("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertThat(getTableComment(tableName)).isEqualTo("my-table-comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithDifferentTableName()
    {
        String tableName = "test_register_table_with_different_table_name_old_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String showCreateTableOld = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        String tableLocation = getTableLocation(tableName);
        // Drop table from metastore and use the table content to register a new table
        dropTableFromMetastore(tableName);

        String tableNameNew = "test_register_table_with_different_table_name_new_" + randomNameSuffix();
        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation));
        String showCreateTableNew = (String) computeScalar("SHOW CREATE TABLE " + tableNameNew);

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew.replaceFirst(tableNameNew, tableName));
        assertQuery("SELECT * FROM " + tableNameNew, "VALUES (1, 'INDIA', true)");

        assertUpdate(format("DROP TABLE %s", tableNameNew));
    }

    @Test
    public void testRegisterTableWithDroppedTable()
    {
        String tableName = "test_register_table_with_dropped_table_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        // Drop table to verify register_table call fails when no transaction log can be found (table doesn't exist)
        assertUpdate("DROP TABLE " + tableName);

        String tableNameNew = "test_register_table_with_dropped_table_new_" + randomNameSuffix();
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation),
                ".*Table location (.*) does not exist.*");
    }

    @Test
    public void testRegisterTableWithInvalidDeltaTable()
            throws IOException
    {
        String tableName = "test_register_table_with_no_transaction_log_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = "test_register_table_with_no_transaction_log_new_" + randomNameSuffix();

        // Delete files under transaction log directory and put an invalid log file to verify register_table call fails
        String transactionLogDir = getTransactionLogDir(new org.apache.hadoop.fs.Path(tableLocation)).toString();
        deleteDirectoryContents(Path.of(transactionLogDir), ALLOW_INSECURE);
        new File(getTransactionLogJsonEntryPath(new org.apache.hadoop.fs.Path(transactionLogDir), 0).toString()).createNewFile();

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation),
                ".*Failed to access table location: (.*)");

        deleteRecursively(Path.of(tableLocation), ALLOW_INSECURE);
        dropTableFromMetastore(tableName);
    }

    @Test
    public void testRegisterTableWithNoTransactionLog()
            throws IOException
    {
        String tableName = "test_register_table_with_no_transaction_log_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = "test_register_table_with_no_transaction_log_new_" + randomNameSuffix();

        // Delete files under transaction log directory to verify register_table call fails
        deleteDirectoryContents(Path.of(getTransactionLogDir(new org.apache.hadoop.fs.Path(tableLocation)).toString()), ALLOW_INSECURE);

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableNameNew, tableLocation),
                ".*No transaction log found in location (.*)");

        deleteRecursively(Path.of(tableLocation), ALLOW_INSECURE);
        dropTableFromMetastore(tableName);
    }

    @Test
    public void testRegisterTableWithNonExistingTableLocation()
    {
        String tableName = "test_register_table_with_non_existing_table_location_" + randomNameSuffix();
        String tableLocation = "/test/delta-lake/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation),
                ".*Table location (.*) does not exist.*");
    }

    @Test
    public void testRegisterTableWithNonExistingSchema()
    {
        String tableLocation = "/test/delta-lake/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA + "_new", "delta_table_1", tableLocation),
                "Schema (.*) not found");
    }

    @Test
    public void testRegisterTableWithExistingTable()
    {
        String tableName = "test_register_table_with_existing_table_" + randomNameSuffix();

        assertQuerySucceeds("CREATE TABLE " + tableName + " AS SELECT 1 as a, 'INDIA' as b, true as c");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        String tableLocation = getTableLocation(tableName);

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation),
                ".*Table already exists: '(.*)'.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithInvalidURIScheme()
    {
        String tableName = "test_register_table_with_invalid_uri_scheme_" + randomNameSuffix();
        String tableLocation = "invalid://hadoop-master:9000/test/delta-lake/hive/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '%s')", CATALOG_NAME, SCHEMA, tableName, tableLocation),
                ".*Failed checking table location (.*)");
    }

    @Test
    public void testRegisterTableWithInvalidParameter()
    {
        String tableName = "test_register_table_with_invalid_parameter_" + randomNameSuffix();
        String tableLocation = "/test/delta-lake/hive/table1/";

        assertQueryFails(format("CALL %s.system.register_table('%s', '%s')", CATALOG_NAME, SCHEMA, tableName),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL %s.system.register_table('%s')", CATALOG_NAME, SCHEMA),
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails(format("CALL %s.system.register_table()", CATALOG_NAME),
                ".*'SCHEMA_NAME' is missing.*");

        assertQueryFails(format("CALL %s.system.register_table(NULL, '%s', '%s')", CATALOG_NAME, tableName, tableLocation),
                ".*schema_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', NULL, '%s')", CATALOG_NAME, SCHEMA, tableLocation),
                ".*table_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', NULL)", CATALOG_NAME, SCHEMA, tableName),
                ".*table_location cannot be null or empty.*");

        assertQueryFails(format("CALL %s.system.register_table('', '%s', '%s')", CATALOG_NAME, tableName, tableLocation),
                ".*schema_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', '', '%s')", CATALOG_NAME, SCHEMA, tableLocation),
                ".*table_name cannot be null or empty.*");
        assertQueryFails(format("CALL %s.system.register_table('%s', '%s', '')", CATALOG_NAME, SCHEMA, tableName),
                ".*table_location cannot be null or empty.*");
    }

    protected String getTableLocation(String tableName)
    {
        return (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
    }

    protected void dropTableFromMetastore(String tableName)
    {
        metastore.dropTable(SCHEMA, tableName, false);
        assertThat(metastore.getTable(SCHEMA, tableName)).as("Table in metastore should be dropped").isEmpty();
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar(format(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = '%s'",
                CATALOG_NAME,
                SCHEMA,
                tableName));
    }
}
