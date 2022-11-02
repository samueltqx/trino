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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.testing.BaseFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BaseBigQueryFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    public BaseBigQueryFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(
                configProperties,
                coordinatorProperties,
                ImmutableMap.of(),
                requiredTpchTables,
                runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.<String, String>builder()
                            .put("exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager")
                            .buildOrThrow());
                });
    }

    @Override
    protected void createPartitionedLineitemTable(String tableName, List<String> columns, String partitionColumn)
    {
    }

    @Override
    public void testJoinDynamicFilteringDisabled()
    {
        assertThatThrownBy(super::testJoinDynamicFilteringDisabled)
                .hasMessageContaining("Unknown session property bigquery.dynamic_filtering_wait_timeout");
        throw new SkipException("skipped");
    }

    @Override
    public void testJoinDynamicFilteringEnabled()
    {
        assertThatThrownBy(super::testJoinDynamicFilteringEnabled)
                .hasMessageContaining("Unknown session property bigquery.dynamic_filtering_wait_timeout");
        throw new SkipException("skipped");
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }
}
