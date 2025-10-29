package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import com.clickhouse.spark.ClickHouseCatalog;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClickhouseHandler implements CatalogHandler {
    private final DatasetNamespaceCombinedResolver namespaceResolver;

    public ClickhouseHandler(OpenLineageContext context) {
        namespaceResolver = new DatasetNamespaceCombinedResolver(context.getOpenLineageConfig());
    }

    @Override
    public boolean hasClasses() {
        return true;
    }

    @Override
    public boolean isClass(TableCatalog tableCatalog) {
        return tableCatalog instanceof ClickHouseCatalog;
    }

    @Override
    public DatasetIdentifier getDatasetIdentifier(SparkSession sparkSession, TableCatalog tableCatalog,
                                                  Identifier identifier, Map<String, String> map) {
        ClickHouseCatalog catalog = (ClickHouseCatalog) tableCatalog;

        List<String> parts =
                Stream.concat(Arrays.stream(identifier.namespace()), Stream.of(identifier.name()))
                        .collect(Collectors.toList());

        return namespaceResolver.resolve(new DatasetIdentifier(
                String.join(".", parts),
                catalog.name()
        ));
    }

    @Override
    public String getName() {
        return "clickhouse";
    }
}
