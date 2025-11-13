package datahub.spark;

import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.metadata.key.ContainerKey;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.spark.SparkConf;

import java.net.URISyntaxException;

public class AirflowMetadata {
    public static final String AIRFLOW_URL_KEY = "spark.airflow.url";
    public static final String AIRFLOW_DAG_ID_KEY = "spark.airflow.dag_id";
    public static final String AIRFLOW_TASK_ID_KEY = "spark.airflow.task_id";

    private final SparkConf conf;

    private static final String ENV = "prod";

    public AirflowMetadata(SparkConf sparkConf) {
        this.conf = sparkConf;
    }

    public boolean isUrlDefined() {
        return conf.contains(AIRFLOW_URL_KEY);
    }

    public boolean isDagIdDefined() {
        return conf.contains(AIRFLOW_DAG_ID_KEY);
    }

    public boolean isTaskIdDefined() {
        return conf.contains(AIRFLOW_TASK_ID_KEY);
    }

    public String getUrl() {
        String url = conf.get(AIRFLOW_URL_KEY);

        if (url.endsWith("/"))
            return url.substring(0, url.length() - 1);
        else
            return url;
    }

    public String getDagId() {
        return conf.get(AIRFLOW_DAG_ID_KEY);
    }

    public String getTaskId() {
        return conf.get(AIRFLOW_TASK_ID_KEY);
    }

    public String createDagContainerGuid() {
        return String.format("airflow__dag__%s__%s", getDagId(), ENV);
    }

    public String createDagContainerUrn() {
        return String.format("urn:li:container:%s", createDagContainerGuid());
    }

    public String createDagContainerName() {
        return "Airflow dag:" + getDagId();
    }

    public String createTaskContainerGuid() {
        return String.format("airflow__task__%s__%s", getTaskId(), ENV);
    }

    public String createTaskContainerUrn() {
        return String.format("urn:li:container:%s", createTaskContainerGuid());
    }

    public String createTaskContainerName() {
        return "Airflow task:" + getTaskId();
    }

    public String createDagUrl() {
        return String.format("%s/dags/%s/grid", getUrl(), getDagId());
    }

    public String createTaskUrl() {
        return String.format("%s?task_id=%s", createDagUrl(), getTaskId());
    }


    public MetadataChangeProposalWrapper generateDagContainerKeyMCPW() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("container")
                .entityUrn(createDagContainerUrn())
                .upsert()
                .aspect(new ContainerKey().setGuid(createDagContainerGuid()))
                .build();
    }

    public MetadataChangeProposalWrapper generateDagContainerPropertiesMCPW() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("container")
                .entityUrn(createDagContainerUrn())
                .upsert()
                .aspect(
                        new ContainerProperties()
                                .setExternalUrl(new Url(createDagUrl()))
                                .setName(createDagContainerName())
                )
                .build();
    }

    public MetadataChangeProposalWrapper generateTaskContainerKeyMCPW() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("container")
                .entityUrn(createTaskContainerUrn())
                .upsert()
                .aspect(new ContainerKey().setGuid(createTaskContainerGuid()))
                .build();
    }

    public MetadataChangeProposalWrapper generateTaskContainerPropertiesMCPW() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("container")
                .entityUrn(createTaskContainerUrn())
                .upsert()
                .aspect(
                        new ContainerProperties()
                                .setExternalUrl(new Url(createTaskUrl()))
                                .setName(createTaskContainerName())
                )
                .build();
    }

    public MetadataChangeProposalWrapper generateDagTaskRelationMCPW() throws URISyntaxException {
        return MetadataChangeProposalWrapper.builder()
                .entityType("container")
                .entityUrn(createTaskContainerUrn())
                .upsert()
                .aspect(new Container().setContainer(new Urn(createDagContainerUrn())))
                .build();
    }
}
