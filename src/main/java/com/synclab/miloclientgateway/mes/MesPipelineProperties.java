package com.synclab.miloclientgateway.mes;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "mes.pipeline")
public class MesPipelineProperties {

    private final FilterProperties filter = new FilterProperties();
    private final CompressionProperties compression = new CompressionProperties();
    private final EnergyAggregationProperties energyAggregation = new EnergyAggregationProperties();

    @Getter
    @Setter
        public static class FilterProperties {
            private boolean enabled = false;
            private List<String> allowedMachines = Collections.emptyList();
            private List<String> allowedTags = Collections.emptyList();
            private List<String> allowedTagSuffixes = Collections.emptyList();
            private List<String> allowedTagKeywords = Collections.emptyList();
            private List<String> includeFields = Collections.emptyList();
            private boolean dropNullValues = true;
        }

    @Getter
    @Setter
    public static class CompressionProperties {
        private boolean enabled = false;
        private CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;
    }

    public enum CompressionAlgorithm {
        GZIP
    }

    @Getter
    @Setter
    public static class EnergyAggregationProperties {
        private boolean enabled = false;
        private long windowMs = 10_000;
    }
}
