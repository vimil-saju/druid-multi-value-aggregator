FROM apache/druid:24.0.0
COPY target/druid-multi-value-aggregator-1.0.jar extensions/druid-multi-value-aggregator/druid-multi-value-aggregator-1.0.jar
