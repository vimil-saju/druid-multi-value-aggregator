FROM apache/druid:26.0.0
RUN mkdir extensions/druid-multi-value-aggregator
COPY target/druid-multi-value-aggregator-*.jar extensions/druid-multi-value-aggregator
