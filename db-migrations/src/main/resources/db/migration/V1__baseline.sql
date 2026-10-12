-- Baseline captured from the working local database (PostgreSQL 16, pg_dump --schema-only).
-- Replaces the stale alerting-service/src/main/resources/db/schema.sql.

CREATE TABLE sensors (
                         sensor_id    VARCHAR(64)  NOT NULL,
                         location     VARCHAR(255) NOT NULL,
                         install_date DATE         NOT NULL,
                         model        VARCHAR(100) NOT NULL,
                         CONSTRAINT sensors_pkey PRIMARY KEY (sensor_id)
);

CREATE TABLE anomaly_alerts (
                                id                    BIGSERIAL        NOT NULL,
                                sensor_id             VARCHAR(64)      NOT NULL,
                                reading_timestamp     TIMESTAMPTZ      NOT NULL,
                                value                 DOUBLE PRECISION NOT NULL,
                                baseline_mean         DOUBLE PRECISION NOT NULL,
                                baseline_std_dev      DOUBLE PRECISION NOT NULL,
                                z_score               DOUBLE PRECISION NOT NULL,
                                consecutive_anomalies BIGINT           NOT NULL,
                                severity              VARCHAR(16)      NOT NULL,
                                ingested_at           TIMESTAMPTZ      NOT NULL DEFAULT now(),
                                kafka_partition       INTEGER,
                                kafka_offset          BIGINT,
                                CONSTRAINT anomaly_alerts_pkey PRIMARY KEY (id),
                                CONSTRAINT anomaly_alerts_severity_check
                                    CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
                                CONSTRAINT uq_anomaly_alerts_kafka_position UNIQUE (kafka_partition, kafka_offset),
                                CONSTRAINT fk_anomaly_alerts_sensor FOREIGN KEY (sensor_id) REFERENCES sensors (sensor_id)
);

CREATE INDEX idx_anomaly_alerts_sensor_id ON anomaly_alerts (sensor_id);

CREATE TABLE alert_dlq (
                           id            BIGSERIAL    NOT NULL,
                           kafka_topic   VARCHAR(255) NOT NULL,
                           kafka_partition INTEGER    NOT NULL,
                           kafka_offset  BIGINT       NOT NULL,
                           sensor_id     VARCHAR(64),
                           payload       TEXT         NOT NULL,
                           sql_state     VARCHAR(30),
                           error_message TEXT,
                           failed_at     TIMESTAMPTZ  DEFAULT now(),
                           CONSTRAINT alert_dlq_pkey PRIMARY KEY (id),
                           CONSTRAINT uq_alert_dlq_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);