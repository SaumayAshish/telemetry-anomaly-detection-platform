CREATE TABLE IF NOT EXISTS anomaly_alerts (
                                              id                     BIGSERIAL         PRIMARY KEY,
                                              sensor_id              VARCHAR(64)       NOT NULL,
    reading_timestamp      TIMESTAMPTZ       NOT NULL,
    value                  DOUBLE PRECISION  NOT NULL,
    baseline_mean          DOUBLE PRECISION  NOT NULL,
    baseline_std_dev       DOUBLE PRECISION  NOT NULL,
    z_score                DOUBLE PRECISION  NOT NULL,
    consecutive_anomalies  BIGINT            NOT NULL,
    severity               VARCHAR(16)       NOT NULL
    CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    ingested_at            TIMESTAMPTZ       NOT NULL DEFAULT now()
    );