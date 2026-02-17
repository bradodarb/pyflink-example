CREATE TABLE IF NOT EXISTS sensor_readings (
    id         VARCHAR NOT NULL,
    kind       VARCHAR,
    value      DOUBLE PRECISION,
    timestamp  VARCHAR NOT NULL,
    PRIMARY KEY (id, timestamp)
);
