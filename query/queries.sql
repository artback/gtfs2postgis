-- name: create-table-agency
CREATE TABLE agency
(
    agency_id       VARCHAR(255) NOT NULL PRIMARY KEY,
    agency_name     VARCHAR(255),
    agency_url      VARCHAR(255),
    agency_timezone VARCHAR(255),
    agency_lang     VARCHAR(255)
);

-- name: create-table-calendar_dates
CREATE TABLE calendar_dates
(
    service_id     VARCHAR(255),
    date           date,
    exception_type smallint,
    PRIMARY KEY (service_id, date)
);


-- name: create-table-stops
CREATE TABLE stops
(
    stop_id       VARCHAR(255)  NOT NULL PRIMARY KEY,
    stop_name     VARCHAR(255)  NOT NULL,
    stop_lat      DECIMAL(8, 6) NOT NULL,
    stop_lon      DECIMAL(9, 6) NOT NULL,
    location_type SMALLINT CHECK (location_type BETWEEN 0 AND 1)
);


-- name: create-table-routes
CREATE TABLE routes
(
    route_id         VARCHAR(255) NOT NULL PRIMARY KEY,
    agency_id        VARCHAR(255) references agency (agency_id),
    route_type       VARCHAR(255),
    route_short_name VARCHAR(255),
    route_long_name  VARCHAR(255),
    route_url        VARCHAR(255)
);

-- name: update-geom-stops
ALTER TABLE stops
    ADD COLUMN geom geometry(Point, 4326);
UPDATE stops
SET geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
WHERE geom IS Null;
CREATE INDEX IF NOT EXISTS stops_geom ON stops USING GIST (geom);


-- name: create-table-stop_times
CREATE TABLE stop_times
(
    trip_id        VARCHAR(255),
    arrival_time   VARCHAR(8) NOT NULL,
    departure_time VARCHAR(8) NOT NULL,
    stop_id        VARCHAR(255) REFERENCES stops (stop_id),
    stop_sequence  INTEGER    NOT NULL,
    pickup_type    SMALLINT,
    drop_off_type  SMALLINT
);


-- name: create-table-trips
CREATE TABLE trips
(
    route_id        VARCHAR(255) references routes (route_id),
    service_id      VARCHAR(255) NOT NULL,
    trip_id         VARCHAR(255) NOT NULL PRIMARY KEY,
    trip_headsign   VARCHAR(255),
    trip_short_name VARCHAR(255)
);

-- name: drop-table
DROP TABLE IF EXISTS %s;

