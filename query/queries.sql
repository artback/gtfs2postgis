-- name: create-table-agency
CREATE TABLE IF NOT EXISTS agency
(
    agency_id       VARCHAR(255) NOT NULL PRIMARY KEY,
    agency_name     VARCHAR(255),
    agency_url      VARCHAR(255),
    agency_timezone VARCHAR(255),
    agency_lang     VARCHAR(255)
);

-- name: create-table-calendar_dates
CREATE TABLE IF NOT EXISTS calendar_dates
(
    service_id     VARCHAR(255),
    date           date,
    exception_type smallint,
    PRIMARY KEY (service_id, date)
);


-- name: create-table-stops
CREATE TABLE IF NOT EXISTS stops
(
    stop_id       VARCHAR(255)  NOT NULL PRIMARY KEY,
    stop_name     VARCHAR(255)  NOT NULL,
    stop_lat      DECIMAL(8, 6) NOT NULL,
    stop_lon      DECIMAL(9, 6) NOT NULL,
    location_type SMALLINT CHECK (location_type BETWEEN 0 AND 1)
);


-- name: create-table-routes
CREATE TABLE IF NOT EXISTS routes
(
    route_id         VARCHAR(255) NOT NULL PRIMARY KEY,
    agency_id        VARCHAR(255) references agency (agency_id),
    route_type       VARCHAR(255),
    route_short_name VARCHAR(255),
    route_long_name  VARCHAR(255),
    route_url        VARCHAR(255)
);

-- name: update-geom-stops
SELECT AddGeometryColumn('stops', 'geom', 4326, 'POINT', 2);
UPDATE stops
SET geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326);
CREATE INDEX stops_geom ON stops USING GIST (geom);


-- name: create-table-stop_times
CREATE TABLE IF NOT EXISTS stop_times
(
    trip_id             VARCHAR(255) REFERENCES trips (trip_id),
    arrival_time        VARCHAR(8) NOT NULL,
    departure_time      VARCHAR(8) NOT NULL,
    stop_id             VARCHAR(255) REFERENCES stops (stop_id),
    stop_sequence       INTEGER    NOT NULL,
    stop_headsign       VARCHAR(255),
    pickup_type         SMALLINT CHECK (pickup_type BETWEEN 0 AND 3),
    drop_off_type       SMALLINT CHECK (drop_off_type BETWEEN 0 AND 3),
    shape_dist_traveled VARCHAR(255),
    timepoint           SMALLINT CHECK (timepoint BETWEEN 0 AND 1)
);
COMMENT ON COLUMN stop_times.pickup_type IS '0 regularly scheduled pickup, 1 no pickup available, 2 must phone agency to arrange pickup, 3 must coordinate with driver to arrange pickup';
COMMENT ON COLUMN stop_times.drop_off_type IS '0 regularly scheduled drop off, 1 no drop off available, 2 must phone agency to arrange drop off, 3 must coordinate with driver to arrange drop off';
COMMENT ON COLUMN stop_times.timepoint IS '0 times are considered approximate, 1 times are considered exact';


-- name: create-table-trips
CREATE TABLE IF NOT EXISTS trips
(
    route_id              VARCHAR(255) references routes (route_id),
    service_id            VARCHAR(255) NOT NULL,
    trip_id               VARCHAR(255) NOT NULL PRIMARY KEY,
    trip_headsign         VARCHAR(255),
    trip_short_name       VARCHAR(255),
    direction_id          SMALLINT,
    block_id              VARCHAR(255),
    shape_id              VARCHAR(255),
    wheelchair_accessible SMALLINT CHECK (wheelchair_accessible BETWEEN 0 AND 2),
    bikes_allowed         SMALLINT CHECK (bikes_allowed BETWEEN 0 AND 2)
);
COMMENT ON COLUMN trips.wheelchair_accessible IS '0 no info, 1 accommodate at least one rider, 2 no riders can be accommodated';
COMMENT ON COLUMN trips.bikes_allowed IS '0 no info, 1 accommodate at least one bicycle, 2 no bicycles can be accommodated';

-- name: drop-table
DROP TABLE IF EXISTS %s;