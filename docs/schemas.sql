-- Table for storing airport information
CREATE TABLE Airports (
    airport_id          UNSIGNED_INT NOT NULL,
    airport_code        CHAR(3) NOT NULL,
    A.country           VARCHAR,
    A.city              VARCHAR,
    A.airport_name      VARCHAR,
    A.creation_time     TIMESTAMP,
    CONSTRAINT pk_airports PRIMARY KEY (airport_id, airport_code)
) 
VERSIONS=1;

-- Table for storing aircraft information
CREATE TABLE Aircrafts (
    aircraft_id         UNSIGNED_INT NOT NULL,
    manufacturer        VARCHAR NOT NULL,
    model               VARCHAR NOT NULL,
    A.capacity          UNSIGNED_INT,
    A.range_km          UNSIGNED_LONG,
    A.service_entry_date TIMESTAMP,
    CONSTRAINT pk_aircrafts PRIMARY KEY (aircraft_id, manufacturer, model)
) 
VERSIONS=1,IMMUTABLE_ROWS=true;

-- Table for storing crew members
CREATE TABLE CrewMembers (
    crew_id             UNSIGNED_INT NOT NULL PRIMARY KEY,
    A.first_name        VARCHAR NOT NULL,
    A.last_name         VARCHAR NOT NULL,
    A.role              VARCHAR,
    A.license_number    VARCHAR,
    A.date_of_birth     DATE,
    A.hire_date         TIMESTAMP,
    A.salary            BIGINT
) 
VERSIONS=1,IMMUTABLE_ROWS=true;

-- Table for storing passenger information
CREATE TABLE Passengers (
    passenger_id        UNSIGNED_INT NOT NULL PRIMARY KEY,
    A.first_name        VARCHAR NOT NULL,
    A.last_name         VARCHAR NOT NULL,
    A.passport_number   VARCHAR,
    A.nationality       VARCHAR,
    A.date_of_birth     DATE,
    A.gender            VARCHAR,
    A.email             VARCHAR,
    A.phone_number      VARCHAR,
    A.frequent_flyer_miles UNSIGNED_LONG DEFAULT 0
) 
VERSIONS=1,IMMUTABLE_ROWS=true,SALT_BUCKETS=10;

-- Table for storing flight information
CREATE TABLE Flights (
    flight_id           UNSIGNED_INT NOT NULL,
    flight_number       VARCHAR NOT NULL,
    A.aircraft_id       UNSIGNED_INT,
    A.departure_airport UNSIGNED_INT,
    A.arrival_airport   UNSIGNED_INT,
    A.departure_time    TIMESTAMP,
    A.arrival_time      TIMESTAMP,
    A.status            VARCHAR,
    A.available_seats   UNSIGNED_INT,
    A.price             DECIMAL(10, 2),
    A.total_flight_time BIGINT,
    CONSTRAINT pk_flights PRIMARY KEY (flight_id, flight_number)
) 
VERSIONS=1,SALT_BUCKETS=10;

-- Table for storing flight crew assignments
CREATE TABLE FlightCrew (
    flight_id           UNSIGNED_INT NOT NULL,
    crew_id             UNSIGNED_INT NOT NULL,
    A.role              VARCHAR,
    A.assignment_time   TIMESTAMP,
    CONSTRAINT pk_flightcrew PRIMARY KEY (flight_id, crew_id)
) 
VERSIONS=1;

-- Table for storing passenger bookings
CREATE TABLE Bookings (
    booking_id          UNSIGNED_INT NOT NULL,
    flight_id           UNSIGNED_INT NOT NULL,
    passenger_id        UNSIGNED_INT NOT NULL,
    A.booking_date      TIMESTAMP,
    A.seat_number       VARCHAR,
    A.status            VARCHAR,
    A.price_paid        DECIMAL(10, 2),
    A.points_earned     UNSIGNED_LONG DEFAULT 0,
    CONSTRAINT pk_bookings PRIMARY KEY (booking_id, flight_id, passenger_id)
) 
VERSIONS=1,SALT_BUCKETS=10;

-- Sequences for generating unique IDs
CREATE SEQUENCE airport_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE aircraft_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE crew_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE passenger_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE flight_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE booking_seq START WITH 1 INCREMENT BY 1;