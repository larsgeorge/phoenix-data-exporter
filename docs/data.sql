UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (1, 'JFK', 'USA', 'New York', 'John F. Kennedy International Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (2, 'LAX', 'USA', 'Los Angeles', 'Los Angeles International Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (3, 'ORD', 'USA', 'Chicago', 'O\'Hare International Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (4, 'LHR', 'UK', 'London', 'Heathrow Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (5, 'CDG', 'FRA', 'Paris', 'Charles de Gaulle Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (6, 'HND', 'JPN', 'Tokyo', 'Haneda Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (7, 'SYD', 'AUS', 'Sydney', 'Sydney Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (8, 'DXB', 'UAE', 'Dubai', 'Dubai International Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (9, 'SIN', 'SGP', 'Singapore', 'Changi Airport', CURRENT_DATE());

UPSERT INTO Airports (airport_id, airport_code, A.country, A.city, A.airport_name, A.creation_time) 
VALUES (10, 'YYZ', 'CAN', 'Toronto', 'Toronto Pearson International Airport', CURRENT_DATE());


UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (1, 'Boeing', '747-8', 467, 14815, TO_TIMESTAMP('2020-01-01 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (2, 'Airbus', 'A380', 555, 15200, TO_TIMESTAMP('2019-06-15 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (3, 'Boeing', '777-300ER', 396, 13650, TO_TIMESTAMP('2018-03-23 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (4, 'Airbus', 'A350-1000', 440, 16100, TO_TIMESTAMP('2017-11-12 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (5, 'Boeing', '787-9', 290, 14140, TO_TIMESTAMP('2021-05-20 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (6, 'Airbus', 'A330-900neo', 287, 13330, TO_TIMESTAMP('2022-02-28 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (7, 'Boeing', '737 MAX 8', 189, 6570, TO_TIMESTAMP('2019-09-30 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (8, 'Airbus', 'A320neo', 194, 6850, TO_TIMESTAMP('2020-08-18 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (9, 'Embraer', 'E195-E2', 146, 4800, TO_TIMESTAMP('2019-07-10 00:00:00'));

UPSERT INTO Aircrafts (aircraft_id, manufacturer, model, A.capacity, A.range_km, A.service_entry_date) 
VALUES (10, 'Bombardier', 'CRJ900', 90, 2940, TO_TIMESTAMP('2018-04-05 00:00:00'));


UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (1, 'John', 'Smith', 'Pilot', 'P12345', TO_DATE('1980-05-12'), CURRENT_DATE(), 150000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (2, 'Jane', 'Doe', 'Pilot', 'P67890', TO_DATE('1978-09-23'), CURRENT_DATE(), 160000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (3, 'Alice', 'Johnson', 'Cabin Crew', NULL, TO_DATE('1985-02-15'), CURRENT_DATE(), 55000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (4, 'Bob', 'Brown', 'Cabin Crew', NULL, TO_DATE('1990-08-19'), CURRENT_DATE(), 52000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (5, 'Charlie', 'Davis', 'Engineer', 'E54321', TO_DATE('1975-03-30'), CURRENT_DATE(), 130000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (6, 'Diana', 'Clark', 'Engineer', 'E98765', TO_DATE('1982-12-02'), CURRENT_DATE(), 125000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (7, 'Eve', 'Williams', 'Cabin Crew', NULL, TO_DATE('1988-07-22'), CURRENT_DATE(), 54000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (8, 'Frank', 'Taylor', 'Pilot', 'P11223', TO_DATE('1979-11-11'), CURRENT_DATE(), 155000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (9, 'Grace', 'Lee', 'Cabin Crew', NULL, TO_DATE('1992-01-14'), CURRENT_DATE(), 51000);

UPSERT INTO CrewMembers (crew_id, A.first_name, A.last_name, A.role, A.license_number, A.date_of_birth, A.hire_date, A.salary) 
VALUES (10, 'Hank', 'White', 'Cabin Crew', NULL, TO_DATE('1986-04-08'), CURRENT_DATE(), 53000);


UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (1, 'Michael', 'Johnson', 'A1234567', 'USA', TO_DATE('1985-11-25'), 'M', 'mjohnson@example.com', '+11234567890', 15000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (2, 'Sarah', 'Brown', 'B7654321', 'UK', TO_DATE('1990-04-15'), 'F', 'sbrown@example.com', '+441234567890', 20000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (3, 'David', 'Wilson', 'C9876543', 'CAN', TO_DATE('1982-07-08'), 'M', 'dwilson@example.com', '+14161234567', 25000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (4, 'Laura', 'Smith', 'D6543210', 'AUS', TO_DATE('1989-09-23'), 'F', 'lsmith@example.com', '+61234567890', 18000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (5, 'James', 'Williams', 'E5432109', 'USA', TO_DATE('1975-12-30'), 'M', 'jwilliams@example.com', '+112398765432', 22000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (6, 'Linda', 'Taylor', 'F0987654', 'UK', TO_DATE('1980-01-11'), 'F', 'ltaylor@example.com', '+441234567899', 17000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (7, 'Robert', 'Davis', 'G5678901', 'USA', TO_DATE('1987-06-18'), 'M', 'rdavis@example.com', '+112356789012', 23000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (8, 'Emily', 'Miller', 'H2345678', 'CAN', TO_DATE('1992-03-29'), 'F', 'emiller@example.com', '+14161239876', 16000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (9, 'William', 'Jones', 'I8765432', 'AUS', TO_DATE('1984-10-03'), 'M', 'wjones@example.com', '+61412345678', 19000);

UPSERT INTO Passengers (passenger_id, A.first_name, A.last_name, A.passport_number, A.nationality, A.date_of_birth, A.gender, A.email, A.phone_number, A.frequent_flyer_miles) 
VALUES (10, 'Jessica', 'Garcia', 'J0123456', 'USA', TO_DATE('1986-02-17'), 'F', 'jgarcia@example.com', '+112312345678', 21000);


UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (1, 'AA101', 1, 1, 2, TO_TIMESTAMP('2023-08-19 07:30:00'), TO_TIMESTAMP('2023-08-19 11:00:00'), 'On Time', 120, 199.99, 210);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (2, 'BA202', 2, 4, 5, TO_TIMESTAMP('2023-08-19 09:45:00'), TO_TIMESTAMP('2023-08-19 12:20:00'), 'Delayed', 200, 299.99, 155);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (3, 'UA303', 3, 3, 6, TO_TIMESTAMP('2023-08-19 12:15:00'), TO_TIMESTAMP('2023-08-19 18:45:00'), 'On Time', 300, 399.99, 390);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (4, 'AF404', 4, 5, 7, TO_TIMESTAMP('2023-08-19 14:00:00'), TO_TIMESTAMP('2023-08-19 20:30:00'), 'On Time', 150, 459.99, 385);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (5, 'QF505', 5, 7, 8, TO_TIMESTAMP('2023-08-19 15:30:00'), TO_TIMESTAMP('2023-08-19 21:45:00'), 'Canceled', 180, 559.99, 375);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (6, 'EK606', 6, 8, 9, TO_TIMESTAMP('2023-08-19 18:00:00'), TO_TIMESTAMP('2023-08-20 00:15:00'), 'On Time', 250, 659.99, 375);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (7, 'SQ707', 7, 9, 10, TO_TIMESTAMP('2023-08-20 07:00:00'), TO_TIMESTAMP('2023-08-20 09:45:00'), 'On Time', 220, 499.99, 225);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (8, 'CA808', 8, 2, 3, TO_TIMESTAMP('2023-08-20 10:00:00'), TO_TIMESTAMP('2023-08-20 13:15:00'), 'On Time', 190, 329.99, 195);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (9, 'JL909', 9, 6, 4, TO_TIMESTAMP('2023-08-20 11:30:00'), TO_TIMESTAMP('2023-08-20 19:00:00'), 'Delayed', 210, 549.99, 450);

UPSERT INTO Flights (flight_id, flight_number, A.aircraft_id, A.departure_airport, A.arrival_airport, A.departure_time, A.arrival_time, A.status, A.available_seats, A.price, A.total_flight_time) 
VALUES (10, 'AC010', 10, 10, 1, TO_TIMESTAMP('2023-08-20 14:45:00'), TO_TIMESTAMP('2023-08-20 18:00:00'), 'On Time', 240, 379.99, 195);


UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (1, 1, 'Pilot', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (1, 3, 'Cabin Crew', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (2, 2, 'Pilot', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (2, 4, 'Cabin Crew', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (3, 1, 'Pilot', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (3, 5, 'Engineer', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (4, 2, 'Pilot', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (4, 3, 'Cabin Crew', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (5, 6, 'Engineer', CURRENT_DATE());

UPSERT INTO FlightCrew (flight_id, crew_id, A.role, A.assignment_time) 
VALUES (6, 4, 'Cabin Crew', CURRENT_DATE());


UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (1, 1, 1, CURRENT_DATE(), '12A', 'Confirmed', 199.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (2, 2, 2, CURRENT_DATE(), '14C', 'Confirmed', 299.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (3, 3, 3, CURRENT_DATE(), '18D', 'Confirmed', 399.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (4, 4, 4, CURRENT_DATE(), '22F', 'Confirmed', 459.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (5, 5, 5, CURRENT_DATE(), '1A', 'Canceled', 559.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (6, 6, 6, CURRENT_DATE(), '3B', 'Confirmed', 659.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (7, 7, 7, CURRENT_DATE(), '6C', 'Confirmed', 499.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (8, 8, 8, CURRENT_DATE(), '9D', 'Confirmed', 329.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (9, 9, 9, CURRENT_DATE(), '4E', 'Confirmed', 549.99);

UPSERT INTO Bookings (booking_id, flight_id, passenger_id, A.booking_date, A.seat_number, A.status, A.price_paid) 
VALUES (10, 10, 10, CURRENT_DATE(), '8F', 'Confirmed', 379.99);