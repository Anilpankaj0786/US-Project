use database flight_analysis;
create or replace view vw_airport_kpi as
select
    origin_code as airport_code,
    origin_airport_name as airport_name,
    origin_airport_city as city,
    origin_lat as latitude,
    origin_lon as longitude,

    flight_year,
    -- total flights departed
    sum(total_departure) as total_flights_departed,
    -- avg on-time 
    avg(departure_on_time_percentage) as avg_on_time_percentage,
    -- avg delay
    avg(avg_departure_delay_minutes) as avg_delay_minutes,
    -- max operating flights
    max(number_of_flights_operating) as max_operating_flights
from flight_analysis.staging.airport_departure_kpi
group by
    origin_code,
    origin_airport_name,
    origin_airport_city,
    origin_lat,
    origin_lon,
    flight_year;



create or replace view vw_airline_performance as
select
    m.airline_code,
    d.airline_name,
    d.is_carier,
    m.reporting_airline,
    m.flight_year,
    m.flight_month,
    -- total flights
    sum(m.total_number_of_flights) as total_flights,
    -- cancelled flights
    sum(m.cancelled_flights) as total_cancelled_flights,
    -- avg cancellation 
    avg(m.cancelled_flights) as avg_cancelled_filght,
    -- delay average 
    avg(m.avg_arrival_delay_minutes) as avg_arrival_delay,
    avg(m.avg_carrier_delay) as avg_carrier_delay,
    avg(m.avg_weather_delay) as avg_weather_delay,
    avg(m.avg_late_aircraft_delay) as avg_late_aircraft_delay
from flight_analysis.staging.monthly_airline_kpi m

left join flight_analysis.flight_analytics.dim_airline d
on m.airline_code = d.airline_code
    

group by
    m.airline_code,
    d.airline_name,
    d.is_carier,
    m.reporting_airline,
    m.flight_year,
    m.flight_month;