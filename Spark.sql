create database if not exists flight_analysis;
create schema if not exists flight_analysis.staging;
use database flight_analysis;
use schema staging;


-- snowflake ko s3 se connect karne ke liye storage integration
create or replace storage integration s3_int_flight
type = external_stage
storage_provider = s3
enabled = true
-- ye IAM role snowflake ko s3 access dene ke liye use hota hai
-- sirf is path tak hi access allow hai (security ke liye)
storage_aws_role_arn = 'arn:aws:iam::184588785196:role/snowflake-s3-role'
storage_allowed_locations = ('s3://airlines-bucket-07860/Gold_data');
desc integration s3_int_flight;



-- external stage banate hain (ye s3 ka entry point hai snowflake ke liye)
create or replace stage gold_stage_s3
url = 's3://airlines-bucket-07860/Gold_data/'
storage_integration = s3_int_flight;

-- stage check karne ke liye
desc stage gold_stage_s3;

-- s3 me kaun si files available hain wo dekhne ke liye
list @gold_stage_s3;



-- ab tables create kar rahe hain (yaha data load hoga)

create or replace table monthly_airline_kpi (
    flight_year int,
    flight_month int,
    airline_code string,
    reporting_airline string,
    total_number_of_flights number,
    delayed_flights number,
    total_flights_cancelled number,
    diverted_flights number,
    non_cancelled_flights number,
    avg_arrival_delay_minutes float,
    median_arrival_delay_minutes float,
    on_time_flights number,
    on_time_flight_percentage float,
    cancelled_flights number,
    cancelled_flight_percentage float,
    avg_carrier_delay float,
    avg_weather_delay float,
    avg_security_delay float,
    avg_late_aircraft_delay float,
    avg_distance_travelled float,
    total_distance_travelled float,
    monthly_rank int
)
data_retention_time_in_days = 10;



create or replace table annual_route_performance (
    flight_year int,
    route string,
    origin_code string,
    destination_code string,
    number_of_flights number,
    avg_arrival_delay float,
    avg_distance_travelled float,
    total_delayed_flights number,
    total_on_time_flights number,
    number_of_airlines_on_route number,
    on_time_percentage_airline_percentage float
)
data_retention_time_in_days = 10;



create or replace table airport_departure_kpi (
    flight_year int,
    flight_month int,
    origin_code string,
    origin_airport_name string,
    origin_airport_city string,
    origin_airport_state string,
    origin_lon float,
    origin_lat float,
    total_departure number,
    total_cancelled_departure number,
    avg_departure_delay_minutes float,
    avg_route_distance float,
    number_of_flights_operating number,
    avg_airtime float,
    departure_on_time_percentage float
)
data_retention_time_in_days = 10;



create or replace table delay_cause_table (
    flight_year int,
    flight_month int,
    airline_code string,
    total_minutes_delayed float,
    total_weather_delayed_minutes float,
    total_carrier_delayed_minutes float,
    total_security_delayed_minutes float,
    total_late_aircraft_delayed_minutes float,
    weather_delay_percentage float,
    carrier_delay_percentage float,
    security_delay_percentage float,
    late_aircraft_delay_percentage float
)
data_retention_time_in_days = 10;





-- snowpipe create kar rahe hain (ye automatically s3 se data load karega)

-- monthly_airline_kpi ke liye pipe
create or replace pipe pipe_monthly_airline_kpi
auto_ingest = true
as
copy into monthly_airline_kpi
from @gold_stage_s3/monthly_airline_kpi/
file_format = (type = parquet)
match_by_column_name = case_insensitive;



-- annual_route_performance ke liye pipe
create or replace pipe pipe_annual_route_performance
auto_ingest = true
as
copy into annual_route_performance
from @gold_stage_s3/annual_route_performance/
file_format = (type = parquet)
match_by_column_name = case_insensitive;



-- airport_departure_kpi ke liye pipe
create or replace pipe pipe_airport_departure_kpi
auto_ingest = true
as
copy into airport_departure_kpi
from @gold_stage_s3/airport_departure_kpi/
file_format = (type = parquet)
match_by_column_name = case_insensitive;



-- delay_cause_table ke liye pipe
create or replace pipe pipe_delay_cause_table
auto_ingest = true
as
copy into delay_cause_table
from @gold_stage_s3/delay_cause_table/
file_format = (type = parquet)
match_by_column_name = case_insensitive;



-- pipe sahi bana ya nahi check karne ke liye
desc pipe pipe_monthly_airline_kpi;


-- data load hua ya nahi check karne ke liye
select * from monthly_airline_kpi;
select * from annual_route_performance;
select * from airport_departure_kpi;
select * from delay_cause_table;

show tables like 'monthly_airline_kpi';

