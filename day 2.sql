create schema if not exists flight_analysis.flight_analytics;
use schema flight_analysis.flight_analytics;

drop table dim_airline;
create or replace table dim_airline (
    airline_code string,
    airline_name string,
    is_carier boolean,
    hub_airport string,
    effective_from date,
    effective_to date,
    is_current boolean
);


insert into dim_airline (
    airline_code,
    airline_name,
    is_carier,
    hub_airport,
    effective_from,
    effective_to,
    is_current
    
)
values
('AA','American Airlines',false,'DFW','2021-01-01','9999-12-31',true),
('DL','Delta Air Lines',false,'ATL','2021-01-01','9999-12-31',true),
('UA','United Airlines',false,'ORD','2021-01-01','9999-12-31',true),
('WN','Southwest Airlines',false,'DAL','2021-01-01','9999-12-31',true),
('AS','Alaska Airlines',false,'SEA','2021-01-01','9999-12-31',true),
('B6','JetBlue Airways',false,'JFK','2021-01-01','9999-12-31',true),
('NK','Spirit Airlines',false,'FLL','2021-01-01','9999-12-31',true),
('F9','Frontier Airlines',false,'DEN','2021-01-01','9999-12-31',true),
('G4','Allegiant Air',false,'LAS','2021-01-01','9999-12-31',true),
('HA','Hawaiian Airlines',false,'HNL','2021-01-01','9999-12-31',true),
('MQ','Envoy Air',false,'DFW','2021-01-01','9999-12-31',true),
('OO','SkyWest Airlines',false,'SLC','2021-01-01','9999-12-31',true),
('YV','Mesa Airlines',false,'PHX','2021-01-01','9999-12-31',true),
('YX','Republic Airways',false,'IND','2021-01-01','9999-12-31',true),
('OH','PSA Airlines',false,'CLT','2021-01-01','9999-12-31',true),
('9E','Endeavor Air',false,'MSP','2021-01-01','9999-12-31',true),
('QX','Horizon Air',false,'SEA','2021-01-01','9999-12-31',true);



-- merge into dim_airline tgt
-- using flight_analysis.staging.monthly_airline_kpi src
-- on tgt.airline_code = src.airline_code
-- and tgt.is_current = true

-- when matched and (
--     tgt.airline_name != src.airline_name
--     or tgt.hub_airport != src.hub_airport
--     or tgt.is_cargo != src.is_cargo
-- )
-- then update set
--     tgt.effective_to = current_date - 1,
--     tgt.is_current = false

-- when not matched then
-- insert (
--     airline_code,
--     airline_name,
--     is_cargo,
--     hub_airport,
--     effective_from,
--     effective_to,
--     is_current
-- )
-- values (
--     src.airline_code,
--     src.airline_name,
--     src.is_cargo,
--     src.hub_airport,
--     current_date,
--     '9999-12-31',
--     true
-- );


merge into dim_airline tgt
using (
    select distinct airline_code, Reporting_Airline
    from flight_analysis.staging.monthly_airline_kpi
) src

on tgt.airline_code = src.airline_code
and tgt.is_current = true

when matched and (
    tgt.airline_name != src.Reporting_Airline
)
then update set
    tgt.effective_to = current_date - 1,
    tgt.is_current = false

when not matched then
insert (
    airline_code,
    airline_name,
    is_carier,
    hub_airport,
    effective_from,
    effective_to,
    is_current
)
values (
    src.airline_code,
    src.Reporting_Airline,
    false,
    null,
    current_date,
    '9999-12-31',
    true
);

select * from dim_airline;


create or replace table date_dim as

select
    full_date,

    -- Year
    extract(year from full_date) as year,

    -- Month Number
    extract(month from full_date) as month_number,

    -- Month Name
    to_char(full_date, 'MMMM') as month_name,

    -- Day of Week (1–7)
    extract(dayofweekiso from full_date) as day_of_week,

    -- Day Name
    to_char(full_date, 'DAY') as day_name,

    -- Quarter
    extract(quarter from full_date) as quarter,

    -- Weekend (Saturday = 6, Sunday = 7)
    case 
        when extract(dayofweekiso from full_date) in (6,7) then true
        else false
    end as weekend,

    -- Season
    case 
        when extract(month from full_date) in (12,1,2) then 'Winter'
        when extract(month from full_date) in (3,4) then 'Spring'
        when extract(month from full_date) in (5,6,7) then 'Summer'
        when extract(month from full_date) in (8,9) then 'Rainy'
        else 'Other'
    end as season,

    -- Year_Month (YYYY-MM)
    to_char(full_date, 'YYYY-MM') as year_month

from (
    select 
        dateadd(day, seq4(), '2021-01-01') as full_date
    from table(generator(rowcount => 750))   -- 750 dates generate
-- generator = rows banata hai
-- seq4 = numbering deta hai
-- dateadd = us number ko date me convert karta hai
)
  where  (extract(year from full_date) = 2021 and extract(month from full_date) in (1,2,3))
 or (extract(year from full_date) = 2022 and extract(month from full_date) in (1,3,4));   --  filter applied

select * from date_dim;

-- for check 
-- select full_date,
-- dayofweek(full_date) as weekday_number,
-- to_char(full_date, 'DY') as weekday_name
-- from date_dim;


