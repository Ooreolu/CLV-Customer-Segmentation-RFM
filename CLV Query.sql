with
---- Extract only unique first visits -- Purpose: This CTE identifies the first visit of each unique user by taking the minimum date for each user (reg_date) and the earliest timestamp (first_visit).--
  first_visit as (
    select distinct user_pseudo_id as user, 
           min(parse_date('%Y%m%d', event_date)) as reg_date, 
           min(event_timestamp) as first_visit
    from `tc-da-1.turing_data_analytics.raw_events`
    group by user_pseudo_id
  ),

---- Extract only purchases --Purpose: Extracts only purchase events, including the purchase date and revenue.--
  purchases as (
    select user_pseudo_id as user, 
           parse_date('%Y%m%d', event_date) as pur_date, 
           purchase_revenue_in_usd as revenue
    from `tc-da-1.turing_data_analytics.raw_events`
    where event_name = 'purchase' 
      and purchase_revenue_in_usd > 0
  ), 

---- Combine first visits and purchases --Purpose: Combines the first visit data with purchase data for each user.--
  cohort_raw_data as (
    select f.user as f_user, 
           date_trunc(reg_date, week) as reg_week, 
           p.user as p_user, 
           date_trunc(pur_date, week) as pur_week, 
           revenue
    from first_visit f
    left join purchases p on f.user = p.user
  ),

---- Calculate registration week and purchase week difference --Purpose: Calculates the difference in weeks between the registration and purchase weeks.--
  cohort_week as (
    select f_user, 
           reg_week, 
           pur_week, 
           date_diff(pur_week, reg_week, week) as week_diff, 
           revenue
    from cohort_raw_data
  )

---- Cohort table with revenues --Purpose: Constructs a cohort table to show weekly revenue by the number of weeks since registration.--
select reg_week, 
       count(f_user) as registrations,
       sum(if(week_diff = 0, revenue, 0)) as week_0,
       sum(if(week_diff = 1, revenue, 0)) as week_1,
       sum(if(week_diff = 2, revenue, 0)) as week_2,
       sum(if(week_diff = 3, revenue, 0)) as week_3,
       sum(if(week_diff = 4, revenue, 0)) as week_4,
       sum(if(week_diff = 5, revenue, 0)) as week_5,
       sum(if(week_diff = 6, revenue, 0)) as week_6,
       sum(if(week_diff = 7, revenue, 0)) as week_7,
       sum(if(week_diff = 8, revenue, 0)) as week_8,
       sum(if(week_diff = 9, revenue, 0)) as week_9,
       sum(if(week_diff = 10, revenue, 0)) as week_10,
       sum(if(week_diff = 11, revenue, 0)) as week_11,
       sum(if(week_diff = 12, revenue, 0)) as week_12
from cohort_week 
group by reg_week
order by reg_week;
