{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dim']
    )
}}

/*
    Sinh toàn bộ ngày từ 2022-01-01 đến 3 năm sau ngày chạy.
    Dùng cross join thay recursive CTE để tránh giới hạn MAXRECURSION.
    Chạy full-refresh mỗi lần pipeline chạy (nhanh, <1s).
*/

with e0 as (
    select 1 as x union all select 1 union all select 1 union all select 1
    union all select 1 union all select 1 union all select 1 union all select 1
    union all select 1 union all select 1
),
e1 as (select a.x from e0 a cross join e0 b),
e2 as (select a.x from e1 a cross join e1 b),
numbers as (
    select top (datediff(day, '2022-01-01', dateadd(year, 3, getdate())) + 1)
        row_number() over (order by (select null)) - 1 as n
    from e2
),
date_spine as (
    select dateadd(day, n, cast('2022-01-01' as date)) as date_day
    from numbers
)

select
    cast(format(date_day, 'yyyyMMdd') as int)           as date_key,
    cast(date_day as date)                              as full_date,
    year(date_day)                                      as year,
    datepart(quarter, date_day)                         as quarter,
    month(date_day)                                     as month,
    datename(month, date_day)                           as month_name,
    day(date_day)                                       as day_of_month,
    datepart(weekday, date_day)                         as day_of_week,
    datename(weekday, date_day)                         as day_name,
    cast(
        case when datepart(weekday, date_day) in (1, 7) then 1 else 0 end
    as bit)                                             as is_weekend
from date_spine
