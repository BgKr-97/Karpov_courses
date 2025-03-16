# 5 урок (агрегация данных)

**Задача № 17**

```sql
SELECT count(1) as orders,
       count(1) filter (WHERE array_length(product_ids, 1) >= 5) as large_orders,
       round(1.0 * count(1) filter (WHERE array_length(product_ids, 1) >= 5) / count(1), 2) as large_orders_share
FROM   orders
```


# 6 урок (группировка данных)

**Задача № 18**

```sql
SELECT user_id,
       count(user_id) filter(WHERE action = 'create_order') as orders_count,
       round(1.0 * count(user_id) filter(WHERE action = 'cancel_order') / count(distinct order_id), 2) as cancel_rate
FROM   user_actions
GROUP BY user_id having count(user_id) filter(
WHERE  action = 'create_order') > 3
   and round(1.0 * count(user_id) filter(WHERE  action = 'cancel_order') / count(distinct order_id), 2) >= 0.5
ORDER BY 1
```

**Задача № 19**

```sql
SELECT date_part('isodow', time)::int as weekday_number,
       to_char(time, 'Dy') as weekday,
       count(distinct order_id) filter (WHERE action = 'create_order') as created_orders,
       count(distinct order_id) filter (WHERE action = 'cancel_order') as canceled_orders,
       count(distinct order_id) filter (WHERE action = 'create_order') - count(distinct order_id) filter (WHERE action = 'cancel_order') as actual_orders,
       round((count(distinct order_id) filter (WHERE action = 'create_order') - count(distinct order_id) filter (WHERE action = 'cancel_order'))::decimal / count(distinct order_id) filter (WHERE action = 'create_order'),
             3) as success_rate
FROM   user_actions
WHERE  time >= '2022-08-24'
   and time < '2022-09-07'
GROUP BY weekday_number, weekday
ORDER BY weekday_number
```

# 8 урок (JOIN)

**Задача № 17**

```sql
with cancel as (SELECT users.user_id,
                       coalesce(users.sex, 'unknown') as sex,
                       1.0 * count(user_actions.user_id) filter (WHERE action = 'cancel_order') / count(distinct order_id) as cancel_rate
                FROM   users
                    RIGHT JOIN user_actions using (user_id)
                GROUP BY users.user_id, sex
                ORDER BY 1 desc)

SELECT sex,
       round(avg(cancel_rate), 3) as avg_cancel_rate
FROM   cancel
GROUP BY sex
```

# 8 урок (JOIN)

**Задача № 20**

```sql
SELECT DISTINCT orders.order_id,
                users.user_id,
                date_part('year', age((SELECT max(time)
                       FROM   user_actions), users.birth_date))::int as user_age, couriers.courier_id, date_part('year', age((SELECT max(time)
                                                                                                       FROM   user_actions), couriers.birth_date))::int as courier_age
FROM   orders join courier_actions
        ON orders.order_id = courier_actions.order_id join user_actions
        ON user_actions.order_id = orders.order_id join users
        ON users.user_id = user_actions.user_id join couriers
        ON couriers.courier_id = courier_actions.courier_id
WHERE  orders.order_id in (SELECT order_id
                           FROM   orders
                           WHERE  array_length(product_ids, 1) = (SELECT max(array_length(product_ids, 1))
                                                                  FROM   orders))
```


**Задача № 21**

```sql
with exploded as (SELECT DISTINCT order_id,
                                  unnest(product_ids) as product_id
                  FROM   orders
                  WHERE  order_id not in (SELECT order_id
                                          FROM   user_actions
                                          WHERE  action = 'cancel_order')), product_pairs as (SELECT t1.product_id as product_1,
                                                           t2.product_id as product_2
                                                    FROM   exploded as t1 join exploded as t2
                                                            ON t1.order_id = t2.order_id and
                                                               t1.product_id < t2.product_id)

SELECT array_sort(array[t3.name, t4.name]) as pair,
       count(*) as count_pair
FROM   product_pairs join products as t3
        ON t3.product_id = product_1 join products as t4
        ON t4.product_id = product_2
GROUP BY t3.name, t4.name
ORDER BY 2 desc, 1
```


# 10 урок (Оконные функции)

**Задача № 10**

```sql
with cte as (SELECT user_id,
                    order_id,
                    time::date as date,
                    dense_rank() OVER (PARTITION BY user_id
                                       ORDER BY time) as rang
             FROM   user_actions
             WHERE  order_id not in (SELECT order_id
                                     FROM   user_actions
                                     WHERE  action = 'cancel_order')
             ORDER BY 1, 2)

SELECT date,
       case when rang = 1 then 'Первый'
            else 'Повторный' end as order_type,
       count(*) as orders_count
FROM   cte
GROUP BY date, order_type
ORDER BY date, order_type
```

**Задача № 13**

```sql
SELECT user_id,
       order_id,
       action,
       time,
       count(*) filter (WHERE action = 'create_order') OVER count_order as created_orders,
       count(*) filter (WHERE action = 'cancel_order') OVER count_order as canceled_orders,
       round(1.0 * count(*) filter (WHERE action = 'cancel_order') OVER percentage / count(*) filter (WHERE action = 'create_order') OVER percentage, 2) as cancel_rate
FROM   user_actions
WINDOW
  count_order as (PARTITION BY user_id ORDER BY time rows between unbounded preceding and current row),
  percentage as (PARTITION BY user_id ORDER BY time rows between unbounded preceding and current row)
ORDER BY 1, 2, 4
limit 1000
```

**Задача № 16**

```sql
with prod as (SELECT order_id,
                     creation_time,
                     unnest(product_ids) as p
              FROM   orders
              WHERE  order_id not in (SELECT order_id
                                      FROM   user_actions
                                      WHERE  action = 'cancel_order')
              ORDER BY 1)

SELECT order_id,
       creation_time,
       sum(price) as order_price,
       sum(sum(price)) OVER (PARTITION BY creation_time::date) as daily_revenue,
       round(100.0 * sum(price) / sum(sum(price)) OVER (PARTITION BY creation_time::date), 3) as percentage_of_daily_revenue
FROM   products join prod
        ON products.product_id = prod.p
GROUP BY order_id, creation_time
ORDER BY creation_time::date desc, percentage_of_daily_revenue desc, order_id
```

**Задача № 17**

```sql
with prod as (SELECT order_id,
                     creation_time,
                     unnest(product_ids) as p
              FROM   orders
              WHERE  order_id not in (SELECT order_id
                                      FROM   user_actions
                                      WHERE  action = 'cancel_order')
              ORDER BY 1)

SELECT creation_time::date as date,
       sum(price) as daily_revenue,
       round(1.0 * last_value(sum(price)) OVER wind - first_value(sum(price)) OVER wind, 1) as revenue_growth_abs,
       round(100.0 * (last_value(sum(price)) OVER wind - first_value(sum(price)) OVER wind) / first_value(sum(price)) OVER wind, 1) as revenue_growth_percentage
FROM   products join prod
        ON products.product_id = prod.p
GROUP BY date
window wind as (ORDER BY creation_time::date rows between 1 preceding and current row)
ORDER BY 1
```

**Задача № 18**

```sql

```





