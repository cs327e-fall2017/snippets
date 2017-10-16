/* Practice Problem 1:               */
/* Find instacart's core customers   */
select user_id, count(distinct o.order_id) as orders_placed
from Orders o join Order_Products op on o.order_id = op.order_id
where days_since_prior_order <= 7 and add_to_cart_order >= 5
group by user_id
having count(distinct o.order_id) >= 10
order by orders_placed desc
limit 20;


/* create a virtual view over the core customers query */
/* Use this type of view to hide the complexity of the aggregate query */ 
/* and when the query response time is sufficient (< 3 seconds) */
/* Note: response time is measured using \timing in the psql shell */
create view v_instacart_core_customers as
select user_id, count(distinct o.order_id) as orders_placed
from Orders o join Order_Products op on o.order_id = op.order_id
where days_since_prior_order <= 7 and add_to_cart_order >= 5
group by user_id
having count(distinct o.order_id) >= 10
order by orders_placed desc;



/* create a materialized view to cache the query results */
/* Use this type of view to speed up query processing when the query */
/* response time is >= 3 sec */
/* Note: response time is measured using \timing in the psql shell */
create materialized view v_instacart_core_customers as
select user_id, count(distinct o.order_id) as orders_placed
from Orders o join Order_Products op on o.order_id = op.order_id
where days_since_prior_order <= 7 and add_to_cart_order >= 5
group by user_id
having count(distinct o.order_id) >= 10
order by orders_placed desc;

