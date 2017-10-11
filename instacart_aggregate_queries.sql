/* Practice Problems 1 */
/* Find the total number of orders placed on the weekend */
select count(*) 
from orders where order_dow in (6, 0);   


/* Find the highest number of products in an order */
select max(add_to_cart_order)
from order_products;                 


/* Find the average number of days between orders */
select avg(days_since_prior_order) 
from orders;                       


/* Practice Problem 2 */
/* Find the 10 most popular products (popular = frequently ordered) */
select product_name, count(*) 
from order_products op join products p on op.product_id = p.product_id
group by product_name
order by count(*) desc 
limit 10;


/* Practice Problem 3 */
/* Find the 10 least popular products (popular = frequently ordered) */
select product_name, p.product_id, count(op.product_id)
from order_products op right outer join products p on op.product_id = p.product_id
group by product_name, p.product_id 
order by count(op.product_id)
limit 10;


