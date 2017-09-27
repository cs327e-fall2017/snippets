/* instructions: 

1) download the full instacart dataset from: http://cs327e-fall2017-imdb.s3.amazonaws.com/postgres/instacart_full.zip

2) unzip instacart_full.zip

3) change copy command file paths below based on the location of files on your machine

*/

\c instacart;

\copy Aisles from C:/utcs_work/cs327e_fall_2017/datasets/instacart/instacart_2017_05_01/instacart_full/aisles.csv (header FALSE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy Departments from C:/utcs_work/cs327e_fall_2017/datasets/instacart/instacart_2017_05_01/instacart_full/departments.csv (header FALSE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy Orders from C:/utcs_work/cs327e_fall_2017/datasets/instacart/instacart_2017_05_01/instacart_full/orders.csv (header FALSE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy Products from C:/utcs_work/cs327e_fall_2017/datasets/instacart/instacart_2017_05_01/instacart_full/products.csv (header FALSE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy Order_Products from C:/utcs_work/cs327e_fall_2017/datasets/instacart/instacart_2017_05_01/instacart_full/order_products.csv (header FALSE, format csv, delimiter ',', null '', encoding 'UTF8');

