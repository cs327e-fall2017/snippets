\c dev;

drop database if exists instacart;

create database instacart;

\c instacart;

create table Aisles(
   aisle_id int primary key,
   aisle_name varchar(30)
);

create table Departments(
	department_id int primary key,
	department_name varchar(20)
);

create table Orders(
    order_id int primary key,
	user_id int,
	eval_set varchar(10),
	order_number int,
	order_dow int,
	order_hour_of_day int,
	days_since_prior_order numeric(6, 2)
);

create table Products(
	product_id int primary key,
	product_name varchar(160),
	aisle_id int,
	department_id int,
	foreign key(aisle_id) references Aisles(aisle_id),
	foreign key(department_id) references Departments(department_id)
);

create table Order_Products(
	order_id int,
	product_id int,
	add_to_cart_order int,
	reordered_by_user boolean,
	primary key(order_id, product_id)
);

/* To speed up the load, first run the copy command and then add foreign keys to the Order_Products table */

alter table Order_Products add foreign key(order_id) references Orders(order_id);
alter table Order_Products add foreign key(product_id) references Products(product_id);
