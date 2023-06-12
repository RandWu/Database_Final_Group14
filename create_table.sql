-- Table 1: Users
CREATE TABLE Users (
    user_id NUMBER PRIMARY KEY
);

-- Table 2: Departments
CREATE TABLE Departments2 (
    department_id NUMBER PRIMARY KEY,
    department VARCHAR2(255)
);

-- Table 3: Products
CREATE TABLE Products (
    product_id NUMBER PRIMARY KEY,
    department_id NUMBER REFERENCES Departments2(department_id),
    product_name VARCHAR2(255)
);

-- Table 4: Orders
CREATE TABLE Orders (
    order_id NUMBER PRIMARY KEY,
    user_id NUMBER REFERENCES Users(user_id),
    order_dow NUMBER,
    order_hour_of_day NUMBER,
    days_since_prior_order NUMBER
);

-- Table 5: Order Details
CREATE TABLE Order_Details (
    order_id NUMBER REFERENCES Orders(order_id),
    product_id NUMBER REFERENCES Products(product_id),
    add_to_cart_order NUMBER,
    reordered NUMBER
);
