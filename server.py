import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='retail_store'
)

cursor = conn.cursor()

# Create tables
create_tables_queries = [
    """
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerID INT PRIMARY KEY,
        FirstName VARCHAR(50),
        LastName VARCHAR(50),
        Email VARCHAR(100),
        DateOfBirth DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INT PRIMARY KEY,
        ProductName VARCHAR(100),
        Price DECIMAL(10, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Orders (
        OrderID INT PRIMARY KEY,
        CustomerID INT,
        OrderDate DATE,
        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS OrderItems (
        OrderItemID INT PRIMARY KEY,
        OrderID INT,
        ProductID INT,
        Quantity INT,
        FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
    );
    """
]

for query in create_tables_queries:
    cursor.execute(query)

# Clear tables before inserting data using DELETE
clear_tables_queries = [
    "DELETE FROM OrderItems;",
    "DELETE FROM Orders;",
    "DELETE FROM Products;",
    "DELETE FROM Customers;"
]

for query in clear_tables_queries:
    cursor.execute(query)

# Reset auto-increment values (optional)
reset_auto_increment_queries = [
    "ALTER TABLE OrderItems AUTO_INCREMENT = 1;",
    "ALTER TABLE Orders AUTO_INCREMENT = 1;",
    "ALTER TABLE Products AUTO_INCREMENT = 1;",
    "ALTER TABLE Customers AUTO_INCREMENT = 1;"
]

for query in reset_auto_increment_queries:
    cursor.execute(query)

# Insert data into tables
insert_data_queries = [
    """
    INSERT INTO Customers (CustomerID, FirstName, LastName, Email, DateOfBirth) VALUES
    (1, 'John', 'Doe', 'john.doe@example.com', '1985-01-15'),
    (2, 'Jane', 'Smith', 'jane.smith@example.com', '1990-06-20');
    """,
    """
    INSERT INTO Products (ProductID, ProductName, Price) VALUES
    (1, 'Laptop', 1000),
    (2, 'Smartphone', 600),
    (3, 'Headphones', 100);
    """,
    """
    INSERT INTO Orders (OrderID, CustomerID, OrderDate) VALUES
    (1, 1, '2023-01-10'),
    (2, 2, '2023-01-12');
    """,
    """
    INSERT INTO OrderItems (OrderItemID, OrderID, ProductID, Quantity) VALUES
    (1, 1, 1, 1),
    (2, 1, 3, 2),
    (3, 2, 2, 1),
    (4, 2, 3, 1);
    """
]

for query in insert_data_queries:
    cursor.execute(query)

# Commit the inserts
conn.commit()

# Sample queries
sample_queries = {
    "List all customers": "SELECT * FROM Customers;",
    "Find all orders placed in January 2023": """
        SELECT * FROM Orders
        WHERE OrderDate BETWEEN '2023-01-01' AND '2023-01-31';
    """,
    "Get the details of each order, including the customer name and email": """
        SELECT Orders.OrderID, Customers.FirstName, Customers.LastName, Customers.Email, Orders.OrderDate
        FROM Orders
        JOIN Customers ON Orders.CustomerID = Customers.CustomerID;
    """,
    "List the products purchased in a specific order (e.g., OrderID = 1)": """
        SELECT Products.ProductName, OrderItems.Quantity
        FROM OrderItems
        JOIN Products ON OrderItems.ProductID = Products.ProductID
        WHERE OrderItems.OrderID = 1;
    """,
    "Calculate the total amount spent by each customer": """
        SELECT Customers.CustomerID, Customers.FirstName, Customers.LastName, SUM(Products.Price * OrderItems.Quantity) AS TotalSpent
        FROM Customers
        JOIN Orders ON Customers.CustomerID = Orders.CustomerID
        JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
        JOIN Products ON OrderItems.ProductID = Products.ProductID
        GROUP BY Customers.CustomerID, Customers.FirstName, Customers.LastName;
    """,
    "Find the most popular product (the one that has been ordered the most)": """
        SELECT Products.ProductID, Products.ProductName, SUM(OrderItems.Quantity) AS TotalQuantity
        FROM OrderItems
        JOIN Products ON OrderItems.ProductID = Products.ProductID
        GROUP BY Products.ProductID, Products.ProductName
        ORDER BY TotalQuantity DESC
        LIMIT 1;
    """,
    "Get the total number of orders and the total sales amount for each month in 2023": """
        SELECT DATE_FORMAT(Orders.OrderDate, '%Y-%m') AS Month, 
               COUNT(Orders.OrderID) AS TotalOrders, 
               SUM(Products.Price * OrderItems.Quantity) AS TotalSales
        FROM Orders
        JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
        JOIN Products ON OrderItems.ProductID = Products.ProductID
        WHERE YEAR(Orders.OrderDate) = 2023
        GROUP BY DATE_FORMAT(Orders.OrderDate, '%Y-%m');
    """,
    "Find customers who have spent more than $1000": """
        SELECT Customers.CustomerID, Customers.FirstName, Customers.LastName, SUM(Products.Price * OrderItems.Quantity) AS TotalSpent
        FROM Customers
        JOIN Orders ON Customers.CustomerID = Orders.CustomerID
        JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
        JOIN Products ON OrderItems.ProductID = Products.ProductID
        GROUP BY Customers.CustomerID, Customers.FirstName, Customers.LastName
        HAVING TotalSpent > 1000;
    """
}

# Execute and print the results of each sample query
for description, query in sample_queries.items():
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"\n{description}:")
    for row in results:
        print(row)

# Close cursor and connection
cursor.close()
conn.close()
