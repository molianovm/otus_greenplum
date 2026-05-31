-- ### Hubs
-- Hubs store the unique business keys and provide the primary means of linking data together.

-- 1. Hub_Customer
       CREATE TABLE Hub_Customer (
        Customer_HashKey CHAR(32) PRIMARY KEY,
        CustomerID INTEGER NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL
    );

    insert into Hub_Customer (Customer_HashKey, CustomerID, LoadDate, RecordSource)
    select md5(c_custkey::text),
        c_custkey,
        now(),
        'source_hw2'
    from customer;
    
-- 2. Hub_Order
       CREATE TABLE Hub_Order (
        Order_HashKey CHAR(32) PRIMARY KEY,
        OrderID INTEGER NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL
    );

    insert into Hub_Order (Order_HashKey, OrderID, LoadDate, RecordSource)
    select md5(O_ORDERKEY::text),
        O_ORDERKEY,
        now(),
        'source_hw2'
    from orders;
    
-- 3. Hub_Supplier
       CREATE TABLE Hub_Supplier (
        Supplier_HashKey CHAR(32) PRIMARY KEY,
        SupplierID INTEGER NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL
    );
    
    insert into Hub_Supplier (Supplier_HashKey, SupplierID, LoadDate, RecordSource)
    select md5(S_SUPPKEY::text),
        S_SUPPKEY,
        now(),
        'source_hw2'
    from supplier;

-- 4. Hub_Part
       CREATE TABLE Hub_Part (
        Part_HashKey CHAR(32) PRIMARY KEY,
        PartID INTEGER NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL
    );

    insert into Hub_Part (Part_HashKey, PartID, LoadDate, RecordSource)
    select md5(P_PARTKEY::text),
        P_PARTKEY,
        now(),
        'source_hw2'
    from part;
    
-- 5. Hub_LineItem
       CREATE TABLE Hub_LineItem (
        LineItem_HashKey CHAR(32) PRIMARY KEY,
        LineItemID INTEGER NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL
    );
    
    insert into Hub_LineItem (LineItem_HashKey, LineItemID, LoadDate, RecordSource)
    select md5(row(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER) ::text),
        L_LINENUMBER,
        now(),
        'source_hw2'
    from lineitem;


-- ### Links
-- Links store the relationships between business keys.

-- 1. Link_Customer_Order
       CREATE TABLE Link_Customer_Order (
        Link_HashKey CHAR(32) PRIMARY KEY,
        Customer_HashKey CHAR(32) NOT NULL,
        Order_HashKey CHAR(32) NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_customer FOREIGN KEY (Customer_HashKey) REFERENCES Hub_Customer(Customer_HashKey),
        CONSTRAINT fk_order FOREIGN KEY (Order_HashKey) REFERENCES Hub_Order(Order_HashKey)
    );

    insert into Link_Customer_Order (
        Link_HashKey, Customer_HashKey, Order_HashKey,
        LoadDate, RecordSource
    )
    select MD5(CONCAT(MD5(CAST(o_custkey AS TEXT)), MD5(CAST(o_orderkey AS TEXT)))) AS Link_HashKey,
        MD5(CAST(o_custkey AS TEXT)) AS Customer_HashKey,
        MD5(CAST(o_orderkey AS TEXT)) AS Order_HashKey,
        now(),
        'source_hw2'
    from orders;
    
-- 2. Link_Order_LineItem
       CREATE TABLE Link_Order_LineItem (
        Link_HashKey CHAR(32) PRIMARY KEY,
        Order_HashKey CHAR(32) NOT NULL,
        LineItem_HashKey CHAR(32) NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_order FOREIGN KEY (Order_HashKey) REFERENCES Hub_Order(Order_HashKey),
        CONSTRAINT fk_lineitem FOREIGN KEY (LineItem_HashKey) REFERENCES Hub_LineItem(LineItem_HashKey)
    );

    insert into Link_Order_LineItem (
        Link_HashKey, Order_HashKey, LineItem_HashKey,
        LoadDate, RecordSource
    )
    select MD5(CONCAT(MD5(CAST(l_orderkey AS TEXT)), MD5(CAST(l_linenumber AS TEXT)))) AS Link_HashKey,
        MD5(CAST(l_orderkey AS TEXT)) AS Order_HashKey,
        md5(row(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER) ::text) AS LineItem_HashKey,
        now(),
        'source_hw2'
    from LINEITEM;
    
-- 3. Link_Supplier_Part
       CREATE TABLE Link_Supplier_Part (
        Link_HashKey CHAR(32) PRIMARY KEY,
        Supplier_HashKey CHAR(32) NOT NULL,
        Part_HashKey CHAR(32) NOT NULL,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_supplier FOREIGN KEY (Supplier_HashKey) REFERENCES Hub_Supplier(Supplier_HashKey),
        CONSTRAINT fk_part FOREIGN KEY (Part_HashKey) REFERENCES Hub_Part(Part_HashKey)
    );

    insert into Link_Supplier_Part (
        Link_HashKey, Supplier_HashKey, Part_HashKey,
        LoadDate, RecordSource
    )
    select MD5(CONCAT(MD5(CAST(ps_suppkey AS TEXT)), MD5(CAST(ps_partkey AS TEXT)))) AS Link_HashKey,
        MD5(CAST(ps_suppkey AS TEXT)) AS Supplier_HashKey,
        MD5(CAST(ps_partkey AS TEXT)) AS Part_HashKey,
        now(),
        'source_hw2'
    from PARTSUPP;
    

-- ### Satellites
-- Satellites store the descriptive data related to business keys or relationships.

-- 1. Satellite_Customer
       CREATE TABLE Satellite_Customer (
        Customer_HashKey CHAR(32) NOT NULL,
        CustomerName VARCHAR(50),
        CustomerAddress VARCHAR(100),
        CustomerPhone VARCHAR(20),
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_customer FOREIGN KEY (Customer_HashKey) REFERENCES Hub_Customer(Customer_HashKey)
    );

    INSERT INTO Satellite_Customer (Customer_HashKey, CustomerName, CustomerAddress, CustomerPhone, LoadDate, RecordSource)
    SELECT 
        MD5(CAST(c_custkey AS TEXT)) AS Customer_HashKey,
        c_name AS CustomerName,
        c_address AS CustomerAddress,
        c_phone AS CustomerPhone,
        now(),
        'source_hw2'
    FROM CUSTOMER;
    
-- 2. Satellite_Order
       CREATE TABLE Satellite_Order (
        Order_HashKey CHAR(32) NOT NULL,
        OrderDate DATE,
        ShipDate DATE,
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_order FOREIGN KEY (Order_HashKey) REFERENCES Hub_Order(Order_HashKey)
    );

    INSERT INTO Satellite_Order (Order_HashKey, OrderDate, ShipDate, LoadDate, RecordSource)
    SELECT 
        MD5(CAST(o_orderkey AS TEXT)) AS Order_HashKey,
        o_orderdate AS OrderDate,
        o_orderdate + interval '1 month' AS ShipDate,
        now(),
        'source_hw2'
    FROM ORDERS;
    
-- 3. Satellite_Supplier
       CREATE TABLE Satellite_Supplier (
        Supplier_HashKey CHAR(32) NOT NULL,
        SupplierName VARCHAR(50),
        SupplierAddress VARCHAR(100),
        SupplierPhone VARCHAR(20),
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_supplier FOREIGN KEY (Supplier_HashKey) REFERENCES Hub_Supplier(Supplier_HashKey)
    );

    INSERT INTO Satellite_Supplier (Supplier_HashKey, SupplierName, SupplierAddress, SupplierPhone, LoadDate, RecordSource)
    SELECT 
        MD5(CAST(s_suppkey AS TEXT)) AS Supplier_HashKey,
        s_name AS SupplierName,
        s_address AS SupplierAddress,
        s_phone AS SupplierPhone,
        now(),
        'source_hw2'
    FROM SUPPLIER;
    
-- 4. Satellite_Part
       CREATE TABLE Satellite_Part (
        Part_HashKey CHAR(32) NOT NULL,
        PartName VARCHAR(50),
        PartDescription VARCHAR(200),
        PartPrice DECIMAL(10, 2),
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_part FOREIGN KEY (Part_HashKey) REFERENCES Hub_Part(Part_HashKey)
    );

    INSERT INTO Satellite_Part (Part_HashKey, PartName, PartDescription, PartPrice, LoadDate, RecordSource)
    SELECT 
        MD5(CAST(p_partkey AS TEXT)) AS Part_HashKey,
        p_name::VARCHAR(50) AS PartName,
        p_mfgr AS PartDescription,
        p_retailprice AS PartPrice,
        now(),
        'source_hw2'
    FROM PART;
    
-- 5. Satellite_LineItem
       CREATE TABLE Satellite_LineItem (
        LineItem_HashKey CHAR(32) NOT NULL,
        Quantity INTEGER,
        Price DECIMAL(10, 2),
        Discount DECIMAL(5, 2),
        LoadDate TIMESTAMP NOT NULL,
        RecordSource VARCHAR(50) NOT NULL,
        CONSTRAINT fk_lineitem FOREIGN KEY (LineItem_HashKey) REFERENCES Hub_LineItem(LineItem_HashKey)
    );

    INSERT INTO Satellite_LineItem (LineItem_HashKey, Quantity, Price, Discount, LoadDate, RecordSource)
    SELECT 
        md5(row(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER) ::text) AS LineItem_HashKey,
        l_quantity AS Quantity,
        l_extendedprice AS Price,
        l_discount AS Discount,
        now(),
        'source_hw2'
    FROM LINEITEM;