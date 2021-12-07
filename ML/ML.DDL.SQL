create table ProductInfo(
		productID int NOT NULL, 
		productName varchar(255), 
		manufactuer varchar(255), 
		price float, 
		PRIMARY KEY (productID));


create table saleshistory(
        productID INT, 
        dateSold DATE, 
        amount INT, 
        PRIMARY KEY (productID, dateSold),
        FOREIGN KEY (productID) REFERENCES ProductInfo(productID));

