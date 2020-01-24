CREATE TABLE IF NOT EXISTS Costs(id SERIAL, cost DECIMAL);
INSERT INTO Costs(cost) values(20), (25), (15), (45);

CREATE TABLE IF NOT EXISTS Sales(sale_price DECIMAL, date DATE);
INSERT INTO Sales VALUES(23.99, '2019-12-20'), (15.99, '2020-01-16'), (14.49, '2020-01-20'), (12.00, '2019-11-23');