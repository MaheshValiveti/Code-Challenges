use Test

CREATE TABLE Mahesh_Solution 
(
  name           VARCHAR(1000),
  DOMAIN VARCHAR(1000),
  homepage_url   VARCHAR(1000),
  email          VARCHAR(200),
  phone          VARCHAR(200)
)

DELETE
FROM Mahesh_Solution COMMIT;

SELECT COUNT(*)
FROM Mahesh_Solution LIMIT 1000;

SELECT *
FROM Mahesh_Solution 
where domain ='vastnedretailbelgium.be'


SELECT *
FROM companies
where domain ='vastnedretailbelgium.be'
