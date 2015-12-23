--Add csv-serde-1.1.2-0.11.0-all.jar file to SerDe the csv file and remove " from data attribute.
--It can be downloaded from https://drone.io/github.com/ogrodnek/csv-serde/files/target/csv-serde-1.1.2-0.11.0-all.jar ;

ADD JAR csv-serde-1.1.2-0.11.0-all.jar;

--Create table mybook from BX-Books.csv;

CREATE TABLE mybook (
isbn STRING,
bname STRING,
bauthor STRING,
year INT,
publisher STRING,
images STRING,
imagem STRING,
imagel STRING)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
with serdeproperties (
   "separatorChar" = "\;");

LOAD DATA LOCAL INPATH 'BX-Books.csv' into TABLE mybook;


--Use Case 1;
SELECT year, COUNT(isbn) FROM mybook GROUP BY year ;

--Use Case 2;
SELECT year, COUNT(isbn) AS frequency FROM mybook GROUP BY year ORDER BY frequency DESC LIMIT 1;

--Create table mybook from BX-Book-Ratings.csv;
CREATE TABLE bookrating (
uid INT,
isbn STRING,
rating INt)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
with serdeproperties (
   "separatorChar" = "\;");

LOAD DATA LOCAL INPATH 'BX-Book-Ratings.csv' into TABLE bookrating;


--Use Case 3;
SELECT rating, COUNT(rating) FROM bookrating br JOIN mybook mb ON br.isbn = mb.isbn AND mb.year=2002  GROUP BY rating
ORDER BY rating; 
