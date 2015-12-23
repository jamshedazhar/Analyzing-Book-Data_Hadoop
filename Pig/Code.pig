/*
Usecase 1
*/
REGISTER MyUDF.jar;

bxbook = LOAD 'BX-Books.csv' USING PigStorage(';') AS (isbn,bname,bauthor,year,publisher,images,imagem,imagel);
bx = FOREACH bxbook GENERATE  REPLACE(year,'"',''), REPLACE(isbn,'"','');
bxbooks = FILTER bx BY IsNumber($0);
results = GROUP bxbooks BY $0;
freq = FOREACH results GENERATE $0, COUNT($1);
DUMP freq;

/*
Usecase 2
*/

res = GROUP freq ALL;
resultf = FOREACH res GENERATE  MAX($1.$1);
maxbook= FILTER freq BY $1 == resultf.$0;
DUMP maxbook;

/*
Usecase 3
*/
bxrating = LOAD 'BX-Book-Ratings.csv' USING PigStorage(';') as (user,isbn,rating);
bxratings = FOREACH bxrating GENERATE REPLACE(rating,'"',''), REPLACE(isbn,'"','');
book2002 = FILTER bxbooks BY $0 == '2002';
results = JOIN book2002 BY $1 , bxratings BY $1;
result = GROUP results BY $2;
total2002 = FOREACH result GENERATE $0, COUNT($1);
DUMP total2002;
