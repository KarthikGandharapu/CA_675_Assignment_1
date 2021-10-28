--Loading data into pig
QR1 = LOAD 'hdfs://cluster-3317-m/QueryResults_1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (Id:int, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, Title:chararray, Tags:chararray);
QR2 = LOAD 'hdfs://cluster-3317-m/QueryResults_2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (Id:int, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, Title:chararray, Tags:chararray);
QR3 = LOAD 'hdfs://cluster-3317-m/QueryResults_3.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (Id:int, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, Title:chararray, Tags:chararray);
QR4 = LOAD 'hdfs://cluster-3317-m/QueryResults_4.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (Id:int, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, Title:chararray, Tags:chararray);

--Combining all files into one file
Concat_File = UNION QR1, QR2, QR3, QR4;

--Removing Null Values from the data
NULL_R_File = FILTER Concat_File BY (Id IS NOT NULL) AND (Score IS NOT NULL) AND (OwnerUserId IS NOT NULL);

--Removing new lines and spaces such as tabular
Cleaned_File = FOREACH NULL_R_File GENERATE (REPLACE(REPLACE(Body,'\\n|\\r|\\t|\\s',' '),'([^a-zA-Z0-9\\s]+)',' '), Score, Id, ViewCount, OwnerUserId, OwnerDisplayName, REPLACE(REPLACE(Title,',',''), '\n', ' '),REPLACE(REPLACE(Tags,',',''), '\n', ' '));

--Storing the cleaned file into hdfs
STORE Cleaned_File INTO 'hdfs://cluster-3317-m/QFile' USING PigStorage(',');