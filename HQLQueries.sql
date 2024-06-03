--\* Table creation*\

CREATE EXTERNAL TABLE spam_ham (
  sender STRING,
  receiver STRING,
  dates STRING,
  subject STRING,
  body_ STRING,
  label STRING,
  urls  STRING

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://cloudca675data-bucket/inputdata/';


--\* Second Table for for processing *\


CREATE TABLE IF NOT EXISTS spam_ham_processed
STORED AS PARQUET
SELECT sender,receiver,subject,body_
FROM spam_ham
WHERE body_ IS NOT NULL 
  AND body_ NOT LIKE '%>+=+%' 
  AND sender RLIKE '^([0-9]|[a-z]|[A-Z])', 
  AND receiver RLIKE '^([0-9]|[a-z]|[A-Z])';

  --\* Calssification Table on top of spam_ham_processed table*\

  CREATE TABLE spam_classification AS
SELECT
    sender,
    receiver,
    dates,
    subject,
    body_,
    CASE
        WHEN LOWER(subject) RLIKE '.*earn.*' OR LOWER(body_) RLIKE '.*earn.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*cash.*' OR LOWER(body_) RLIKE '.*cash.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*free.*' OR LOWER(body_) RLIKE '.*free.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*cnn.*' OR LOWER(body_) RLIKE '.*cnn.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*order.*' OR LOWER(body_) RLIKE '.*order.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*credit.*' OR LOWER(body_) RLIKE '.*credit.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*debit.*' OR LOWER(body_) RLIKE '.*debit.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*sold.*' OR LOWER(body_) RLIKE '.*sold.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*sale.*' OR LOWER(body_) RLIKE '.*sale.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*100.*' OR LOWER(body_) RLIKE '.*100.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*money.*' OR LOWER(body_) RLIKE '.*money.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*urgent.*' OR LOWER(body_) RLIKE '.*urgent.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*price.*' OR LOWER(body_) RLIKE '.*price.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*welcome.*' OR LOWER(body_) RLIKE '.*welcome.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*click.*' OR LOWER(body_) RLIKE '.*click.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*amazing.*' OR LOWER(body_) RLIKE '.*amazing.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*guaranteed.*' OR LOWER(body_) RLIKE '.*guaranteed.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*benefit.*' OR LOWER(body_) RLIKE '.*benefit.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*looking.*' OR LOWER(body_) RLIKE '.*looking.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*affordable.*' OR LOWER(body_) RLIKE '.*affordable.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*save.*' OR LOWER(body_) RLIKE '.*save.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*biggest.*' OR LOWER(body_) RLIKE '.*biggest.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*mega.*' OR LOWER(body_) RLIKE '.*mega.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*get.*' OR LOWER(body_) RLIKE '.*get.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*shop.*' OR LOWER(body_) RLIKE '.*shop.*' THEN 'spam'
        WHEN LOWER(subject) RLIKE '.*great.*' OR LOWER(body_) RLIKE '.*great.*' THEN 'spam'
        ELSE 'ham'
    END AS classification
FROM spam_ham_processed;

---Spam--
SELECT sender, receiver,subject,body_, COUNT(*) AS spam_count
FROM spam_classification
WHERE classification = 'spam'
GROUP BY sender, receiver,dates,subject,body_
ORDER BY spam_count DESC
LIMIT 10;

---Spam extract--

INSERT OVERWRITE LOCAL DIRECTORY 's3://cloudca675data-bucket/hive_output_csv/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT sender, receiver,dates,subject,body_, COUNT(*) AS spam_count
FROM spam_classification
WHERE classification = 'spam'
GROUP BY sender, receiver,dates,subject,body_
ORDER BY spam_count DESC
LIMIT 20;

-----Ham--

SELECT sender, receiver, dates,subject,body_, COUNT(*) AS ham_count
FROM spam_classification
WHERE classification = 'ham'
GROUP BY sender, receiver, dates,subject,body_
ORDER BY ham_count DESC
LIMIT 10;

--Ham Extract--

INSERT OVERWRITE LOCAL DIRECTORY 's3://cloudca675data-bucket/hive_output_csv/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT sender, receiver, dates,subject,body_, COUNT(*) AS ham_count
FROM spam_classification
WHERE classification = 'ham'
GROUP BY sender, receiver, dates,subject,body_
ORDER BY ham_count DESC
LIMIT 10;
