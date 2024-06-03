# AWS-EC2-EMR-HQL-PySpark
This project aims to leverage AWS EC2, Hive, and Spark to preprocess data and execute a Spark job to classify emails as spam or ham using the TF-IDF (Term Frequency-Inverse Document Frequency) method. The key steps involved in this project are outlined below:

1. Data Preprocessing with Hive: Hive is used to organize and prepare the raw email data stored on AWS EC2 instances. This involves cleaning the data, tokenizing the text, and storing the processed data in a structured format suitable for further analysis.

2. Feature Extraction using TF-IDF: The Spark job utilizes the TF-IDF technique to convert the textual content of emails into numerical features. TF-IDF helps in identifying the importance of terms within each email relative to the entire dataset, which is crucial for distinguishing between spam and ham.

3. Classification:  a classification algorithm is applied to categorize emails as spam or ham. 

By employing AWS EC2 for computational resources, Hive for data preprocessing, and Spark for feature extraction and classification using TF-IDF, this project demonstrates an efficient and scalable approach to handling and analyzing large volumes of email data. The TF-IDF-based classification model aims to improve email filtering systems by accurately identifying spam, thus enhancing overall email security and user experience.
