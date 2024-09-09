# AWS DataLake

The AWS DataLake is a database hosted on AWS. This process runs a query on the AWS DataLake to extract necessary information and then write to the internal SQL database. The information from Daily Revenue Google Sheets is derived from this, however, the Google Sheets is from a point inm time, whereas the Datalake is real-time.

The query outputs the following columns (Date, Sum of Revenue, and Count of Invoices) using the following parameters (Invoice active = True and  approve date > 0). All times are converted from Unix time into UTC time and then converted to West Coast time (-08:00). It only gets the data for the last 7 days by default. This is a variable that can be customized at runtime.

The date and the location are used as to generate a primary key for the database. If an entry for that location and date already exists, it is overwritten. If it does not exist, it is written.