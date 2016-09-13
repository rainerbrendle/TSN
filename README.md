# TSN

A method to create a "transactional sequence number" generator using PostgreSQL and GO. 

.sql file describes the defined schema for a PostgreSQL database to create a "Sequence" and a stored function to access it. 
The go code makes this available as a go function - together with a testing framework. 

Database connection parameters to be delivered thru environment variable. 
