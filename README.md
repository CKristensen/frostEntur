Gets the weather reports for each buss station using FrostAPi and Entur.

Goes into FROSTAPI and Entur and gets all the buss station and weather station data in Oslo.
Airflow DAG that gets the weather data from yesterday and updates the star schema in the database.

Database in use: Postgresql hosted in AWS.
Capy the DAG into Airflow schelduler to run the update function once a day.
