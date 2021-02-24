# ny_health_data

## Running the job every day at 9 AM 

* 9 * * * python main.py db_loc table_prefix URL countystr

### Example Arguments:

db_loc=C:\sqlite\db\covid_newyork.db\
table_prefix=covid_newyork\
countrystr=Albany,Allegany, etc.
