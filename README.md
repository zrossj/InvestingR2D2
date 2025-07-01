HOW TO RUN

1. Install the required packages with pip install -r requirements

1. Download the operations file at investidor.b3.com.br/extrato/negociação. 
    - the database updates its data with a incrementally manner. In order to get the right position, you need to care to not duplicate the data;

2. Run load_raw_data.py using the file you donwloaded above;

3. Set up the profiles.yml and the app.properties. Those files must be configured to allow DBT and Python have access to the database. See the documentation on DBT and jproperties lib; 

4. Run the queries on init_first_tables.sql

5. Run "dbt run" command; 
