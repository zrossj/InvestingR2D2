HOW TO RUN

1. Install the required packages with pip install -r requirements

2. Download the operations file at investidor.b3.com.br/extrato/negociação. 
    - the database updates its data with a incrementally manner. In order to get the right position, you need care to not duplicate the data;

3. Create the necessary tables (init_first_tables.sql)

4. Set up the profiles.yml and the app.properties. Those files must be configured to allow DBT and Python have access to the database. See the documentation on DBT and jproperties lib; 

5. Run the load_raw_data.py using the file that contains the operations data

6. Run "dbt run" command; 

7. Check the main_notebook. You must set up the desired date and wallet to process its updated position. 



Comments:
    You shouldn't advance the wallet's date if you haven't uploaded the operations data from the B3.
    When downloading at B3, be careful to not choose overlaping dates as this can lead to the duplication of existing data.
    Each trade.xlsx file should be inserted only once. 