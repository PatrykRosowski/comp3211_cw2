import logging, os, pyodbc, json
import azure.functions as func
from random import randint as randint


app = func.FunctionApp()

# The function used for simulating data collection from sensors
# and performing an insert/update operation in the database
@app.function_name(name="data_collection")
@app.schedule(schedule="*/5 * * * * *", arg_name="myTimer", 
              run_on_startup=True,
              use_monitor=False)
def data_collection(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('data_collection function executed.')

    # The variable holding the number of sensors we are collecting data from
    num_of_sensors = 20

    # A nested dictionary storing data from each sensor
    # key = sensor ID
    # value = a dictionary with the data for this sensor
    collected_data = {}

    # This 'for' loop generates random data for each sensor ID
    # This data is added into 'collected_data' to create a nested dictionary
    for sensor_ID in range(1, num_of_sensors + 1):
        collected_data[sensor_ID] = {
            "Temperature": randint(8, 15),
            "Wind": randint(15, 25),
            "R_Humidity": randint(40, 70),
            "CO2": randint(500, 1500)
        }

    # Establish a database connection using the connection string stored in
    # local.settings.json (local) and an environment variable on Azure (deployed)
    db_connection = pyodbc.connect(os.environ["SqlConnectionStringTask1"])
    # Create a cursor object to manipulate the db table using sql queries
    cursor = db_connection.cursor()

    # The sql query used to update/insert data into the table,
    # depending on whether the records already exist or not
    query = """
        IF EXISTS (SELECT 1 FROM LeedsSensorsData WHERE Sensor_ID = ?)
        BEGIN
            UPDATE LeedsSensorsData
            SET Temperature = ?, 
                Wind = ?, 
                R_Humidity = ?, 
                CO2 = ?
            WHERE Sensor_ID = ?
        END
        ELSE
        BEGIN
            INSERT INTO LeedsSensorsData (Temperature, Wind, R_Humidity, CO2)
            VALUES (?, ?, ?, ?)
        END
    """

    # To improve efficiency I will make sure that the changes are only committed
    # after the program has executed the sql query for each sensor in the dictionary
    db_connection.autocommit = False

    # To ensure that the ACID rules are followed, I will use the try/except statements
    # and rollback any changes in case of an error
    try:
        for sensor_ID, data in collected_data.items():
            # Data used if records already exist and need to be updates
            update_info = (
                sensor_ID,
                data["Temperature"],
                data["Wind"],
                data["R_Humidity"],
                data["CO2"],
                sensor_ID
            )
            # Data used if the LeedsSensorsData table is empty
            insert_info = (
                data["Temperature"],
                data["Wind"],
                data["R_Humidity"],
                data["CO2"]
            )

            # Use the cursor to execute the update/insert query
            cursor.execute(query, update_info + insert_info)

        # Commit the db changes
        db_connection.commit()

    except pyodbc.Error as error:
        db_connection.rollback()
        print(error)

    # Set the autocommit attribute back to True, its default value, for clean up
    db_connection.autocommit = True
    # Close the cursor object and the db connection
    cursor.close()
    db_connection.close()

# The function responsible for calculating statistical data
# It gets triggered whenever a change in the database is detected
@app.function_name(name="stats")
@app.generic_trigger(arg_name="change", type="sqlTrigger",
                        TableName="LeedsSensorsData",
                        ConnectionStringSetting="SqlConnectionStringTask2")
def stats(change: str) -> None:
    logging.info("SQL Changes: %s", json.loads(change))

    """
    # Establish a database connection using the connection string stored in
    # local.settings.json (local) and an environment variable on Azure (deployed)
    db_connection = pyodbc.connect(os.environ["SqlConnectionString"])
    # Create a cursor object to manipulate the db table using sql queries
    cursor = db_connection.cursor()"""