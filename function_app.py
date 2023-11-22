import logging, os, pyodbc
import azure.functions as func


app = func.FunctionApp()

@app.function_name(name="data_collection")
@app.schedule(schedule="*/5 * * * * *", arg_name="myTimer", 
              run_on_startup=False,
              use_monitor=False)

def data_collection(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('data_collection function executed.')

    # Establish a database connection using the connection string stored in
    # local.settings.json (local) and an environment variable on Azure (deployed)
    db_connection = pyodbc.connect(os.environ["SqlConnectionString"])
    # Create a cursor object to manipulate the db table using sql queries
    cursor = db_connection.cursor()

    query = """
        IF EXISTS (SELECT 1 FROM LeedsSensorsData)
        BEGIN
            UPDATE LeedsSensorsData
            SET Temperature = 100, 
                Wind = 100, 
                R_Humidity = 100, 
                CO2 = 100
            WHERE Sensor_ID = 1
        END
        ELSE
        BEGIN
            INSERT INTO LeedsSensorsData (Sensor_ID, Temperature, Wind, R_Humidity, CO2)
            VALUES (200, 200, 200, 200, 200)
        END

    """
    # Use the cursor to execute the update/insert query
    cursor.execute(query)
    # Commit the db changes
    db_connection.commit()

    # Clean up by closing the cursor object and the db connection
    cursor.close()
    db_connection.close()

@app.event_grid_trigger(arg_name="azeventgrid")
def stats(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')
