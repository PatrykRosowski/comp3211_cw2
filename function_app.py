import logging
import azure.functions as func
from azure.functions.decorators.core import DataType
import uuid

app = func.FunctionApp()

@app.function_name(name="data_collection")
@app.schedule(schedule="*/5 * * * * *", arg_name="myTimer", 
              run_on_startup=False,
              use_monitor=False)
@app.generic_output_binding(arg_name="toDoItems", 
                            type="sql", CommandText="dbo.LeedsSensorsData", 
                            ConnectionStringSetting="SqlConnectionString",
                            data_type=DataType.STRING)
def data_collection(myTimer: func.TimerRequest, toDoItems: func.Out[func.SqlRow]) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('data_collection function executed.')

    toDoItems.set(func.SqlRow({"Temperature": 45, 
                               "Wind": 50, 
                               "R_Humidity": 55, 
                               "CO2": 60}))

@app.event_grid_trigger(arg_name="azeventgrid")
def stats(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')
