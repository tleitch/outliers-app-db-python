# app.py

from shiny.express import input, ui, render
from shinywidgets import render_widget  
import plotly.express as px
import pandas as pd
import duckdb
import ibis
from plot import plot_ozone
import datetime
import json


ui.page_opts(fillable=True)


def create_outliers(data, col):
    iqr_bound = 1.5 * (data[col].quantile(0.75) - data[col].quantile(0.25))
    q1 = data[col].quantile(0.25)
    q3 = data[col].quantile(0.75)

    # Filter the data to get outliers
    outliers = data[(data[col] > (q3 + iqr_bound)) | (data[col] < (q1 - iqr_bound))]
    
    return outliers

def initialize_database(con, source_db, table_name):
    source_con = ibis.duckdb.connect(database=source_db)
    table = source_con.table(table_name).execute()
    con.create_table(table_name, table)
    source_con.disconnect()

# Initialize the Ibis connection
con = ibis.duckdb.connect(database=':memory:')

# Read initial data and initialize the table
initialize_database(con, "data/ozone.duckdb", "ozone")

ozone = con.table("ozone").to_pandas()
ozone.rename(
    columns={
        "state_name": "State",
        "date_local": "Date",
        "arithmetic_mean": "PPM",
        "aqi": "AQI",
        "flag": "Flag",
    },
    inplace=True,
)

ozone = ozone[["State", "Date", "PPM", "AQI", "Flag"]]
outliers = create_outliers(ozone, "PPM")

ui.markdown("## Identify outliers in air quality data")

with ui.layout_columns():

    with ui.card():
        ui.card_header("Click on an outlier to highlight the point in the table.")

        with ui.layout_columns():
            ui.input_select("x", "X axis", choices=["Date", "PPM", "AQI"])
            ui.input_select("y", "Y axis", choices=["PPM", "AQI"])

        @render_widget  
        def plot():
            return plot_ozone(input.x(), input.y(), ozone, outliers)  
    
    with ui.card():
        ui.card_header(ui.markdown("Change `Flag` to `1` to flag a value as an error. Flagged points will appear red in the plot."))
        
        @render.data_frame
        def table():
            return render.DataTable(
                outliers,
                editable=True
            )
    
    con.disconnect()



                
  