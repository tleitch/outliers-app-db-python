# app.py

from shiny.express import input, ui, render, session
from shiny import reactive
from shinywidgets import render_widget  
import plotly.express as px
import pandas as pd
import duckdb
import ibis
from plot import plot_ozone
import datetime
import json
#from table import create_outliers_table


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

ozone = ozone[["id", "State", "Date", "PPM", "AQI", "Flag"]]
ozone["Flag"] = ozone["Flag"].astype("string")
outliers = create_outliers(ozone, "PPM")

selected_row = reactive.value(-1)

# outliers_editable = reactive.value()

# @reactive.calc
# def outliers_editable():
    # return outliers

ui.markdown("## Identify outliers in air quality data")

with ui.layout_columns():

    with ui.card():
        ui.card_header("Click on an outlier to highlight the point in the table.")

        with ui.layout_columns():
            ui.input_select("x", "X axis", choices=["Date", "PPM", "AQI"])
            ui.input_select("y", "Y axis", choices=["PPM", "AQI"])

        @render_widget  
        def plot():
            return plot_ozone(input.x(), input.y(), ozone, outliers_editable.data_view() )  
    
    with ui.card():
        ui.card_header(ui.markdown("Change `Flag` to `1` to flag a value as an error. Flagged points will appear red in the plot."))
        
        @render.data_frame
        def outliers_editable():
            return render.DataGrid(
                outliers, 
                editable=True
            )
        
        ui.input_action_button("write_data", "Write to database")


@reactive.Effect
@reactive.event(input.write_data)
def write_data():
    # Capture the current state of the edited data from DataGrid
    edited_data = outliers_editable.data_view()

    # Find the rows where the flag value has changed
    changed_flags = pd.merge(
        outliers,
        edited_data,
        on=["id", "State", "Date", "PPM", "AQI"],  # Assuming 'id' is a unique identifier
        suffixes=('_old', '_new')
    )

    rows_to_update = changed_flags[changed_flags["Flag_old"] != changed_flags["Flag_new"]]

    # Update only the changed rows in the DuckDB database
    for _, row in rows_to_update.iterrows():
        sql_query = f"UPDATE ozone SET flag = '{row['Flag_new']}' WHERE id = '{row['id']}'"
        con.raw_sql(sql_query)

    # ozone_new = con.table("ozone")
    # expr = ozone_new.flag == 1
    # print(expr.value_counts().execute())