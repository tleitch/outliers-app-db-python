# app.py

from shiny.express import input, ui, render, session
from shiny import reactive
from shinywidgets import render_plotly 
import plotly.express as px
import pandas as pd
import ibis
from plotly.callbacks import Points
from faicons import icon_svg
import helpers

# Initialize the Ibis connection
con = ibis.duckdb.connect(database=':memory:')

# Read initial data and initialize the table
helpers.initialize_database(con, "data/ozone.duckdb", "ozone")

ozone = con.table("ozone").rename(
    {
        "ID":"id",
        "State": "state_name",
        "Date": "date_local",
        "PPM": "arithmetic_mean",
        "AQI": "aqi",
        "Flag": "flag",
    }
).select(["ID", "State", "Date", "PPM", "AQI", "Flag"])

ozone = ozone.mutate(Flag=ozone.Flag.cast('string'))
outliers, ozone = helpers.create_outliers_table(ozone, "PPM")
# Need the entire outliers table
outliers = outliers.to_pandas()

ui.page_opts(fillable=True, title="Identify suspicious values in air quality data")
with ui.layout_columns():

    with ui.card():
        ui.card_header(
            ui.markdown(
                f"{icon_svg('circle-info')} Click on a suspicious value (in blue) to highlight the point in the table."
            ),
            class_="bg-light"
        )

        with ui.layout_columns():
            ui.input_select("x", "X-axis variable:", choices=["Date", "PPM", "AQI"])
            ui.input_select("y", "Y-axis variable:", choices=["PPM", "AQI"])

        @render_plotly
        def plot():
            fig = helpers.plot_ozone(input.x(), input.y(), ozone, outliers_editable.data_view()) 
            fig.data[0].on_click(on_point_click) # Color 1
            fig.data[1].on_click(on_point_click) # Color 2
            return fig

        pt_selected = reactive.value()

        def on_point_click(trace, points, state):
            if len(points.point_inds) > 0:
                pt_selected.set(points)
    
    with ui.layout_column_wrap(width=1, heights_equal="row"):
        with ui.card():
            ui.card_header(
                ui.markdown(
                    f"{icon_svg('circle-info')}Change `Flag` to `1` to flag a value as an error. Flagged points will appear red in the plot."
                ),
                class_="bg-light"
            )
        
            @render.data_frame
            def outliers_editable():
                # outliers["Date"] = outliers.Date.astype("string")
                return helpers.create_editable_table(outliers)

            ui.input_action_button("write_data", "Write to database", width="40%")
        
        with ui.card():
            ui.card_header("About this app", class_="bg-light")
            
            ui.markdown(
                """ This app uses ozone data from the [EPA](https://www.epa.gov/outdoor-air-quality-data). 
                The values shown in blue represent rows where `PPM` (ozone level in parts-per-million) was an outlier, 
                identified using the [IQR method](https://en.wikipedia.org/wiki/Interquartile_range#Outliers). 
                Some of these values are real, but some are errors, created for the purposes of this app.  
                \nThe app reads from and writes to an in-memory DuckDB database. 
                When you refresh the page, the database will be regenerated from scratch, so you will not see your changes."""
            )

# Highlight corresponding row on point click
@reactive.effect
async def _():
    points: Points | None = pt_selected.get()
    if points is None:
        await outliers_editable.update_cell_selection({"type": "row", "rows": []})
    
    else:
        original_index = helpers.find_row_number(points, outliers_editable)
        await outliers_editable.update_cell_selection({"type": "row", "rows": original_index})

# Validate edit 
@outliers_editable.set_patch_fn
def upgrade_patch(*, patch):
    pt_selected.set(None)
    return helpers.validate_patch(
        patch, 
        outliers_editable.data().iloc[patch["row_index"], patch["column_index"]]
    )

# Write data to the database
@reactive.Effect
@reactive.event(input.write_data)
def write_data():

    # Capture the current state of the edited data from DataGrid
    edited_data = outliers_editable.data_view()

    # Find the rows where the flag value has changed
    changed_values = pd.merge(
        outliers,
        edited_data,
        on=["ID", "State", "Date", "PPM", "AQI"],  
        suffixes=('_old', '_new')
    )

    rows_to_update = changed_values[changed_values["Flag_old"] != changed_values["Flag_new"]]

    # Update only the changed rows
    for _, row in rows_to_update.iterrows():
        outliers.loc[outliers['ID'] == row['ID'], 'Flag'] = row['Flag_new']
        sql_query = f"UPDATE ozone SET flag = '{row['Flag_new']}' WHERE id = '{row['ID']}'"
        con.raw_sql(sql_query)
    
    
    n_rows = len(rows_to_update)
    if n_rows > 0:
        if n_rows == 1:
            values = "value"
        else:
            values = "values"
        ui.notification_show(
            ui.markdown(f"`{n_rows}` {values} successfully updated in database."),
            type="message", 
            duration=5
        )
    else:
        ui.notification_show(ui.markdown("No changes to write to database."), type="warning")
