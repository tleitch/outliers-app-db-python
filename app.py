# app.py

from shiny.express import input, ui, render, session
from shiny import reactive
from shinywidgets import render_plotly 
import plotly.express as px
import pandas as pd
import ibis
import helpers
#import plotly.graph_objects as go
from plotly.callbacks import Points

ui.page_opts(fillable=True, title="Identify suspicious values in air quality data")

# Initialize the Ibis connection
con = ibis.duckdb.connect(database=':memory:')

# Read initial data and initialize the table
helpers.initialize_database(con, "data/ozone.duckdb", "ozone")

ozone = con.table("ozone").to_pandas()
ozone.rename(
    columns={
        "id": "ID",
        "state_name": "State",
        "date_local": "Date",
        "arithmetic_mean": "PPM",
        "aqi": "AQI",
        "flag": "Flag",
    },
    inplace=True,
)

ozone = ozone[["ID", "State", "Date", "PPM", "AQI", "Flag"]]
ozone.loc["Flag"] = ozone["Flag"].astype("string")
outliers, ozone = helpers.create_outliers(ozone, "PPM")

# ui.markdown("## Identify suspicious values in air quality data")

with ui.layout_columns():

    with ui.card():
        ui.card_header("Click on an outlier to highlight the point in the table.")

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
        
    with ui.card():
        ui.card_header(ui.markdown("Change `Flag` to `1` to flag a value as an error. Flagged points will appear red in the plot."))
        
        @render.data_frame
        def outliers_editable():
            outliers["Date"] = outliers.Date.astype("string")
            return render.DataGrid(
                outliers, 
                editable=True,
                selection_mode="rows"
            )
        
        @reactive.effect
        async def _():
            points: Points | None = pt_selected.get()
            if points is None:
                await outliers_editable.update_cell_selection({"type": "row", "rows": []})
            
            else:
                point_inds: list[int] = points.point_inds

                df = outliers_editable.data_view().reset_index()
                df_original = outliers_editable.data().reset_index()

                df_original["ID"] = pd.to_numeric(df_original["ID"])
                df["Flag"] = df["Flag"].astype("string")

                flag_inds = list(df[df["Flag"] == points.trace_name].index)
                df_inds = [flag_inds[i] for i in point_inds if i < len(flag_inds)]
                id = df.loc[df_inds, "ID"].values[0]
                original_index = df_original[df_original["ID"] == id].index.values.astype(int)[0].item()

                await outliers_editable.update_cell_selection({"type": "row", "rows": original_index})

        
        ui.input_action_button("write_data", "Write to database", width="50%")

        @outliers_editable.set_patch_fn
        def upgrade_patch(*, patch):
            pt_selected.set(None)
            return helpers.validate_patch(
                patch, 
                outliers_editable.data().iloc[patch["row_index"], patch["column_index"]]
            )

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

    # Update only the changed rows in the DuckDB database
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

    # ozone_new = con.table("ozone")
    # expr = ozone_new.flag == 1
    # print(expr.value_counts().execute())