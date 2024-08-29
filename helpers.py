# helpers.py

from shiny.express import ui
import ibis
from io import StringIO
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

def create_outliers(data, col):
    iqr_bound = 1.5 * (data[col].quantile(0.75) - data[col].quantile(0.25))
    q1 = data[col].quantile(0.25)
    q3 = data[col].quantile(0.75)

    # Filter the data to get outliers
    outliers = data.loc[(data[col] > (q3 + iqr_bound)) | (data[col] < (q1 - iqr_bound))]
    ozone = data.loc[(data[col] <= (q3 + iqr_bound)) & (data[col] >= (q1 - iqr_bound))]
    
    return outliers, ozone

def initialize_database(con, source_db, table_name):
    source_con = ibis.duckdb.connect(database=source_db)
    table = source_con.table(table_name).execute()
    con.create_table(table_name, table)
    source_con.disconnect()

def validate_patch(patch, original_value):
    if patch["column_index"] < 5:
        ui.notification_show(ui.markdown("Only the `Flag` column is editable."), type = "warning")
        return original_value

    if (patch["value"] == "1" or patch["value"] == "0"):
        return patch["value"]

    else:
        ui.notification_show(ui.markdown("`Flag` must be `0` or `1`."), type = "error")
        return original_value

def plot_ozone(x, y, ozone, outliers):
    ozone["Flag"] = "-1"
    combined = pd.concat([outliers, ozone])
    combined["Date"] = combined.Date.astype("string")
    combined["Flag"] = combined["Flag"].astype("string")

    cols = list(set([x, y]))
    combined = combined[[*cols, "Flag", "ID"]]

    fig = px.scatter(
        combined, 
        x=x, 
        y=y, 
        color="Flag", 
        opacity=0.9, 
        color_discrete_map={"-1": "#D3D3D3", "0": "#6ea0ff", "1": "#dc3545"},
        hover_data={"Flag": False}
    )

    fig.update_traces(marker=dict(size=11))
    fig.update_layout(template="plotly_white", showlegend=False)

    fig = go.FigureWidget(fig.data, fig.layout)

    return fig



