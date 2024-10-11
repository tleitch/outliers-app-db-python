# helpers.py

from shiny.express import ui, render
import ibis
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

def create_outliers_table(table, col):
    # Calculate the IQR (Interquartile Range) bounds using Ibis
    iqr_bound = 1.5 * (table[col].quantile(0.75) - table[col].quantile(0.25))
    q1 = table[col].quantile(0.25)
    q3 = table[col].quantile(0.75) 

    # Create conditions for outliers and non-outliers
    outlier_condition = (table[col] > (q3 + iqr_bound)) | (table[col] < (q1 - iqr_bound))
    non_outlier_condition = (table[col] <= (q3 + iqr_bound)) & (table[col] >= (q1 - iqr_bound))

    # Filter the table to get outliers and non-outliers
    outliers = table.filter(outlier_condition)
    ozone = table.filter(non_outlier_condition)
    
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
    # Need the entire table for plotting
    ozone = ozone.to_pandas()
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
        opacity=0.8, 
        color_discrete_map={"-1": "#D3D3D3", "0": "#6ea0ff", "1": "#dc3545"},
        hover_data={"Flag": False}
    )

    fig.update_traces(marker=dict(size=12))
    fig.update_layout(template="plotly_white", showlegend=False)

    fig = go.FigureWidget(fig.data, fig.layout)

    return fig

def create_editable_table(df):
    df["Date"] = df.Date.astype("string")
    return render.DataGrid(
        df, 
        editable=True,
        selection_mode="rows",
        summary=False,
        styles={"style": {"font-size": "16px", "padding-top": "12px", "padding-bottom": "12px"}}
    )

def find_row_number(points, editable_table):
    point_inds: list[int] = points.point_inds
    df = editable_table.data_view().reset_index()
    df_original = editable_table.data().reset_index()

    df_original["ID"] = pd.to_numeric(df_original["ID"])
    df["Flag"] = df["Flag"].astype("string")

    flag_inds = list(df[df["Flag"] == points.trace_name].index)
    df_inds = [flag_inds[i] for i in point_inds if i < len(flag_inds)]
    id = df.loc[df_inds, "ID"].values[0]
    return df_original[df_original["ID"] == id].index.values.astype(int)[0].item()