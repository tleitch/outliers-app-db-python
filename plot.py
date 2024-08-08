import plotly.express as px
import pandas as pd

def plot_ozone(x, y, ozone, outliers):
    ozone["Flag"] = -1
    combined = pd.concat([ozone, outliers])
    combined["Flag"] = combined["Flag"].astype('category')
    combined["Date"] = combined["Date"].astype("string")
    combined = combined[[x, y, "Flag"]]

    # Create the base plot for ozone data
    # fig = px.scatter(ozone, x=input['plot_x'], y=input['plot_y'], opacity=0.4)
    fig = px.scatter(
        combined, 
        x=x, 
        y=y, 
        color="Flag", 
        opacity=0.4, 
        color_discrete_map={0: 'blue', 1: 'red', -1: 'black'},
        hover_data={"Flag": False}
    )
    
    fig.update_layout(
        # xaxis_title=input['plot_x'],
        # yaxis_title=input['plot_y'],
        template='plotly_white',
        showlegend=False
    )

    return fig
