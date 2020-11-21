# -*- coding: utf-8 -*-
import apsw
import dash
import dash_core_components as dcc
import dash_html_components as html
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from dash.dependencies import Input, Output
from data_stats import DB_FILENAME, DATA_STATS_ENABLED

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# Create the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# The graphs to be given to the app
graph = dcc.Graph(id="blob_graphs")

app.layout = html.Div(style={"background-color": "#222222",
                             "color": "#EEEEEE", "width": 1200},
                      children=[html.H1("Data Page"),
                                graph, dcc.Interval(
                                   id='interval-component',
                                   interval=60*1000, # in milliseconds
                                   n_intervals=0
                                  )])


# Multiple components can update everytime interval gets fired.
@app.callback([Output("blob_graphs", "figure")],
              [Input('interval-component', 'n_intervals')])
def update_data(n):

    # Get recent data from database
    conn = apsw.Connection(DB_FILENAME, flags=apsw.SQLITE_OPEN_READONLY)
    db = conn.cursor()
    ts = []
    down = []
    up = []
    announce = []
    now = time.time()
    for row in db.execute("SELECT * FROM hour WHERE start_time >= ?;",
                          (time.time() - 3*86400.0, )):
        ts.append(row[0])
        up.append(row[1])
        down.append(row[2])
        announce.append(row[3])

    for i in range(len(ts)):
        ts[i] = datetime.datetime.fromtimestamp(ts[i])

    fig = go.Figure()
    fig.update_layout(
                        {
                            "title": "LBRY Data Network Information",
                            "plot_bgcolor":  "#222222",
                            "paper_bgcolor": "#222222",
                            "xaxis": {"title": "Time"},
                            "yaxis": {"title": "Blobs"},
                            "font": {"color": "#EEEEEE"},
                            "width": 1200, "height": 800
                        }
                     )

    bar1 = go.Bar(x=ts, y=down, name="Downloaded", marker_color="red")
    bar2 = go.Bar(x=ts, y=up, name="Uploaded", marker_color="rgb(100, 255, 100)")
    bar3 = go.Bar(x=ts, y=announce, name="Announced", marker_color="rgb(100, 100, 255)")
    fig.add_trace(bar1)
    fig.add_trace(bar2)
    fig.add_trace(bar3)
    fig.update_layout(barmode="group")

    return [fig]


if __name__ == '__main__':
    app.run_server(host="127.0.0.1")

