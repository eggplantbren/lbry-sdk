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

# The graph to be given to the app
graph = dcc.Graph(id="num_streams")

app.layout = html.Div(style={"background-color": "#222222",
                             "color": "#EEEEEE", "width": 1400},
                      children=[html.H1(children="Data Page"),
                                html.P(children="This is just a test!"),
                                graph, dcc.Interval(
                                   id='interval-component',
                                   interval=60*1000, # in milliseconds
                                   n_intervals=0
                                  )])


# Multiple components can update everytime interval gets fired.
@app.callback(Output('num_streams', 'figure'),
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
    for row in db.execute("SELECT * FROM hour;"):
        ts.append(row[0])
        up.append(row[1])
        down.append(row[2])
        announce.append(row[3])

    fig = make_subplots(rows=2, cols=1)
    fig.update_layout(
                        {
                            "title": "Blobs Seeded",
                            "plot_bgcolor":  "#222222",
                            "paper_bgcolor": "#222222",
                            "xaxis": {"title": "Time"},
                            "yaxis": {"title": "Blobs"},
                            "font": {"color": "#EEEEEE"},
                            "width": 1400, "height": 1400
                        }
                     )

    bar = go.Bar(x=ts, y=up, name="Blobs Uploaded")
    fig.add_trace(bar, row=1, col=1)

    return fig


if __name__ == '__main__':
    app.run_server(host="127.0.0.1")

