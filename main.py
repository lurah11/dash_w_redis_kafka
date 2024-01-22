from dash import Dash, html, dcc, callback, Output, Input, State
import plotly.express as px
import pandas as pd
import redis 
import json

redis_client = redis.StrictRedis(decode_responses=True)

def get_data_from_redis():
    object_list = []
    keys = [key for key in redis_client.zrange('item_order', 0, -1)]
    for key in keys:
        data_from_redis = redis_client.get(key)
        object_list.append(json.loads(data_from_redis))
    df = pd.DataFrame(object_list)
    return df 

app = Dash(__name__)

app.layout = html.Div([
    html.H1(children='Title of Dash App', style={'textAlign': 'center'}),
    dcc.Graph(id='graph-map'),
    dcc.Graph(id='graph-line'),
    dcc.Interval(
        id='interval-component',
        interval=1 * 400,  # in milliseconds
        n_intervals=0
    ),
])

@callback(
    [Output('graph-map', 'figure'),
     Output('graph-line', 'figure')],
    [Input('interval-component', 'n_intervals')],
    [State('graph-map', 'relayoutData')]
)
def update_graph(n, relayout_data):
    df = get_data_from_redis()

    if relayout_data is None:
        relayout_data = {}

    # Get the last stored zoom and center properties
    last_zoom = float(relayout_data.get('mapbox.zoom', 0))
    print(f"last-zoom={last_zoom}")
    last_center_lat = float(relayout_data.get('mapbox.center.lat', df['latitude'].mean()))
    print(f"last_center_lat={last_center_lat}")
    last_center_lon = float(relayout_data.get('mapbox.center.lon', df['longitude'].mean()))
    print(f"last_center_lon={last_center_lon}")
    # Create map figure with last zoom and center properties
    map_figure = px.scatter_mapbox(df, lat='latitude', lon='longitude', hover_data=['date', 'magnitude'],
                                   size="magnitude", zoom=last_zoom,
                                   center=dict(lat=last_center_lat, lon=last_center_lon),
                                   color='magnitude')
    map_figure.update_layout(mapbox_style="open-street-map")

    # Create line figure
    line_figure = px.line(df[-100:-1], x='date', y='magnitude')
    return map_figure, line_figure

if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0',port=8080)
