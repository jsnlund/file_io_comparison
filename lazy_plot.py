
import polars as pl
import plotly.graph_objects as go
from datetime import datetime

if __name__ == "__main__":
    # need to generate filewith other script first.
    file = r'Path\to\array_polars_500000.parquet'

    lf = (
        pl.scan_parquet(file)
        .with_columns(pl.col('*'))
    )

    lfe = (
        pl.concat(
            [lf.with_columns(
                pl.col('x1').alias('x5')
            ),
             lf.with_columns(
                 [
                    (pl.col('time') + pl.col('time').last()),
                    ((pl.col('x3')*-1) + pl.col('x1').last()).alias('x5')
                 ]
             )])
        .select(['time', 'x5'])
    )

    df = lfe.collect()

    # df = pl.DataFrame(
    #     {
    #         "integer": [1, 2, 3, 4, 5],
    #         "date": [
    #             datetime(2022, 1, 1),
    #             datetime(2022, 1, 2),
    #             datetime(2022, 1, 3),
    #             datetime(2022, 1, 4),
    #             datetime(2022, 1, 5),
    #         ],
    #         "float": [4.0, 5.0, 6.0, 7.0, 8.0],
    #     }
    # )

    print(df.head(5))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['time'],
        y=df['x5'],
        name = 'x5',
        ),
    )

    
    fig.show()
