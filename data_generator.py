import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from os import getcwd
from pathlib import Path


def pandas_export(data: dict, output_path: Path, N: int) -> None:
    df = pd.DataFrame(data=data)
    df.to_parquet(path = output_path.joinpath(f'array_pandas_{N}.parquet'), engine='pyarrow', compression='snappy', )


def polars_export(data: dict, output_path: Path, N: int) -> None:
    df = pl.DataFrame(data=data)
    df.write_parquet(file=output_path.joinpath(f'array_polars_{N}.parquet'),compression='snappy', statistics=True)

def pyarrow_export(data: dict, output_path: Path, N: int) -> None:
    table = pa.Table.from_pydict(data)
    pq.write_table(table,output_path.joinpath(f'array_pyarrow_{N}.parquet'), compression='snappy')


if __name__ == "__main__":
    
    output_path = Path(getcwd())

    NNs = [50_000, 500_000] #,1_000_000, 5_000_000, 10_000_000]

    for N in NNs:
        print(N)

        t = np.linspace(0,10,N)
        x1 = np.sin(t)
        x2 = np.cos(t)
        x3 = x1*x2
        x4 = np.random.randint(0, 100, size=N)

        data = {'time':t, 'x1':x1, 'x2':x2, 'x3': x3,'x4':x4 }

        pandas_export(data=data, output_path=output_path, N=N)
        polars_export(data=data, output_path=output_path, N=N)
        pyarrow_export(data=data, output_path=output_path, N=N)