# import pandas as pd
import polars as pl
import duckdb
# import pyarrow as pa

from pathlib import Path
import timeit

# TODO: add read functions and a basic calculation for each of the tools

# def duck(file: str) -> None:
#     df = duckdb.sql(
#                     f"""
#                     SELECT median('time')
#                     FROM read_parquet({file})
#                     """
#                     ).pl()
#     print('QUACK!')

def duck(file: str) -> None:
    df = duckdb.sql(
                    f"""
                    SELECT median('time')
                    FROM {file}
                    """
                    ).pl()
    # print('QUACK!')
    # print(med)

def bear(file:str) -> None:
    df = pl.read_parquet(source=file, columns=['time'], rechunk=False)
    # df = pl.read_csv(source=file, has_header=True, rechunk=False)
    med = df.median()
    # print('ROAR!')
    # print(med)
    # print(type(med))

def arrow_bear(file:str) -> None:
    df = pl.read_parquet(source=file, columns=['time'], use_pyarrow=True, rechunk=False)
    # df = pl.read_csv(source=file, has_header=True, use_pyarrow=True, rechunk=False)
    med = df.median()
    # print('SWOOSH!')
    # print(med)

def lazy_bear(file:str) -> None:
    lf = pl.scan_parquet(source=file, rechunk=False)
    # lf = pl.scan_csv(source=file, has_header=True, rechunk=False)
    med = lf.select('time').median().collect()
    # print('YAWN!')
    # print(med)
    # print(type(med))

if __name__ == "__main__":
    file = 'array_polars_500000.parquet'
    # file = 'array_polars_10000000.csv'

    # df = bear(file=file)
    # df.write_csv(Path(file).with_suffix('.csv'))

    N = 1000
    # duck_time = timeit.timeit(lambda: duck(file=file), setup='pass', number= N)/N
    lazy_time = timeit.timeit(lambda: lazy_bear(file=file), setup='pass', number= N)
    bear_time = timeit.timeit(lambda: bear(file=file), setup='pass', number= N)
    # arrow_time = timeit.timeit(lambda: arrow_bear(file=file), setup='pass', number= N)/N
    # print(f'Duck: {duck_time: .2f}')
    print(f'Lazy Bear: {lazy_time: .2f}')
    print(f'Bear: {bear_time: .2f}')
    # print(f'Arrow Bear: {arrow_time: .2f}')
