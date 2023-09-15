# File IO Comparison

## Reading & Writing

Filetype: parquet

- DuckDB
- Polars DataFrame
- Polars (pyarrow engine)
- Polars LazyFrame

## Polars Misc Sandbox

Plus, finally figuring out why LazyFrames are Fucking awesome!
Do all the chained calculations without calculations and let LazyFrame optimize the query for you.

Also, vscode terminal is STUPID.  Kept throwing an encoding error whenever I tried to print polars dataframe stuff (eg. `print(df.head(3))`).  Thought I was messing up the polars commands. Finally ran it on a normal terminal and itworked fine.  Had to add the following to the vscode settings.json so that it would run print out the polars stuff.

    ```json
    "code-runner.executorMap": {
        "python": "set PYTHONIOENCODING=utf8 && python"
    }`
    ```

Lesson learned... always use a REAL terminal.
