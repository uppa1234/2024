import polars as pl

# General functions

def scan_file(path:str) -> pl.LazyFrame:
    '''
    Lazily scans a .parquet(.gzip), .csv or .txt file with a delimiter of , or $$$ with all columns as str
    Assumes no single $ in text.
    '''
    # If parquet, read it immediately
    if path.suffixes[0] == '.parquet':
        return pl.scan_parquet(path)

    # If .csv/.txt:
    # Reads the first line to check the delimiter
    with open(path) as f:
        first_line = f.readline()

    # Read $$$
    if first_line.count('$$$') > 0:
        # Polars can only read delimiters of one character
        lf = pl.scan_csv(path, separator='$', infer_schema_length=0)
        # Drop all 2nd and 3rd characters
        return lf.select(pl.col([i for i in lf.columns if not (i.startswith('_')) | (len(i) == 0)]))

    # Read ,
    return pl.scan_csv(path, separator=',', infer_schema_length=0)

def parse_dates(lf:pl.LazyFrame, date_col: str='D001KEY', format: str='%Y%m%d') -> pl.LazyFrame:
    '''Parses a single date column in a DataFrame/LazyFrame.'''
    dtype = lf.select(pl.col(date_col)).dtypes[0]

    # Already good if type is Date
    if dtype == pl.Date:
        return lf

    # Change Datetime to just Date
    if lf.select(date_col).dtypes[0] == pl.Datetime:
        return lf.with_columns(pl.col(date_col).dt.date())
    
    # Change str to Date
    if lf.select(date_col).dtypes[0] == pl.Utf8:
        return lf.with_columns(pl.col(date_col).str.strptime(pl.Datetime, format=format).dt.date())
    
    # Other types not accepted
    raise TypeError('Wrong dtype.')

def clip_dates(lf:pl.LazyFrame, date_col: str, start_year: int = None, start_month: int = 1, start_day: int = 1, end_year: int = None, end_month: int = 12, end_day: int = 31) -> pl.LazyFrame:
    '''Clip dates in a single pl.Date column in a DataFrame/LazyFrame.'''
    assert lf.select(pl.col(date_col)).dtypes[0] == pl.Date
    if start_year is not None:
        lf = lf.filter(pl.col(date_col) >= pl.date(year=start_year, month=start_month, day=start_day))
    if end_year is not None:
        lf = lf.filter(pl.col(date_col) <= pl.date(year=end_year, month=end_month, day=end_day))
    return lf