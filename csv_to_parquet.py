from pathlib import Path
import argparse
import polars as pl
from tqdm import tqdm

def main():
    '''Converting all csv files to parquet. Uses Polars.'''
    parser = argparse.ArgumentParser()
    # Required argument: path
    parser.add_argument("folder", help='Path to folder', type=str)
    args = parser.parse_args()
    folder = Path(args.folder)
    # Get all .csv and .parquet.gzip files
    files= list(folder.rglob('*'))
    num_converted = 0
    for file in tqdm(files):
        new_file = file.with_suffix('.parquet.gzip')
        if file.suffix == '.csv':
            exists = new_file in files
            if exists:
                continue
            # Read .csv and convert to .parquet.gzip
            # Try reading one column first, all as strings
            df = pl.read_csv(file, infer_schema_length=0, n_rows=1) # str
            if len(df.columns) == 1:
                # Wrong separator, usually $$$
                df = pl.read_csv(file, separator=r'$', infer_schema_length=0)
                df = df.select(pl.col([i for i in df.columns if not (i.startswith('_')) | (len(i) == 0)]))
            else:
                df = pl.read_csv(file, infer_schema_length=0)
            # Export as parquet
            df.write_parquet(new_file, compression='gzip')
            num_converted += 1

    print('Number of files converted: ', num_converted) 

if __name__ == "__main__":
    main()