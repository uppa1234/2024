# 2024

resize_dicom.py - command line program to convert all .dicom/.dcm files in a folder to .png/.jpg and save them into **a new** folder
~~~
!python resize_dicom.py [folder path] --size [size: default=224] --output [output extension: default='.png']
~~~

resize_jpg_png.py - command line program to resize all .png/.jpg images and save them into **a new** folder
~~~
!python resize_jpg_png.py [folder path] --size [size: default=224] --output [output extension: default='.png']
~~~

csv_to_parquet.py - command line program to convert all .csv files in a folder to .parquet.gzip and save them into **the same** folder.
Requires [Polars](https://pypi.org/project/polars/).
~~~
!python csv_to_parquet.py [folder path]
~~~
