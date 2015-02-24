# Commandline tool to split csv
[![NPM](https://nodei.co/npm/csv-split.png)](https://nodei.co/npm/csv-split/)

This small tool spawn off from our need during the Nigeria MDGs Info System data mopup process, we needed to process millions of lines of csv file with a constraint of memory, and a good way to go was to split the csv based on one column and have each be processed separately in R. 

We used streams to pick up one line at a time and dump the result to the output directory.

To use, do

> csv-split -i [file_name] -b [group_by_column] [-o [output_directory]]

if `output_directory` is not specified, it will default to `[file_name]_by_[group]`

you can also pipe from stdin:

> cat data.csv | csv-split -b [group_by_column]

```
  Usage: csv-splitter [options]

  Options:

    -h, --help                output usage information
    -V, --version             output the version number
    -i, --input <file>        select an input csv
    -o, --output <directory>  select an output directory
    -b, --groupby <group>     the column you want to group by
```
