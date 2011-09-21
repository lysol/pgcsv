pgcsv is both a Python module and command-line utility for loading delimited
data from text files into database tables.

Installation
============

* Install pyscopg2 (Should be available in Ubuntu as python-psycopg2)
* Download the latest [tarball](https://github.com/lysol/pgcsv/tarball/master) or clone this repository
* Extract wherever, cd to the directory.
* sudo python setup.py install
* That's it

Usage
=====

You can execute `pgcsv --help` for a list of all options. Basic usage:

    pgcsv -S schema -t table -f csvfile -p connstring

Where `connstring` is a libpq-style [connection string](http://www.postgresql.org/docs/9.1/static/libpq-connect.html).

By default, pgcsv will do the following:

* Strip non-alphanumeric characters from the field names, replacing them with underscores
* Lowercase field names
* Remove trailing whitespace from field values
* Attempt to detect field types based on some clumsy metrics (defaults to varchar)
* Use comma delimiters and double quote quoting
* Display progress after every 5MB of data loaded.
* Use the public schema
* Drop the table first if it already exists

Notes
=====

You can execute either via the `pgcsv` script that is installed to your system PATH,
or via `python -m pgcsv`.
