# xray

Tool to investigate Cloudant/CouchDB cluster usage.


# Installation

If you don't use `pipsi`, you're missing out.
Here are [installation instructions](https://github.com/mitsuhiko/pipsi#readme).

Simply run:

    $ pipsi install .


# Usage

To use it:

    $ xray <URL> <command> --help


# Commands

## Databases

Provides a database-centric view of an account.

### Examples

*Show command help*
xray 'https://foo:pass@foo.cloudant.com' databases --help

*Get top 60 databases, ordered by doc count*
xray 'https://foo:pass@foo.cloudant.com' databases --limit 60

*Get all databases, ordered by doc count, show current/recommended sharding values*
xray 'https://foo:pass@foo.cloudant.com' databases --limit 0 -s

*Get all databases, ordered by doc count, show defined indexes*
xray 'https://foo:pass@foo.cloudant.com' databases --limit 0 --dd

*Get all databases, ordered by doc count, show defined indexes, output to file.csv*
xray 'https://foo:pass@foo.cloudant.com' databases --limit 0 --dd --output file.csv


## Indexes

Provides an index-centric view of an account.

### Examples

*Show command help*
xray 'https://foo:pass@foo.cloudant.com' indexes --help

*Get top 60 indexes, ordered by database/_design doc*
xray 'https://foo:pass@foo.cloudant.com' indexes --limit 60

*Get top 60 indexes for a specific database doc*
xray 'https://foo:pass@foo.cloudant.com/mydb' indexes --limit 60

*Get all indexes, ordered by database/_design doc output to file.csv*
xray 'https://foo:pass@foo.cloudant.com' indexes --limit 0 --output file.csv

