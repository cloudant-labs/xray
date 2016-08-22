# xray

Tool to investigate Cloudant/CouchDB cluster usage.


# Installation

If you don't use `pipsi`, you're missing out.
Here are [installation instructions](https://github.com/mitsuhiko/pipsi#readme).

Simply run:

    $ pipsi install .


# Usage

To use it:

    $ xray --help


# Examples

### Show database specific command help
xray 'https://foo:pass@foo.cloudant.com' databases --help

### Get top 60 databases, ordered by doc count
xray 'https://foo:pass@foo.cloudant.com' databases --limit 60

### Get all databases, ordered by doc count, show current/recommended sharding values
xray 'https://foo:pass@foo.cloudant.com' databases --limit 0 -s

### Get all databases, ordered by doc count, show defined indexes
xray 'https://foo:pass@foo.cloudant.com' databases --limit 0 --dd
