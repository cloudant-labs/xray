# xray

Tool to investigate Cloudant/CouchDB cluster usage.


# Installation

If you don't use `pipsi`, you're missing out.
Here are [installation instructions](https://github.com/mitsuhiko/pipsi#readme).

Simply run:

    $ pipsi install .


# Usage

To use it:

    $ xray --url <url> <command> --help


# Commands

## Databases

Provides a database-centric view of an account.

### Examples

**Show command help**
```
xray --url 'https://foo:pass@foo.cloudant.com' databases --help
```

**Get top 60 databases, ordered by doc count**
```
xray --url 'https://foo:pass@foo.cloudant.com' databases --limit 60
```

**Get all databases, ordered by doc count, show current/recommended sharding values**
```
xray --url 'https://foo:pass@foo.cloudant.com' databases --limit 0 -s
```

**Get all databases, ordered by doc count, show defined indexes**
```
xray --url 'https://foo:pass@foo.cloudant.com' databases --limit 0 --dd
```

**Get all databases, ordered by doc count, show defined indexes, output to file.csv**
```
xray --url 'https://foo:pass@foo.cloudant.com' databases --limit 0 --dd --output file.csv
```

**Get top 60 databases across multiple urls**
```
xray --source urls.csv databases --limit 60
```

In this case, urls is a file with one URL per line

## Indexes

Provides an index-centric view of an account.

### Examples

**Show command help**
```
xray --url 'https://foo:pass@foo.cloudant.com' indexes --help
```

**Get top 60 indexes, ordered by database/_design doc**
```
xray --url 'https://foo:pass@foo.cloudant.com' indexes --limit 60
```

**Get top 60 indexes for a specific database doc**
```
xray --url 'https://foo:pass@foo.cloudant.com/mydb' indexes --limit 60
```

**Get all indexes, ordered by database/_design doc output as csv**
```
xray --url 'https://foo:pass@foo.cloudant.com' indexes --limit 0 --format csv
```

**Get all indexes, ordered by database/_design doc output as json**
```
xray --url 'https://foo:pass@foo.cloudant.com' indexes --limit 0 --format json
```

