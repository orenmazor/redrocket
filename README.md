# RedRocket is a diagnostic tool for redshift

It doesn't do anything you can't already do with sql, once you have a console logged in an open.

# Usage

This tool accepts any environment configuraiton that libpq accepts. For more info, see: http://www.postgresql.org/docs/9.3/static/libpq-envars.html

# Reports

```
redrocket -h

  -diskbased=false: report on queries that went to disk
  -inflight=false: report on currently running queries
  -seq-scans=false: report on pg seq scans
  -time-consuming=false: report on most time consuming queries
  -data-dist=false: report on data distribution
  -query-queues=false: report on service query queues
  -queued-queries=false: report on queries that spend their life in queue

```
