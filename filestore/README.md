# FileStore

This isn't a database binding.
This binding can be used to export the data that will be written, via the insert method call, into files.
These files can later be used to equally test different real databases with the same data for consistency reasons.

## Output location

The standard output location will be `{projectDir}/benchmarkingData/`

You can specify your own output directory by passing it over the `-p` parameter with the key `outputDirectory`.

```
bin/ycsb.sh load filestore -P workloads/workloada -p outputDirectory=/path/to/data
bin/ycsb.sh run filestore -P workloads/workloada -p outputDirectory=/path/to/data
```