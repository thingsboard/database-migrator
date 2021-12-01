# Prerequisites

**IMPORTANT! Requires Java 8 SDK**

Please make sure you have JDK 8 installed:

```
ubuntu@pc:~$ java -version
openjdk version "1.8.0_292"
OpenJDK Runtime Environment (build 1.8.0_292-8u292-b10-0ubuntu1~20.04-b10)
OpenJDK 64-Bit Server VM (build 25.292-b10, mixed mode)
```

# Description
This tool used for migrating ThingsBoard setup from PostgreSQL only into Hybrid mode.

You can use this tool in two scenarios.

## Scenario #1
This is a scenario when rule engine requires historical data to work properly (e.g. you have rule nodes that are fetching historical data during processing new messages from devices). 
In this case system downtime requires for the migration.
 
## Scenario #2 
This is a scenario when rule engine DOES NOT require historical data to work properly. 
In this case system downtime not required (in case you are running cluster you can do this migration with zero downtime), or you'll need to restart ThingsBoard service in case of monolith setup.
You'll need to upgrade configuration of ThingsBoard to use Cassandra for timeseries instead PostgreSQL - old historical data will be added to Cassandra from PostgreSQL later. 
In this scenario historical data will not be available after the reconfiguration of ThingsBoard (for instance on dashboards), but you'll see after migration.  

# Performance

Performance of this tool depends on disk type and instance type (mostly on CPU resources).
Here are few benchmarks in general:
1. Creating Dump of the postgres ts_kv table (if it's size is 100 GB) ~ 1-2 hours
2. Generation SSTables from dump 100 GB ~ 3-4 hours
3. 100 GB dump file will be converted into SSTable with size about ~ 20-30 GB

**NOTE** Recommended instance type in AWS is m5.xlarge or larger. Avoid using instance with burst CPUs.

# Tool build instructions:
To build the project execute: 

```
mvn clean compile assembly:single
```
    
It will generate single jar file with all required dependencies inside `target` dir -> `database-migrator-1.0-SNAPSHOT-jar-with-dependencies.jar`.

# Prepare required files and run tool

## Scenario #1

### Cassandra setup

Install Cassandra - in this scenario you can install only single instance.
 
Using `cqlsh` create `thingsboard` keyspace and required tables.

**NOTE** You can use *schema-ts.cql* and *schema-ts-latest.cql* files, that are located in main/resources folder of this project.

### Dump data from the Postgres DB to files

**Do not use compression if possible because tool can only work with uncompressed files**

* Stop ThingsBoard instance
* Dump related tables (entities) that used to validate telemetry:

`pg_dump -h localhost -U postgres -d thingsboard --exclude-table=admin_settings --exclude-table=attribute_kv --exclude-table=audit_log --exclude-table=component_discriptor --exclude-table=device_credentials --exclude-table=event --exclude-table=oauth2_client_registration --exclude-table=oauth2_client_registration_info --exclude-table=oauth2_client_registration_template --exclude-table=relation --exclude-table=rule_node_state --exclude-table=tb_schema_settings --exclude-table=user_credentials --exclude-table=ts_kv* > /home/user/dump/related_entities.dmp`

* Dump `ts_kv_dictionary`:
   
`pg_dump -h localhost -U postgres -d thingsboard --table=ts_kv_dictionary > /home/user/dump/ts_kv_dictionary.dmp`
   
* Dump `ts_kv` and all partitions:

`pg_dump -h localhost -U postgres -d thingsboard --load-via-partition-root --data-only --table=ts_kv* > /home/user/dump/ts_kv_all.dmp`   

* [Optional] Move table dumps to the instance where cassandra will be hosted

### Prepare directory structure for SSTables
Tool use 3 different directories for saving SSTables - `ts_kv_cf`, `ts_kv_latest_cf`, `ts_kv_partitions_cf`.

Create 3 empty directories. For example:

```
/home/user/migration/ts
/home/user/migration/ts_latest
/home/user/migration/ts_partition
```
    
### Run tool

**IMPORTANT! If you run this tool on the remote instance - don't forget to execute this command in `screen` to avoid unexpected termination**

```
java -jar ./target/database-migrator-1.0-SNAPSHOT-jar-with-dependencies.jar 
        -telemetryFrom /home/user/dump/ts_kv_all.dmp 
        -relatedEntities /home/user/dump/related_entities.dmp 
        -dictionary /home/user/dump/ts_kv_dictionary.dmp
        -latestOut /home/user/migration/ts_latest 
        -tsOut /home/user/migration/ts 
        -partitionsOut /home/user/migration/ts_partition 
        -castEnable false
        -partitioning MONTHS
        -linesToSkip 0 > /tmp/migration.log &
```

*If you want to migrate just `ts_kv` without `ts_kv_latest`, execute next:*

```
java -jar ./target/database-migrator-1.0-SNAPSHOT-jar-with-dependencies.jar 
        -telemetryFrom /home/user/dump/ts_kv_all.dmp 
        -relatedEntities /home/user/dump/related_entities.dmp 
        -dictionary /home/user/dump/ts_kv_dictionary.dmp
        -tsOut /home/user/migration/ts 
        -partitionsOut /home/user/migration/ts_partition 
        -castEnable false
        -partitioning MONTHS
        -linesToSkip 0 > /tmp/migration.log &
```
  
*Use your paths for program arguments*

Tool execution time depends on DB size, CPU resources and disk throughput.

### Loading SSTables into Cassandra

**Note that this part works only for single node Cassandra cluster**

* Stop Cassandra
* Look at `/var/lib/cassandra/data/thingsboard` and check for names of data folders
* Copy generated SSTable files into cassandra data dir using next command:

```
sudo find /home/user/migration/ts -name '*.*' -exec mv {} /var/lib/cassandra/data/thingsboard/ts_kv_cf-0e9aaf00ee5511e9a5fa7d6f489ffd13/ \;
sudo find /home/user/migration/ts_latest -name '*.*' -exec mv {} /var/lib/cassandra/data/thingsboard/ts_kv_latest_cf-161449d0ee5511e9a5fa7d6f489ffd13/ \;
sudo find /home/user/migration/ts_partition -name '*.*' -exec mv {} /var/lib/cassandra/data/thingsboard/ts_kv_partitions_cf-12e8fa80ee5511e9a5fa7d6f489ffd13/ \;
```
  
*Pay attention! Data folders have similar name  `ts_kv_cf-0e9aaf00ee5511e9a5fa7d6f489ffd13`, but you have to use own*
  
* Start Cassandra service and trigger compaction
    * Trigger compactions: `nodetool compact thingsboard`
    * Check compaction status: `nodetool compactionstats`

* Switch ThingsBoard into Hybrid Mode

Modify ThingsBoard properties file `thingsboard.conf` and add next lines:
```
export DATABASE_TS_TYPE=cassandra
export TS_KV_PARTITIONING=MONTHS
export CASSANDRA_URL=YOUR_CASSANDRA_URL
export CASSANDRA_CLUSTER_NAME=YOUR_CASSANDRA_CLUSTER_NAME
export CASSANDRA_USE_CREDENTIALS=true # false if credentials not required 
export CASSANDRA_USERNAME=YOUR_CASSANDRA_USERNAME
export CASSANDRA_PASSWORD=YOUR_CASSANDRA_PASSWORD
```

### Final steps

Start ThingsBoard instance and verify migration.

## Scenario #2

### Cassandra setup

Install Cassandra - you can install single cluster, cluster of N nodes. Cluster can be in docker or k8s or bare metal.
 
Using `cqlsh` create `thingsboard` keyspace and required tables.

**NOTE** You can use *schema-ts.cql* and *schema-ts-latest.cql* files, that are located in main/resources folder of this project.

### Switch ThingsBoard into Hybrid Mode

Modify ThingsBoard properties file `thingsboard.conf` and add next lines:
```
export DATABASE_TS_TYPE=cassandra
export TS_KV_PARTITIONING=MONTHS
export CASSANDRA_URL=YOUR_CASSANDRA_URL
export CASSANDRA_CLUSTER_NAME=YOUR_CASSANDRA_CLUSTER_NAME
export CASSANDRA_USE_CREDENTIALS=true # false if credentials not required 
export CASSANDRA_USERNAME=YOUR_CASSANDRA_USERNAME
export CASSANDRA_PASSWORD=YOUR_CASSANDRA_PASSWORD
```

Re-start ThingsBoard and verify that new timeseries data written into Cassandra.

### Dump data from the Postgres DB to files

**Do not use compression if possible because tool can only work with uncompressed files**

* Dump related tables (entities) that used to validate telemetry:

`pg_dump -h localhost -U postgres -d thingsboard --exclude-table=admin_settings --exclude-table=attribute_kv --exclude-table=audit_log --exclude-table=component_discriptor --exclude-table=device_credentials --exclude-table=event --exclude-table=oauth2_client_registration --exclude-table=oauth2_client_registration_info --exclude-table=oauth2_client_registration_template --exclude-table=relation --exclude-table=rule_node_state --exclude-table=tb_schema_settings --exclude-table=user_credentials --exclude-table=ts_kv* > /home/user/dump/related_entities.dmp`

* Dump `ts_kv_dictionary`:
   
`pg_dump -h localhost -U postgres -d thingsboard --table=ts_kv_dictionary > /home/user/dump/ts_kv_dictionary.dmp`
   
* Dump `ts_kv` and all partitions:

`pg_dump -h localhost -U postgres -d thingsboard --load-via-partition-root --data-only --table=ts_kv* > /home/user/dump/ts_kv_all.dmp`   

* [Optional] Move table dumps to the instance where cassandra will be hosted

### Prepare directory structure for SSTables
Tool use 3 different directories for saving SSTables - `ts_kv_cf`, `ts_kv_latest_cf`, `ts_kv_partitions_cf`

Create 3 empty directories. 

**IMPORTANT** directories MUST follow pattern *.../KEYSPACE_NAME/COLUMN_FAMILY_NAME/*

For example:

```
/home/user/migration/thingsboard/ts_kv_cf/
/home/user/migration/thingsboard/ts_kv_partitions_cf/
/home/user/migration/thingsboard/ts_kv_latest_cf/
```

### Run tool

**IMPORTANT! If you run this tool on the remote instance - don't forget to execute this command in `screen` to avoid unexpected termination**

```
java -jar ./target/database-migrator-1.0-SNAPSHOT-jar-with-dependencies.jar 
        -telemetryFrom /home/user/dump/ts_kv_all.dmp 
        -relatedEntities /home/user/dump/related_entities.dmp 
        -dictionary /home/user/dump/ts_kv_dictionary.dmp
        -latestOut /home/user/migration/thingsboard/ts_kv_latest_cf
        -tsOut /home/user/migration/thingsboard/ts_kv_cf
        -partitionsOut /home/user/migration/thingsboard/ts_kv_partitions_cf
        -castEnable false
        -partitioning MONTHS
        -linesToSkip 0 > /tmp/migration.log &
```

*If you want to migrate just `ts_kv` without `ts_kv_latest`, execute next:*

```
java -jar ./target/database-migrator-1.0-SNAPSHOT-jar-with-dependencies.jar 
        -telemetryFrom /home/user/dump/ts_kv_all.dmp 
        -relatedEntities /home/user/dump/related_entities.dmp 
        -dictionary /home/user/dump/ts_kv_dictionary.dmp
        -tsOut /home/user/migration/thingsboard/ts_kv_cf
        -partitionsOut /home/user/migration/thingsboard/ts_kv_partitions_cf
        -castEnable false
        -partitioning MONTHS
        -linesToSkip 0 > /tmp/migration.log &
```
  
*Use your paths for program arguments*

Tool execution time depends on DB size, CPU resources and disk throughput.

### Loading SSTables into Cassandra

Using [sstableloader](https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/tools/toolsBulkloader.html) start loading data into Cassandra:

```
sstableloader --verbose --nodes CASSANDRA_NODES --username cassandra --password CASSANDRA_PASSWORD /home/user/migration/thingsboard/ts_kv_partitions_cf/

sstableloader --verbose --nodes CASSANDRA_NODES --username cassandra --password CASSANDRA_PASSWORD /home/user/migration/thingsboard/ts_kv_cf/
```

### Final steps

Verify that historical data available in ThingsBoard.

# Troubleshooting

## Continue migration in case of failure on particular migration line

**IMPORTANT: works only in case of Scenario #2**  

Tool is able to continue creation of SSTables from the particular line.
Let's image that tool has stopped at particular line - XXXXXXX:
```
2021-11-30 13:55:22,648 [main] INFO  o.t.c.t.m.writer.AbstractTbWriter - Lines processed 408000000, castOk 0, castErr 0, skippedLines 68956935
2021-11-30 13:55:25,625 [main] INFO  o.t.c.t.m.writer.AbstractTbWriter - Lines migrated 738000000, castOk 0, castErr 0, skippedLines 68966040
2021-11-30 13:55:32,037 [main] INFO  o.t.c.t.m.writer.AbstractTbWriter - Lines processed 409000000, castOk 0, castErr 0, skippedLines 68975104
2021-11-30 13:55:34,974 [main] INFO  o.t.c.t.m.writer.AbstractTbWriter - Lines migrated 739000000, castOk 0, castErr 0, skippedLines 68984191
2021-11-30 13:55:37,762 [main] INFO  o.t.c.t.m.writer.AbstractTbWriter - Lines processed 410000000, castOk 0, castErr 0, skippedLines 6899318
```

You'll need to find the latest entry of `Lines migrated XXXXXX`. You can start migration tool from XXXXXX line.

To use this feature please do next steps:
* Migrate already created SSTable to the Cassandra by following steps in the guide below for Scenario #2 
    *You can copy already created SSTables into some other folder, and later migrate with sstableloader folder by folder*
* Create new 3 different directories for saving SSTables, or reuse the ones used in the previous step 
```
/home/user/migration/thingsboard/ts_kv_cf/
/home/user/migration/thingsboard/ts_kv_partitions_cf/
/home/user/migration/thingsboard/ts_kv_latest_cf/
```
* Start tool according to instructions above, but with an additional parameter **linesToSkip**:

```
java -jar ./target/database-migrator-1.0-SNAPSHOT-jar-with-dependencies.jar 
        -telemetryFrom /home/user/dump/ts_kv_all.dmp 
        -relatedEntities /home/user/dump/related_entities.dmp 
        -dictionary /home/user/dump/ts_kv_dictionary.dmp
        -latestOut /home/user/migration/thingsboard/ts_kv_latest_cf
        -tsOut /home/user/migration/thingsboard/ts_kv_cf
        -partitionsOut /home/user/migration/thingsboard/ts_kv_partitions_cf
        -castEnable false
        -partitioning MONTHS
        -linesToSkip XXXXXX > /tmp/migration.log &
```

where XXXXXXX is the number of the line, that script should continue.

* Continue migration according to the steps above in Scenario #2