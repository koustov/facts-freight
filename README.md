<!-- TOC ignore:true -->
# Facts Freight

<!-- TOC -->
- [Facts Freight](#facts-freight)
- [Idea](#idea)
  - [Features](#features)
- [Design](#design)
- [Supported Databases](#supported-databases)
- [Config Parameters](#config-parameters)
- [Table Config Parameters](#table-config-parameters)

# Idea

A simple ,dumb yet **powerful** data loader tool to transfer data from source to destination with features like

## Features

- Maintaining schema
- Basic sanitization
- Hooks for more flexibility
- Data validation
- Schema update validation
- Support all possible data base providers
- Compatible with cloud and on-prem
- Testability
- Parallel Processing for performance
- Versioning each pass

# Design

![alt text](_docs/design.jpg "The Design")

# Supported Databases

Out of the box this tool supports following databases, however the its not limited. One can write adapter for any database and use it.
| Type     | Source/Destination | Version |
| -------- | ------------------ | ------- |
| Aurora   | Source             |         |
| Postgres | Source             |         |
| Vertica  | Destination        |         |

# Config Parameters

A set of configuration parameters.

| Name                        | Type   | Mandatory? | Purpose                                                                                | Default |
| --------------------------- | ------ | ---------- | -------------------------------------------------------------------------------------- | ------- |
| source                      | string | yes        | Type of the source database                                                            |         |
| target                      | string | yes        | Type of the target database                                                            |         |
| bucket_size                 | number | no         | Bucket size for each transaction                                                       |         |
| object_prefix               | string | no         | Type of the target database                                                            | 10      |
| version                     | string | yes        | Unique version number. An updated value invokes schema validation                      |         |
| log.max_file_count          | number | no         | Maximum number of log file                                                             | 5       |
| hooks.pre_table_collection  | string | yes        | A python file path that will be executed `before` processing each table                |         |
| hooks.post_table_collection | string | yes        | A python file path that will be executed `after` processing each table                 |         |
| hooks.post_table_collection | string | yes        | A python file path that will be executed `before` collection starts                    |         |
| hooks.post_table_collection | string | yes        | A python file path that will be executed `after` collection finished                   |         |
| source_server               | string | yes        | Source server connection string consists of `server`,`port`,`user`,`database`,`schema` |         |
| target_server               | string | yes        | Source server connection string consists of `server`,`port`,`user`,`database`,`schema` |         |


# Table Config Parameters

A set of parameters to identify list of tables that needs to be collected.

| Name         | Type        | Purpose                                            | Default |
| ------------ | ----------- | -------------------------------------------------- | ------- |
| name         | string      | The table name                                     |         |
| type         | string      | Type of the object. `t` for table and `v` for view |         |
| columns      | string:list | A list of columns to be collected                  | all     |
| uniqucolumns | string:list | A column set that defines uniqueness of a row      | none    |