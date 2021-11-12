<!-- TOC ignore:true -->
# Facts Freight

<!-- TOC -->
- [Facts Freight](#facts-freight)
- [Idea](#idea)
  - [Features](#features)
- [Design](#design)
- [Supported Databases](#supported-databases)

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