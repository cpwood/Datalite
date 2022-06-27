# Datalite ![](https://raw.githubusercontent.com/cpwood/Datalite/develop/datalite-logo-64.png)
A plugin-based Sqlite library that allows you to aggregate data from many different sources with minimal code.

## What you can use it for

1. Gathering data from different sources together into one database for further processing or analysis.
2. Moving data from simpler storage that doesn't have richer querying capabilities for analysis - e.g. from key-value storage.
3. Moving data from database products with lesser or non-standard querying capabilities or syntaxes (e.g. Microsoft Access).
4. Moving data from different platforms without having to create intermediary models for Object Relational Mappers.
5. Scenarios where a SQL-based approach is preferred to analyse data held in memory.
6. Automation of data conversion tasks that might currently be handled by people manually.
7. Repeatable, simple database pipeline jobs that can be stored and versioned in source control.
8. To create an output database that can then be used as an input to another more basic "lift and shift" Big Data pipeline.

## Why Sqlite?

Sqlite has been chosen as the database platform into which the data is aggregated because:

1. It has a small footprint;
2. It has rich T-SQL querying capabilities with a syntax that feels comfortable to users of most other RDBMSs;
3. Data can be stored either on disk or in memory;
4. It stores data using storage classes rather than more familiar RDBMS column specifications  which reduces the amount of type conversion and discovery that takes place (e.g. it stores text as `TEXT` rather than `nvarchar(150)`).
5. It is cross-platform - the output of this library's operations can be used in other programming languages in other Operating Systems without the need to license commercial products.

Datalite is not designed to replace pipelines for Data Lakes, Data Warehouses, etc. However, Big Data pipelines are often a sledgehammer to crack a nut when all a developer often needs is a reliable nutcracker!

The origin of this library is from a work-related project where I needed to gather data about message structures and valid enumeration values from multiple H2 databases, a Microsoft Access database, JSON from an API and a CSV file. 

It was the sheer amount of plumbing code required, together with the time taken to gain a working understanding of the quirks of each SQL dialect (especially where Access is concerned) that made me realise that there was a need for a better solution.

## Getting Started

Install one or more of the Datalite packages from Nuget:

* Datalite.Sources.Databases.**AzureTables** - [Guidance](/)
* Datalite.Sources.Databases.**CosmosDb** - [Guidance](/)
* Datalite.Sources.Databases.**H2** - [Guidance](/)
* Datalite.Sources.Databases.**Odbc** - [Guidance](/)
* Datalite.Sources.Databases.**Postgres** - [Guidance](/)
* Datalite.Sources.Databases.**Sqlite** - [Guidance](#)
* Datalite.Sources.Databases.**SqlServer** - [Guidance](#)
* Datalite.Sources.Files.**Csv** - [Guidance](/)
* Datalite.Sources.Files.**Json** - [Guidance](/)
* Datalite.Sources.Files.**Parquet** - [Guidance](/)
* Datalite.Sources.**Objects** - [Guidance](/)

Follow the guidance for the plugin to get started.

More plugins to follow in the future!

## Create a plugin

Follow [this guidance](#) on how to contribute your own plugin!
