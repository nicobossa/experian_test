# Spark Data Processing with Scala and Maven
## General description
*This repository is used for deployment processing for Experian Test:*

This project is designed to process and analyze data from two CSV files. It uses Apache Spark for data processing, allowing for efficient handling of large datasets. The project includes functionality to read the input data, perform transformations, and write the results to an output directory. The configuration for input and output paths, along with other parameters, can be easily managed through a configuration file.


## Configutarion files descrition
config.conf: Main module to insert data into final table.


## Getting started with the new project
### First Compilation

Make a clean install in the root directory of the project

```bash
mvn clean install
```

### First Execution configuration

Go to your IDE run configurations window and set the following configuration:
 * Main class: `src.main.scala.ExperianTest.MainLauncher`
 * Program arguments should be a valid path to a configuration file: `ExperianTest/src/main/resources/config/config.conf`
