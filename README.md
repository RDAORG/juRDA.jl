# juRDA.jl: user api for the Reference Data Archive (RDA)

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://y-chu.github.io/juRDA.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://y-chu.github.io/juRDA.jl/dev/)

Welcome to the **juRDA** module! This package provides user-friendly functions for working with structured Reference Data Archive (RDA) data in Julia. 

# Functions

These functions return a data frame, with parameters that either allow the user to filter the results by additional fields (in the database tables) or to supplement the data frame with additional information.

* `rda_sources()`: returns a data frame with the ID and name of the available data sources in the RDA

* `rda_countries()`: returns a data frame with the country name, country ISO3 code, and the corresponding data source name and ID
  + *arguments*: `source_name` & `source_id` (all are strings) filter the results to only return countries for a particular source

* `rda_sites()`: returns a data frame with the sites (i.e., geographic locations) with data in the RDA, along with the corresponding country and source information
  + *arguments*: `source_name`, `source_id`, `country_name`, & `country_iso3` (all are strings) filter the results by source and/or country

* `rda_deaths()`: returns a data frame containing the death records for each data source in the RDA
  + *arguments*: `source_name`, `source_id`, `site_name`, `site_id`, `country_name`, & `country_iso3` (all are strings) filter the results by source, site, and/or country

* `rda_datasets`: returns a data frame with basic metadata for the available data sets in the RDA, which includes the name, ID, description, and the corresponding
unit of analysis
  + *arguments*: `doi` & `repo_id` (both are logical, i.e., true/false) supplement the data frame with the DOI and repository ID from NADA

* `rda_data_dict`: returns a data frame with the data dictionary for a given data set identified by the `dataset_id` argument (numeric) as given by the `rda_datasets()` output

* `rda_data`: returns a data frame with the actual data from a particular data set identified with either the `dataset_id` argument (numeric) or the `dataset_name` argument (string)

* `rda_schema`: returns a data frame containing the tables (with descriptions) that are available in the RDA.
  + *arguments*: `fields` (logical, i.e.,true/false) supplements the function output with the field names contained in each RDA table

* `rda_table`: returns a data frame containing selected table identified with `table_name` argument (string) 

We also have packages for [R](https://github.com/RDAORG/rRDA) and [Python](https://github.com/RDAORG/pyRDA).
