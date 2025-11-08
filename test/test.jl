using ConfigEnv
using RDAIngest
using SQLite
using DBInterface
using DataFrames
using Dates

# using Pkg
Pkg.develop(PackageSpec(path="../../juRDA.jl"))

using juRDA


include("../src/env.jl") 

@info "Load rda db"

db = load_rda()

db = load_rda("../../RDA/Database/RDA.sqlite")

rda_env.db_path = "../../RDA/Database"
_load_initial_tables()
db = rda_env.db_conn

@info "Take a look at RDA schema"
rda_schema()
rda_schema(fields=true)

@info "Get pre-loaded tables"

rda_sources()

@info "Get countries and sites for source"

rda_countries()
rda_countries(source_name = "CHAMPS")
rda_countries(source_id = 2)

rda_sites(source_name="HEALSL")


@info "Get available datasets"
rda_datasets()
rda_datasets(source_name = "CHAMPS")


@info "Fetch all deaths for source or site"

all_deaths = rda_deaths()
combine(groupby(all_deaths, :source_name), nrow => :count)
size(all_deaths)[1]

champs_deaths = rda_deaths(source_name="CHAMPS")

@info "Dataset as dataframe"

va = dataset_to_dataframe(db,5) 
decode = rda_data(dataset_name = "CHAMPS_deid_decode_results")

@info "Get data dictionary"
rda_data_dict()
rda_data_dict("CHAMPS_deid_decode_results")
rda_data_dict("CHAMPS_deid_verbal_autopsy")

