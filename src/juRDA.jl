"""
Main module for `juRDA.jl` -- a user API package for Reference Data Archive (RDA).
"""

module juRDA

using RDAIngest
using SQLite
using DBInterface
using DataFrames
using Logging

export load_rda, rda_sources, rda_countries, rda_sites, rda_deaths, rda_datasets, rda_data_dict, rda_data, rda_schema, rda_env, rda_table

include("env.jl")
#__init__()

"""
    load_rda(path=nothing)

Opens a connection to the SQLite database at `path`. Use default path if `path` is not specified
"""
function load_rda(path::Union{String, Nothing}=nothing)::SQLite.DB
    if path === nothing
        db = deepcopy(rda_env.db_conn)
    elseif !isfile(path)
        error("Database file not found at: $path")
    else 
        db = SQLite.DB(path)
    end
    return db
end

"""
    rda_sources()

Retrieve sources table as a DataFrame
"""
function rda_sources()
    return deepcopy(rda_env.sources)
end

"""
    rda_datasets(; doi=false, repo_id=false, source_name=nothing, source_id=nothing)

Retrieve available datasets. If doi = true and/or repo_id = true, then also return doi of dataset and/or repository id on RDA Data Repository. Optionally filter by `source_name` or `source_id`
"""
function rda_datasets(; doi=false, repo_id=false, 
                      source_name::Union{Nothing,String}=nothing, 
                      source_id::Union{Nothing,Int}=nothing)

    datasets = deepcopy(rda_env.datasets)
    uoa = rename(deepcopy(rda_env.unit_of_analysis_types), :name => :unit_of_analysis)
    datasets = leftjoin(datasets, uoa, on=:unit_of_analysis_id)
    rename!(datasets, :name => :dataset_name)

    cols = [:dataset_id, :dataset_name, :description, :unit_of_analysis]
    if doi
        push!(cols, :doi)
    end
    if repo_id
        push!(cols, :repository_id)
    end

    # Get source_id from source_name if provided
    if source_name !== nothing
        sources = deepcopy(rda_env.sources)
        matched = sources[sources.name .== source_name, :]
        if nrow(matched) == 0
            error("Data source not found. Call rda_sources() to list available source names with IDs.")
        end
        if source_id !== nothing
            @warn "Both `source_name` and `source_id` provided. Using `source_name`."
        end
        source_id = matched.source_id[1]  # Use first match
    end

    # Filter datasets by source_id if provided
    if source_id !== nothing
        if isnothing(rda_env.data_ingestions) || isnothing(rda_env.ingest_datasets)
            error("Required ingestion tables are not loaded.")
        end

        ingestions = unique(rda_env.data_ingestions[rda_env.data_ingestions.source_id .== source_id, :data_ingestion_id])
        if isempty(ingestions)
            @warn "No data ingestions found for source_id = $source_id"
            return DataFrame()  # Return empty result
        end

        filtered_id = unique(rda_env.ingest_datasets[in.(rda_env.ingest_datasets.data_ingestion_id, Ref(ingestions)), :dataset_id])
        datasets = datasets[in.(datasets.dataset_id, Ref(filtered_id)), :]
    end

    return datasets[:, cols]
end


"""
    rda_countries(; source_name=nothing, source_id=nothing)

Return countries, optionally filter by `source_name` or `source_id`
"""
function rda_countries(; source_name::Union{Nothing,String}=nothing, source_id::Union{Nothing,Int}=nothing)
    sources = deepcopy(rda_env.sources)
    countries = unique(select(rda_env.sites, [:country_name, :country_iso3, :source_id]))

    if source_name !== nothing
        sources = sources[sources.name .== source_name, :]
        if nrow(sources) == 0
            error("Data source not found. Call rda_sources() to list available source names with id.")
        end
        if source_id !== nothing
            @warn "Detected inputs for both source name and id, output based on source name."
        end
    elseif source_id !== nothing
        sources = sources[sources.source_id .== source_id, :]
        if nrow(sources) == 0
            error("Data source not found. Call rda_sources() to list available source names with id.")
        end
    end

    rename!(sources, :name => :source_name)
    countries = innerjoin(countries, sources, on=:source_id)
    select!(countries, [:country_name, :country_iso3, :source_name, :source_id])
    return countries
end
 
"""
    rda_sites(source_name=nothing, source_id=nothing, 
        country_name=nothing, country_iso3=nothing)

Return sites, optionally filtered by source (`source_name` or `source_id`) and/or country (`country_name` or `country_iso3`)
"""
function rda_sites(; source_name::Union{Nothing,String}=nothing,
                     source_id::Union{Nothing,Int}=nothing, 
                     country_name::Union{Nothing,String}=nothing, 
                     country_iso3::Union{Nothing,String}=nothing)

    sources = deepcopy(rda_env.sources)
    sites = leftjoin(rda_env.sites, sources, on=:source_id)
    rename!(sites, :name => :source_name)
    select!(sites, [:site_id, :site_name, :country_name, :country_iso3, :source_id, :source_name])

    if source_name !== nothing
        if !(source_name in sites.source_name)
            error("source_name not found.")
        end
        sites = filter(row -> row.source_name == source_name, sites)
    elseif source_id !== nothing
        if !(source_id in sites.source_id)
            error("source_id not found.")
        end
        sites = filter(row -> row.source_id == source_id, sites)
    end

    if country_name !== nothing
        if !(country_name in sites.country_name)
            error("country_name not found.")
        end
        sites = filter(row -> row.country_name == country_name, sites)
    elseif country_iso3 !== nothing
        if !(country_iso3 in sites.country_iso3)
            error("country_iso3 not found.")
        end
        sites = filter(row -> row.country_iso3 == country_iso3, sites)
    end

    return sites
end


"""
    rda_deaths(source_name=nothing, source_id=nothing, 
                site_name=nothing, site_id=nothing, 
                country_name=nothing, country_iso3=nothing)

Return deaths, optionally filtered by source (`source_name` or `source_id`), site (`site_name` or `site_id`), and/or country (`country_name` or `country_iso3`)
"""
function rda_deaths(; source_name::Union{Nothing,String}=nothing, 
    source_id::Union{Nothing,Int}=nothing, 
    site_name::Union{Nothing,String}=nothing, 
    site_id::Union{Nothing,Int}=nothing, 
    country_name::Union{Nothing,String}=nothing, 
    country_iso3::Union{Nothing,String}=nothing)

    deaths = deepcopy(rda_env.deaths)
    sites = deepcopy(rda_env.sites)
    sources = deepcopy(rda_env.sources)

    deaths = leftjoin(deaths, sites, on=:site_id)
    deaths = leftjoin(deaths, sources, on=:source_id)
    rename!(deaths, :name => :source_name)
    select!(deaths, [:death_id, :external_id, :site_id, :site_name, :country_name, :country_iso3, :source_id, :source_name])

    if source_name !== nothing
        if !(source_name in deaths.source_name)
            error("source_name not found.")
        end
        deaths = filter(row -> row.source_name == source_name, deaths)
    elseif source_id !== nothing
        if !(source_id in deaths.source_id)
            error("source_id not found.")
        end
        deaths = filter(row -> row.source_id == source_id, deaths)
    end

    if site_name !== nothing
        if !(site_name in deaths.site_name)
            error("site_name not found.")
        end
        deaths = filter(row -> row.site_name == site_name, deaths)
    elseif site_id !== nothing
        if !(site_id in deaths.site_id)
            error("site_id not found.")
        end
        deaths = filter(row -> row.site_id == site_id, deaths)
    end

    if country_name !== nothing
        if !(country_name in deaths.country_name)
            error("country_name not found.")
        end
        deaths = filter(row -> row.country_name == country_name, deaths)
    elseif country_iso3 !== nothing
        if !(country_iso3 in deaths.country_iso3)
            error("country_iso3 not found.")
        end
        deaths = filter(row -> row.country_iso3 == country_iso3, deaths)
    end

    return deaths
end


"""
    rda_data_dict(dataset_name=nothing, dataset_id=nothing)

Return data dictionary for dataset specified by either `dataset_name` or `dataset_id`. To get available datasets, use `rda_datasets()` function
"""
function rda_data_dict(;dataset_name::Union{Nothing,String}=nothing, dataset_id::Union{Nothing,Int}=nothing)

    variables          = rda_env.variables
    dataset_variables  = rda_env.dataset_variables
    datasets           = rda_env.datasets
    vocabularies       = rda_env.vocabularies 

    # No dataset specified → return all
    if dataset_name === nothing && dataset_id === nothing
        @warn "No dataset specified; returning all variables. Use `rda_datasets()` to list all available datasets."
        return copy(variables)
    end

    if dataset_name !== nothing
        rows = findall(==(dataset_name), datasets.name)
        if isempty(rows)
            error("Dataset name '$dataset_name' not found. Use `rda_datasets()` to list available datasets.")
        elseif length(rows) > 1
            @warn "Multiple datasets match name '$dataset_name'; using the first match."
        end
        dataset_id = datasets.dataset_id[rows[1]]
    end

    # Safety: ensure we have a dataset_id now
    if dataset_id === nothing
        error("No dataset_id could be resolved. Provide `dataset_name` or `dataset_id`.")
    end

    # Lookup variable IDs linked to the dataset
    var_ids = dataset_variables.variable_id[dataset_variables.dataset_id .== dataset_id]

    if isempty(var_ids)
        @warn "No variables linked to dataset_id=$dataset_id."
        return variables[0, :]  # empty DataFrame with the same schema
    end

    # Filter variables to those in var_ids
    dict = variables[in.(variables.variable_id, Ref(var_ids)), :]    
    
    # Add value types to dictionary
    value_types = deepcopy(rda_env.value_types)[:,[:value_type, :value_type_id]]
    dict = leftjoin(dict, value_types,on=:value_type_id)
    rename!(dict, :name => :Column_Name, :description => :Description,
                   :value_type_id => :DataType, :value_type => :DataType_Label, 
                   :note => :Note, :keyrole => :Key)
    cols = [:Column_Name, :Description, :DataType, :DataType_Label, :Note, :Key]
    
    return dict[:,cols]
    
end

"""
    rda_data(; dataset_name=nothing, dataset_id=nothing)

Load dataset as DataFrame, where dataset is specified by either `dataset_name` or `dataset_id`. To get available datasets, use `rda_datasets()` function
"""
function rda_data(; dataset_name::Union{Nothing,String}=nothing , dataset_id::Union{Nothing,Int}=nothing)
    if dataset_name === nothing && dataset_id === nothing
        error("Please specify dataset id or dataset name to load the data. Call rda_datasets() for available datasets in RDA.")

    else
        if dataset_name !== nothing 
            datasets = deepcopy(rda_env.datasets)
            dataset_id = datasets[datasets.name .== dataset_name, :dataset_id][1]
        end
        
        data = dataset_to_dataframe(rda_env.db_conn, dataset_id)

        return data
    end
end

"""
    rda_schema(fields=false)

List tables and fields (schema)
"""
function rda_schema(; fields=false)
    tables = deepcopy(rda_env.db_schema)
    if fields
        return tables
    else
        return tables[:,:name]
    end
end

"""
    rda_table(table_name="")

Return the contents of `table_name` as a DataFrame
"""
function rda_table(table_name::String)
    db = deepcopy(rda_env.db_conn)
    tables = deepcopy(rda_env.db_schema)

    # Case-insensitive lookup
    lookup = Dict(lowercase(n) => n for n in tables.name)
    key = lowercase(String(table_name))

    if !haskey(lookup, key)
        @warn "Table name '$table_name' is not in schema. Check available tables using rda_schema()."
        return DataFrame()
    end

    table = get_table(db, lookup[key])

    return table
end


end # module juRDA
