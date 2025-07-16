module juRDA

using RDAIngest
using SQLite
using DBInterface
using DataFrames
using Logging

export load_rda, rda_sources, rda_countries, rda_sites, rda_deaths, rda_datasets, rda_data_dict, rda_data, rda_schema, rda_env

include("env.jl")
#__init__()

"""
    load_rda(path::Union{String, Nothing}=nothing)::SQLite.DB

Opens and returns a connection to the SQLite database at the given path.
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

Return sources table
"""

# Return sources dataframe
function rda_sources()
    return deepcopy(rda_env.sources)
end

"""
    rda_datasets(; doi=false, repo_id=false, 
                      source_name::Union{Nothing,String}=nothing, 
                      source_id::Union{Nothing,Int}=nothing)

Return available datasets
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
    rda_countries(; source_name::Union{Nothing,String}=nothing, source_id::Union{Nothing,Int}=nothing)

Return countries, optionally filter by source_name or source_id
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
    rda_sites(; source_name::Union{Nothing,String}=nothing,
                     source_id::Union{Nothing,Int}=nothing, 
                     country_name::Union{Nothing,String}=nothing, 
                     country_iso3::Union{Nothing,String}=nothing)

Return sites, optionally filtered by source and country
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
   rda_deaths(; source_name=nothing, source_id=nothing, site_name=nothing, site_id=nothing, country_name=nothing, country_iso3=nothing)

Return deaths, optionally filtered
"""

function rda_deaths(; source_name=nothing, source_id=nothing, site_name=nothing, site_id=nothing, country_name=nothing, country_iso3=nothing)
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
    rda_data_dict(dataset::Union{Nothing,String}=nothing)

# Return data dictionary for dataset
"""

function rda_data_dict(dataset_name::Union{Nothing,String}=nothing, 
                        dataset_id::Union{Nothing,String}=nothing)
    if dataset_name === nothing
        dict = deepcopy(rda_env.variables)
        # vocs = deepcopy(rda_env.vocabularies)

        @warn "No dataset name is provided, so return all variables available. If only want dictionary for a particular dataset, specify dataset name. 
                A list of all available datasets can be checked using rda_datasets()."
    else
        dataset_variables = deepcopy(rda_env.dataset_variables)
        variables = deepcopy(rda_env.variables)
        vocabularies = deepcopy(rda_env.vocabularies)
        datasets = deepcopy(rda_env.datasets)

        if dataset_name !== nothing 
            dataset_id = datasets[datasets.name .== dataset_name, :dataset_id][1]
        end
        
        var_ids = dataset_variables[dataset_variables.dataset_id .== dataset_id, :variable_id]
        dict = variables[in.(variables.variable_id, Ref(var_ids)), :]

    end
    
    value_types = deepcopy(rda_env.value_types)[:,[:value_type, :value_type_id]]
    dict = leftjoin(dict, value_types,on=:value_type_id)
    rename!(dict, :name => :Column_Name, :description => :Description,
                   :value_type_id => :DataType, :value_type => :DataType_Label, 
                   :note => :Note, :keyrole => :Key)
    cols = [:Column_Name, :Description, :DataType, :DataType_Label, :Note, :Key]
    
    return dict[:,cols]
    
end

"""
   function rda_data(; dataset_id::Union{Nothing,String}=nothing, dataset_name::Union{Nothing,String}=nothing)

Load dataset as DataFrame
"""

function rda_data(; dataset_name::Union{Nothing,String}=nothing , dataset_id::Union{Nothing,String}=nothing)
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
   function rda_schema(; fields=false)

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


end # module juRDA
