using SQLite
using DBInterface
using DataFrames
using Logging
using RDAIngest

# use URI file name to open and set mode to read-only
function _db_filename(path::AbstractString, name::AbstractString)
    fname = endswith(lowercase(name), ".sqlite") ? name : name * ".sqlite"
    return joinpath(path, fname)
end

# Global environment-like object
Base.@kwdef mutable struct RDAEnv
<<<<<<< Updated upstream
    db_path::String = joinpath(@__DIR__, "..", "..","RDAIngest.jl/database") |> normpath
    db_name::String = "RDA"
=======
    db_path::String = joinpath(@__DIR__, "..", "..") |> normpath
    db_name::String = "RDA"              # can also take RDA.sqlite
    
>>>>>>> Stashed changes
    db_conn::Union{SQLite.DB, Nothing} = nothing

    db_schema::Union{DataFrame, Nothing} = nothing
    db_tables::Union{Vector{String}, Nothing} = nothing

    # Table-like fields (only filled if corresponding tables exist)
    sources::Union{DataFrame, Nothing} = nothing
    sites::Union{DataFrame, Nothing} = nothing
    ethics::Union{DataFrame, Nothing} = nothing
    protocols::Union{DataFrame, Nothing} = nothing
    site_protocols::Union{DataFrame, Nothing} = nothing
    transformations::Union{DataFrame, Nothing} = nothing
    data_ingestions::Union{DataFrame, Nothing} = nothing
    value_types::Union{DataFrame, Nothing} = nothing
    vocabularies::Union{DataFrame, Nothing} = nothing
    domains::Union{DataFrame, Nothing} = nothing
    variables::Union{DataFrame, Nothing} = nothing
    vocabulary_mapping::Union{DataFrame, Nothing} = nothing
    datasets::Union{DataFrame, Nothing} = nothing
    repository::Union{DataFrame, Nothing} = nothing
    transformation_inputs::Union{DataFrame, Nothing} = nothing
    transformation_outputs::Union{DataFrame, Nothing} = nothing
    dataset_variables::Union{DataFrame, Nothing} = nothing
    ingest_datasets::Union{DataFrame, Nothing} = nothing
    instruments::Union{DataFrame, Nothing} = nothing
    instrument_datasets::Union{DataFrame, Nothing} = nothing
    protocol_instruments::Union{DataFrame, Nothing} = nothing
    deaths::Union{DataFrame, Nothing} = nothing
    death_rows::Union{DataFrame, Nothing} = nothing
    unit_of_analysis_types::Union{DataFrame, Nothing} = nothing
end

const rda_env = RDAEnv()

function _connect_db()
    if rda_env.db_conn === nothing
        dbfile = _db_filename(rda_env.db_path, rda_env.db_name)

        # Sanity checks
        if !isfile(dbfile)
            error("SQLite file not found: $(dbfile)")
        end
    
        try
            # Open strictly read-only (no -wal/-shm creation)
            rda_env.db_conn = SQLite.DB("file:" * dbfile * "?mode=ro")

            # Extra belt-and-suspenders: forbid writes at connection level
            SQLite.execute(rda_env.db_conn, "PRAGMA query_only = ON;")
            SQLite.execute(rda_env.db_conn, "PRAGMA foreign_keys = ON;")

            @info "SQLite database loaded."

        catch e
            @error "Failed to open SQLite database." dbfile exception=(e, catch_backtrace())
            rethrow(e)
        end
    end

    # Load schema
    try
        sch = SQLite.tables(rda_env.db_conn) |> DataFrame
        rda_env.db_schema = sch
        rda_env.db_tables = String.(getproperty(sch, :name))
        @info "SQLite database schema and tables loaded."
        return rda_env.db_conn
    catch e
        @error "Failed to read SQLite schema." exception=(e, catch_backtrace())
        rethrow(e)
    end
end

function _load_initial_tables() 
    _connect_db() 
    
    if rda_env.db_schema === nothing || rda_env.db_tables === nothing
        @warn "No schema available; skipping table preloads."
        return
    end
    
    try 
        tables = filter(x -> !startswith(string(x), "db_"), fieldnames(RDAEnv)) 
        
        for table in tables 
            try 
                data = DBInterface.execute(rda_env.db_conn, "SELECT * FROM $(string(table))") |> DataFrame 
                setfield!(rda_env, table, data) 
            catch e 
                @warn "Failed to load table: $(table)" exception=(e, catch_backtrace()) 
            end 
        end 
    catch e 
        @error "Unexpected error during initial table loading." exception=(e, catch_backtrace()) 
    finally 
        @info "Core tables loaded. Use rda_env.datasets, rda_env.sources, etc." 
    end 
end

function __init__()
    _load_initial_tables()
end

