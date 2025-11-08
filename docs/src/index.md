```@setup using-pkgs
using juRDA
```

# Introduction

**juRDA** is a package for working with [Reference Data Archive (RDA)](http://data.who.int/platforms/rda) data in Julia.

The package provides user-friendly functions to load RDA SQLite database, check available data sources and datasets, extract data, dictionaries, and supporting documents like protocols etc. 

## Installation

The package is pre-installed on RDA analytics environment. To install it for local use, run the following at the Julia REPL:

```julia
import Pkg; Pkg.add(url="https://github.com/RDAORG/juRDA.jl")
```