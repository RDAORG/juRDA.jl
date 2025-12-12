using Pkg
Pkg.activate(@__DIR__)
Pkg.develop(PackageSpec(path = joinpath(@__DIR__, "..")))
Pkg.instantiate()

using Documenter
using juRDA

DocMeta.setdocmeta!(juRDA, :DocTestSetup, :(using juRDA); recursive = true)

# set latexmk path
if Sys.isapple()
    texbin = "/Library/TeX/texbin"
    if !occursin(texbin, get(ENV, "PATH", ""))
        ENV["PATH"] = texbin * ":" * get(ENV, "PATH", "")
    end
end

makedocs(
    sitename = "juRDA.jl",
    modules  = [juRDA],
    authors  = "Yue Chu",
    clean    = true,
    #format  = Documenter.LaTeX(),
    format  = Documenter.HTML(
        prettyurls=get(ENV, "CI", "false") == "true",
        assets=String[],
    ),
    pages = Any[
        "Home" => "index.md",
        "Functions"  => "functions.md",
    ],
)

deploydocs(
    repo   = "https://github.com/RDAORG/juRDA.jl.git",
    devbranch="main",
    push_preview = true,
    )
