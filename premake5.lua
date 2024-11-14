workspace("exscribo")
configurations({ "Debug", "Release" })

local projectName = "exscribo"
local projectKind = "ConsoleApp"
local lang = "C++"
local standard = lang .. "20"
local pgfeIncludePath = "pgfe/src/pgfe/"
local structMappingIncludePath = "struct_mapping/include/struct_mapping/"

local srcFiles = {
	"src/**",
}

local excludeSrcFiles = {
	"src/lib/**",
	"src/include/**",
}

project(projectName)
kind(projectKind)
language(lang)
cppdialect(standard)
targetdir("bin/%{cfg.buildcfg}")
location("src/")
files(srcFiles)
removefiles({ excludeSrcFiles })
includedirs({ pgfeIncludePath, structMappingIncludePath })
links({ "pq" })
