workspace("cpp_schema")
configurations({ "Debug", "Release" })

local projectName = "cpp_schema"
local projectKind = "ConsoleApp"
local lang = "C++"
local standard = lang .. "20"
local includePath = "src/include/"

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
	includedirs({ includePath })
	links({ "pq" })
