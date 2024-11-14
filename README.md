## [exscribo](https://en.wiktionary.org/wiki/exscribo#Latin)

_to copy_



## tldr
This tool copies data from a source databse to a destination database. For now this only supports Postgres.
Given some table and id, this will copy all relevant rows based on a source database's schema to a destination database.

This generates the schema map and determines which rows fit in to that schema map based on the "root" table passed in as a param. 
Specifically, Postgres' information schema is leveraged to discover foreign key relations between tables. Since these relations can be considered as uni-directional edges between tables, a map can be generated through a variety of search algorithms. There is a bit more subtelty here, but that is the gist.

This has a couple of key requirements:
<ul>
  <li>
    The source and destination databases need to have the same schema.
  </li>
  <li>
    It is strongly suggested to have the destination database be empty as no id mappings are currently performed when copying data. If the destination database is not empty, it runs the risk of some copied rows having identical id values to existing rows.
  </li>
</ul>


## installation
This project uses submodules for the Postgres Driver (pgfe) and JSON (struct_mapping). These will need to be pulled if trying to build from source.

This project links to `libpq` and requires some header files. So, these need to be path accessible, the executable on the `PATH` and the header files on whichever global c++ path your compiler uses. For GCC, it's `CPLUS_INCLUDE_PATH`.

This project uses premake5 as its build tool. 

Once the above have been installed and prepared:

  - Run the `setup.sh` script in the root directory of this repository to copy over the header files for both `pgfe` and `struct_mapping`.
  - Run `premake5 gmake2` or whichever target you want to build for.
  - In the root directory of this repository, run `make` (again if building for gnu makefiles).

