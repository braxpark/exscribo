2 PHASES AND ORDERINGS OF TABLES

QUERY ORDER
&
INSERTION ORDER

DIFFERENCE IN ORDERINGS TO DO ONLY GRABBING FILTERED ROW SETS FROM NON DIRECT DESCENDANTS DEPENDS
ON ROW SET OF DIRECT DESCENDANTS

SO AS IN INSERTION ORDER WHERE NON DIRECT DESCENDANTS MUST BE INSERTED BEFORE THE DESCENDENT TABLES THEY SUPPORT
IN QUERY ORDER, THESE MUST COME AFTER THE ROW SET OF DESCENDANT TABLES ARE FOUND

LET DIRECT DESCENDANTS SET OF TABLES = A
LET NON DIRECT DESCENDANTS SET OF TABLES = B


INSERTION ORDER = B, A
QUERY ORDER = A, B;

WHERE A AND B BOTH ARE TOPOLOGICALLY SORTED
THE TOPOLOGICAL SORT FOR A IS ON THE FKEY DEPENDENCY
THE TOPOLOGICAL SORT FOR B IS ON THE INVERSE FKEY DEPENDENCY

THIS IS EASY TO SEE BECAUSE OF THE NATURE OF B REQUIRING THE REDUCTION ROW SET OF THE SUB SET OF TABLES EACH TABLE IN THE NON DIRECT DESCENDANT SET SUPPORTS

THIS IS OPPPOSITE OF THE NATURE OF A WHERE THE REDUCTION ROW SET OF EACH TABLE IN THE DESCENDENT SET IS FOUND FROM THE TABLES THE DESCENDENT TABLES DEPENDS ON



INFORMATION NEEDED:

TABLE NAME  (map -> map[table_name][dependent_table][table_col_name] -> destination_col_name)
            (**example map[retailer_suppliers][deductions][id] -> retailer_supplier_id)
            (**example inverse_map[retailer_suppliers][deductions][retailer_supplier_id] -> id)
            (reduction of this idea ==> map[supporting_table][dependent_table][supporting_col_name] -> dependent_col_name)
            (reduction of this idea for inverse ==> inverse_map[supporting_table][dependent_table][dependent_col_name] -> supporting_col_name)
 - FOR EACH TABLE IN DEPENDENT QUERY RESULT SET
    - LOCAL FKEY COLUMN
      DESTINATION FKEY COLUMN

    - ALSO STORE THE INVERSE
      - DESTINATION FKEY COLUMN
      - LOCAL FKEY COLUMN


TODO:
- Query and Insert
    - Generating queries for query op per table (joins on table where table.fkey_col IN( list of ids previously queried for));
    - store in flat files results of queries, defined structure and cleanup. Maybe we can have a defer.cleanup() like in zig?

