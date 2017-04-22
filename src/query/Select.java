package query;

import parser.AST_Select;
import global.*;
import relop.*;
import index.*;
import heap.*;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

    String[] columns;
    SortKey[] orders;
    Predicate[][] predicates;
    String[] tables;
    boolean isDistinct;
    boolean isExplain;
    Schema[] schemas;
    Schema allSchema;
    Schema finalSchema;

    /**
    * Optimizes the plan, given the parsed query.
    *
    * @throws QueryException if validation fails
    */
    public Select(AST_Select tree) throws QueryException {
        columns = tree.getColumns();
        orders = tree.getOrders();
        predicates = tree.getPredicates();
        tables = tree.getTables();
        isExplain = tree.isExplain;
        isDistinct = false; //assume not distinct for project
        schemas = new Schema[tables.length];

        //validate inputs
        for (int i = 0; i < tables.length; i++) {
            schemas[i] = QueryCheck.tableExists(tables[i]);
        }

        //build join schema
        allSchema = new Schema(0);
        for (int i = 0; i < schemas.length; i++) {
            allSchema = Schema.join(allSchema, schemas[i]);
        }

        //build final schema
        finalSchema = new Schema(columns.length);
        for (int i = 0; i < columns.length; i++) {
            int fldno = allSchema.fieldNumber(columns[i]);
            if (fldno < 0) {
                throw new QueryException("ERROR: Column not found");
            }
            finalSchema.initField(i, allSchema, fldno);
        }

        if (columns.length == 0) {
            finalSchema = allSchema;
        }
        System.out.println("Final Schema");
        finalSchema.print();
    } // public Select(AST_Select tree) throws QueryException

    /**
    * Executes the plan and prints applicable output.
    */
    public void execute() {

    // print the output message
    System.out.println("0 rows selected. (Not implemented)");

    } // public void execute()

} // class Select implements Plan
