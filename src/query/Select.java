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

    HeapFile[] heapFiles;
    Iterator[] fileScans;
    Iterator[] simpleJoins;
    Iterator projection;
    Iterator selection;
    Integer[] fields;
    //Iterator[] iters;



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
        heapFiles = new HeapFile[tables.length];
        fileScans = new FileScan[tables.length];
        simpleJoins = new SimpleJoin[tables.length - 1];
        fields = new Integer[columns.length];

        //validate and open tables
        for (int i = 0; i < tables.length; i++) {
            try {
                schemas[i] = QueryCheck.tableExists(tables[i]);
            } catch (QueryException e) {
                //close scans just opened
                for (int j = 0; j < i; j++) {
                    fileScans[j].close();
                }
                throw e;
            }
            heapFiles[i] = new HeapFile(tables[i]);
            fileScans[i] = new FileScan(schemas[i], heapFiles[i]);
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
                //close scans just opened
                for (int j = 0; j < fileScans.length; j++) {
                    fileScans[j].close();
                }
                throw new QueryException("Column not found");
            }
            finalSchema.initField(i, allSchema, fldno);
            fields[i] = fldno;
        }

        if (columns.length == 0) {
            finalSchema = allSchema;
            fields = new Integer[finalSchema.getCount()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = i;
            }
        }

        //validate predicates
        for (Predicate[] p1 : predicates) {
            for (Predicate p : p1) {
                if (!p.validate(allSchema)) {
                    //close scans just opened
                    for (int j = 0; j < fileScans.length; j++) {
                        fileScans[j].close();
                    }
                    throw new QueryException("Invalid predicate");
                }
            }
        }

        //naive implementation with 0 joins
        if (simpleJoins.length < 0) {
            throw new QueryException("No tables selected");
        }
        else if (simpleJoins.length == 0) {
            if (predicates.length > 0) {
                selection = new Selection(fileScans[0], predicates[0]);
            }
            else {
                selection = fileScans[0];
            }

            projection = new Projection(selection, fields);
        }

        else {
            for (int i = 0; i < fileScans.length; i++) {
                fileScans[i].close();
            }
            throw new QueryException("Join necessary. (Not implemented)");
        }


    } // public Select(AST_Select tree) throws QueryException

    /**
    * Executes the plan and prints applicable output.
    */
    public void execute() {

        //while (fileScans[0].hasNext())
            //fileScans[0].getNext().print();
        projection.execute();
        for (int i = 0; i < fileScans.length; i++) {
            fileScans[i].close();
        }
    // print the output message


    } // public void execute()

} // class Select implements Plan
