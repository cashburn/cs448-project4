package query;

import parser.AST_Insert;
import global.*;
import relop.*;
import index.*;
import heap.*;
/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {


    String fileName;
    Schema schema;
    Tuple tuple;
    Object[] obj;

    /**
    * Optimizes the plan, given the parsed query.
    *
    * @throws QueryException if table doesn't exists or values are invalid
    */
    public Insert(AST_Insert tree) throws QueryException {
        fileName = tree.getFileName();

        //check if the table exists and set schema
        schema = QueryCheck.tableExists(fileName);
        tuple = new Tuple(schema);

        obj = tree.getValues();

   } // public Insert(AST_Insert tree) throws QueryException

    /**
    * Executes the plan and prints applicable output.
    */
    public void execute() {
        //add tuple to table

        tuple.setAllFields(obj);
        HeapFile hf = new HeapFile(fileName);
        tuple.insertIntoFile(hf);

        //if indexes exist, add tuple to indexes

        Schema syscatS = Minibase.SystemCatalog.s_rel;
        FileScan syscatFs = new FileScan(syscatS, Minibase.SystemCatalog.f_rel);
        String tmp = null;
        Tuple syscatT = null;
        while (syscatFs.hasNext() && tmp == null) {
            syscatT = syscatFs.getNext();
            tmp = syscatT.getStringFld(0);
            if (!fileName.equalsIgnoreCase(tmp))
                tmp = null;
        }

        int recCnt = syscatT.getIntFld(1) + 1;
        syscatT.setIntFld(1, recCnt);
        syscatT.insertIntoFile(hf);


    } // public void execute()

} // class Insert implements Plan
