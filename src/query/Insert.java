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
        obj = tree.getValues();

        //check if the table exists and set schema
        schema = QueryCheck.tableExists(fileName);
        QueryCheck.insertValues(schema, obj);
        tuple = new Tuple(schema);



   } // public Insert(AST_Insert tree) throws QueryException

    /**
    * Executes the plan and prints applicable output.
    */
    public void execute() {
        //add tuple to table

        tuple.setAllFields(obj);
        HeapFile hf = new HeapFile(fileName);
        RID rid = tuple.insertIntoFile(hf);

        //if indexes exist, add tuple to indexes
        IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(fileName);
        for(int i = 0; i < indexes.length; i++) {
            HashIndex index = new HashIndex(indexes[i].indexName);
            SearchKey key = new SearchKey(tuple.getField(indexes[i].columnName));
            index.insertEntry(key, rid);
        }

        //increase count in system catalog
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
        System.out.println("1 rows affected.");

    } // public void execute()

} // class Insert implements Plan
