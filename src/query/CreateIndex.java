package query;

import parser.AST_CreateIndex;
import global.*;
import relop.*;
import index.*;
import heap.*;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

    private String fileName;
    private String ixTable;
    private String ixColumn;
    private Schema schema;

    /**
    * Optimizes the plan, given the parsed query.
    *
    * @throws QueryException if index already exists or table/column invalid
    */
    public CreateIndex(AST_CreateIndex tree) throws QueryException {
        fileName = tree.getFileName();
        ixTable = tree.getIxTable();
        ixColumn = tree.getIxColumn();

        QueryCheck.fileNotExists(fileName);
        //check if the table exists and set schema
        schema = QueryCheck.tableExists(ixTable);
        QueryCheck.columnExists(schema, ixColumn);

        //check if index already exists
        //IndexDesc indexd = new IndexDesc(new Tuple(schema));
        boolean caught = false;
        try {
            QueryCheck.indexExists(fileName);
        } catch(QueryException e) {
            caught = true;
        }
        if (!caught) {
            throw new QueryException("ERROR: index exists");
        }

    } // public CreateIndex(AST_CreateIndex tree) throws QueryException

    /**
    * Executes the plan and prints applicable output.
    */
    public void execute() {
        HashIndex hash = new HashIndex(fileName);
        FileScan fileScan = new FileScan(schema, new HeapFile(ixTable));
        int fieldNum = schema.fieldNumber(ixColumn);
        while (fileScan.hasNext()) {
            Tuple tuple = fileScan.getNext();
            hash.insertEntry(new SearchKey(tuple.getField(fieldNum)),
                    fileScan.getLastRID());
        }
        fileScan.close();
        Minibase.SystemCatalog.createIndex(fileName, ixTable, ixColumn);
        System.out.printf("Index %s created", fileName);
    } // public void execute()

} // class CreateIndex implements Plan
