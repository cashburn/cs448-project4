package query;

import parser.AST_Delete;
import parser.AST_Delete;
import heap.HeapFile;
import relop.FileScan;
import relop.Schema;
import relop.Predicate;
import relop.Tuple;
import global.Minibase;
import global.SearchKey;
import index.HashIndex;
import global.RID;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
  Schema sch;
  String file;
  HeapFile hf;
  Predicate[][] preds;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
    preds = tree.getPredicates();
    sch = QueryCheck.tableExists(tree.getFileName());
    QueryCheck.predicates(sch, preds);
    hf = new HeapFile(tree.getFileName());

    file = tree.getFileName();
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    FileScan filescan = new FileScan(sch, hf);
    Tuple temp;
    int count = 0;
    boolean check;

    while(filescan.hasNext()){
      temp = filescan.getNext();
      check = true;
      //Assigns to the next tuple

      for (int i = 0; i < preds.length; i++){
        for (int j = 0 ; j<preds[i].length; j++){
          if(!preds[i][j].evaluate(temp)){
            check = false;
          }
        }
      }
      if(check){
        count++;
        RID rid = filescan.getLastRID();
        hf.deleteRecord(rid);
        IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(file);
        for (IndexDesc in : indexes){
          HashIndex hash = new HashIndex(in.indexName);
          SearchKey sk = new SearchKey(temp.getField(in.columnName));
          hash.deleteEntry(sk, rid);
        }
      }
    }
    filescan.close();

    System.out.println(count + " rows affected.");
    // print the output message
    // System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Delete implements Plan
