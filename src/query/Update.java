package query;

import parser.AST_Update;
import global.Minibase;
import global.RID;
import heap.HeapFile;
import parser.AST_Update;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {
  String filename;
  HeapFile hf;
  Schema sch;
  Predicate[][] preds;
  String[] headers;
  Object[] vals;
  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {
    filename = tree.getFileName();
    hf = new HeapFile(filename);
    sch = QueryCheck.tableExists(filename);
    preds = tree.getPredicates();
    headers = tree.getColumns();
    vals = tree.getValues();
    int[] nums;

    QueryCheck.predicates(sch, preds);
    for (String c : headers){
      QueryCheck.columnExists(sch, c);
    }

    nums = new int[headers.length];

    for (int i = 0;i<headers.length; i++){
      nums[i] = sch.fieldNumber(headers[i]);
    }
    QueryCheck.updateValues(sch, nums, vals);
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    int count = 0;
    IndexDesc[] ids = Minibase.SystemCatalog.getIndexes(filename);
    boolean check = true;
    FileScan fs = new FileScan(sch, hf);

    while(fs.hasNext()){
      Tuple temp = fs.getNext();
      for (Predicate[] p : preds){
        for (Predicate pre: p){
            if(!pre.evaluate(temp)){
              check = false;
            }
          }
        }
        if(check){
            count++;
            Tuple t = temp;
            RID tRid = fs.getLastRID();
            for (int i = 0; i<headers.length; i++){
              t.setField(headers[i], vals[i]);
              byte[] insert = t.getData();
              hf.updateRecord(tRid, insert);
            }
        }
      }
      fs.close();
    // print the output message
     System.out.println(count + " rows updated.");
  } // public void execute()

} // class Update implements Plan
