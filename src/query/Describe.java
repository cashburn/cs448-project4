package query;

import parser.AST_Describe;
import relop.Schema;
import global.AttrType;
import java.util.ArrayList;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

    String fileName;
    Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
      fileName = tree.getFileName();
      schema = QueryCheck.tableExists(fileName);

  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
      //TODO: Print out types
      // String[] cols = new String[schema.getCount()];
      // String[] names = new String[schema.getCount()];
      for(int i = 0 ; i< schema.getCount(); i++){
        // cols[i] = AttrType.toString(schema.fieldType(i));
        // names[i] = schema.fieldName(i);
        System.out.printf("(%s) %s ", AttrType.toString(schema.fieldType(i)), schema.fieldName(i));
        if(i != schema.getCount()-1){
          System.out.print("|");
        }
      } 
      System.out.println();

  } // public void execute()

} // class Describe implements Plan
