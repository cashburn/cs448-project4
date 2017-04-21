package query;

import parser.AST_Describe;
import relop.*;

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
      schema.print();
  } // public void execute()

} // class Describe implements Plan
