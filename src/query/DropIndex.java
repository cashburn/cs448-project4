package query;

import parser.AST_DropIndex;

import global.Minibase;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_DropTable;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

    private String fileName;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {

      //make sure index exists
      fileName = tree.getFileName();
      QueryCheck.indexExists(fileName);

  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

      new HashIndex(fileName).deleteFile();
      Minibase.SystemCatalog.dropIndex(fileName);

      System.out.printf("Index %s dropped.\n", fileName);

  } // public void execute()

} // class DropIndex implements Plan
