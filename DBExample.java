//STEP 1. Import required packages
import java.sql.*;

public class DBExample {
	
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost:3306/";

   //  Database credentials
   static final String USER = "root";
   static final String PASS = "1";
   
   public static void main(String[] args) {
   Connection conn = null;
   Statement stmt = null;
   String statement1 = 	"SELECT d.dept_name, d.dept_no, COUNT(e.emp_no) AS num_emps\n"+
		   				"FROM departments AS d JOIN dept_emp AS e ON d.dept_no=e.dept_no\n"+
		   				"GROUP BY e.dept_no\n"+
		   				"ORDER BY num_emps;";
   
   String statement2 = 	"SELECT d.dept_no, \n"+ // Wrong
				        "SUM(CASE WHEN e.gender='F' THEN 1 END)/COUNT(*) AS ratio, \n"+
				        "(SUM(CASE WHEN s.emp_no=(CASE WHEN e.gender='F' THEN e.emp_no END) THEN s.salary END)/SUM(CASE WHEN e.gender='F' THEN 1 END)) AS fem_sal, \n"+
				        "(SUM(CASE WHEN s.emp_no=(CASE WHEN e.gender='M' THEN e.emp_no END) THEN s.salary END)/SUM(CASE WHEN e.gender='M' THEN 1 END)) AS male_sal \n"+
				        "FROM employees e, dept_emp d, salaries s \n"+
				        "WHERE d.emp_no=e.emp_no AND d.emp_no=s.emp_no \n"+
				        "GROUP BY dept_no;"; 
   
   String statement3 = 	"SELECT e.first_name, e.last_name, d.to_date, d.from_date, e.emp_no  \n"+ //Wrong
		   				"FROM dept_manager AS d JOIN employees AS e ON d.emp_no=e.emp_no \n"+
		   				"ORDER BY DATEDIFF(d.from_date, d.to_date) \n" + 
		   				"LIMIT 100;";
   String statement4 = "SELECT * FROM " // Wrong
   					+ "(SELECT d.dept_no, CONCAT(SUBSTR(e.birth_date,1,3),'0') "
			   		+ "AS decade, COUNT(e.birth_date) "
			   		+ "AS num_emps, TRUNCATE((SUM(s.salary)/COUNT(e.birth_date)),2) "
			   		+ "AS avg_sal FROM dept_emp d, employees e, (SELECT emp_no, SUM(salary) AS salary FROM salaries GROUP BY emp_no) s "
			   		+ "WHERE d.emp_no=e.emp_no AND d.emp_no=s.emp_no "
			   		+ "GROUP BY dept_no, CONCAT(SUBSTR(birth_date,1,3),'0')) AS sub_query "
			   		+ "ORDER BY dept_no;"; 
   
   String statement5 = 	"SELECT DISTINCT e.*\n"+
			     		"FROM employees e, salaries s, dept_manager d \n" + 
			     		"WHERE e.emp_no=s.emp_no\n" + 
			     		"AND d.emp_no=e.emp_no \n" + 
			     		"AND s.salary > 80000\n" + 
			     		"AND e.gender = 'F'\n" + 
			     		"AND e.birth_date <= '1990-01-01';";
 // for
 String [] statements = {statement1, statement3, statement4, statement5};
 int count = 0;
 for(String sql : statements) {
	 count++;
   try{
      //STEP 2: Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      //STEP 3: Open a connection
      //System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);

      //STEP 4: Execute a query
      System.out.println("Query "+count+":\n");
      stmt = conn.createStatement();
     
      //sql = "SELECT id, first, last, age FROM Employees";
      ResultSet rs = stmt.executeQuery("use employees;");

      
      rs = stmt.executeQuery(sql);
      
      //STEP 5: Extract data from result set
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnsNumber = rsmd.getColumnCount();
      while (rs.next()) {
          for (int i = 1; i <= columnsNumber; i++) {
              if (i > 1) System.out.print(",  ");
              String columnValue = rs.getString(i);
              System.out.print(columnValue);
          }
          System.out.println("");
      }
      //STEP 6: Clean-up environment
      rs.close();
      stmt.close();
      conn.close();
   }
      catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }finally{
      //finally block used to close resources
      try{
         if(stmt!=null)
            stmt.close();
      }catch(SQLException se2){
      }// nothing we can do
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }//end finally try
   }//end try
   System.out.println('\n');
   }// end for
   
   System.out.println("Goodbye!");
}//end main
	

}//end FirstExample

