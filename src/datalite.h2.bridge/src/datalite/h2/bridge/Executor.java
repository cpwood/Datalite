package datalite.h2.bridge;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.h2.tools.Server;

public class Executor {
	public static void main(String[] args) throws SQLException, IOException, JSONException {
        String connectionString = args[0];
        String username = args[1];
        String password = args[2];
        String sql = args[3];
        String jobId = args[4];
        String folder = args[5];
        
        Server server = Server.createTcpServer("-tcpPort", "8899", "-ifExists", "-baseDir", ".").start();
        
        try {
        	Connection conn = DriverManager.getConnection(connectionString, username, password);
        	Statement stmt = conn.createStatement();
        	ResultSet resultSet = stmt.executeQuery(sql);

        	FileWriter schemaWriter = new FileWriter(Paths.get(folder, jobId + "_schema.json").toString());
        	FileWriter writer = new FileWriter(Paths.get(folder, jobId + ".json").toString());
        	
        	ResultSetMetaData md = resultSet.getMetaData();
        	int numCols = md.getColumnCount();
        	String[] colNames = new String[numCols];
        	int[] colTypes = new int[numCols];
        	
        	JSONArray columns = new JSONArray();
        	
        	for (int i = 0; i < colNames.length; i++) {
                colNames[i] = md.getColumnLabel(i + 1);
                colTypes[i] = md.getColumnType(i + 1);
               
                JSONObject column = new JSONObject();
                column.put("column", colNames[i]);
                column.put("type", colTypes[i]);
                column.put("required", md.isNullable(i + 1) == DatabaseMetaData.columnNoNulls);
                columns.put(column);
            }
        	
        	schemaWriter.write(columns.toString());
        	schemaWriter.close();

        	while (resultSet.next()) {
        	    JSONObject row = new JSONObject();
        	    
        	    for (int i = 0; i < colNames.length; i++) {
                    switch (colTypes[i]) {
                    	case Types.ARRAY:
                    		Object[] arr = (Object[]) resultSet.getArray(colNames[i]).getArray();
                    		
                    		if (resultSet.wasNull()) {
                    			row.put(colNames[i], JSONObject.NULL);
                    			break;
                    		}
                    		
                    		JSONArray jarr = new JSONArray();
                    		
                    		for (int j = 0; j < arr.length; j++) {
                    	         jarr.put(arr[j]);
                    	      }
                    		
                    		row.put(colNames[i], jarr);
                    		break;
                    	case Types.BINARY:
                    	case Types.LONGVARBINARY:
                    	case Types.VARBINARY:
                    		InputStream stream = resultSet.getBinaryStream(colNames[i]);
                    		
                    		if (resultSet.wasNull()) {
                    			row.put(colNames[i], JSONObject.NULL);
                    			break;
                    		}
                    		
                    		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
                    		byte[] buffer = new byte[1024 * 1024];
                    		int read = stream.read(buffer, 0, buffer.length);
                    		
                    		while (read > 0) {
                    	  		for (int j = 0; j < read; j++) {
                    	  			outputStream.write(buffer[j]);
                        		}	
                    	  		
                    	  		read = stream.read(buffer, 0, buffer.length);
                    		}
                    		
                    		row.put(colNames[i], "base64:" + Base64.getEncoder().encodeToString(outputStream.toByteArray()));
                    		break;
                    	default:
                    		Object value = resultSet.getObject(colNames[i]);
                    		
                    		if (resultSet.wasNull()) {
                    			row.put(colNames[i], JSONObject.NULL);
                    			break;
                    		}
                    		
            	            row.put(colNames[i], value);
            	            break;
                    }
                }
        	    
        	    writer.write(row.toString());
        	    writer.write("\n");
        	}
        	
        	writer.close();
        	conn.close();
        }
        finally {
        	server.stop();
        }
    }
}
