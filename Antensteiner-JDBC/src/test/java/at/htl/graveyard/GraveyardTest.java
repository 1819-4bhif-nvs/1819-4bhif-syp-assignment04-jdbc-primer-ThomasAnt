package at.htl.graveyard;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@FixMethodOrder
public class GraveyardTest {
    public static final String DRIVER_STRING= "org.apache.derby.jdbc.ClientDriver";
    static final String CONNECTION_STRING ="jdbc:derby://localhost:1527/db;create=true";
    static final String USER = "app";
    static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc(){
        try{
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING,USER,PASSWORD);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (SQLException e){
            System.out.println("Verbindung zur Datenbank nicht möglich:\n" + e.getMessage() + "\n");
            System.exit(1);
        }
    }
    @AfterClass
    public static void teardownJdbc(){
        try {
            conn.createStatement().execute("DROP table grave");
            System.out.println("Tabelle Grave gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle Grave konnte nicht gelöscht werden:\n" +
                    e.getMessage());
        }
        try {
            conn.createStatement().execute("DROP table graveyardkeeper");
            System.out.println("Tabelle graveyardkeeper gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle graveyardkeeper konnte nicht gelöscht werden:\n" +
                    e.getMessage());
        }
        try {
            conn.createStatement().execute("DROP table tombstone");
            System.out.println("Tabelle tombstone gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle tombstone konnte nicht gelöscht werden:\n" +
                    e.getMessage());
        }
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("Goodbye!");
            }
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    @Test
    public void ddl(){
        try{
            Statement statement = conn.createStatement();
            String sql;
            try {
                sql = "CREATE TABLE grave (" +
                        " id INT CONSTRAINT grave_pk PRIMARY KEY," +
                        " familyname VARCHAR(255) NOT NULL," +
                        " price INT NOT NULL)";
                statement.execute(sql);
            }catch (SQLException e){
                System.out.println(e.getMessage());
            }
            try {
                sql = "CREATE TABLE graveyardkeeper (" +
                        " id INT CONSTRAINT graveyardkeeper_pk PRIMARY KEY," +
                        " familyname VARCHAR(255) NOT NULL," +
                        " firstname VARCHAR(255) NOT NULL," +
                        " salary INT NOT NULL)";
                statement.execute(sql);
            }catch (SQLException e){
                System.out.println(e.getMessage());
            }
            try{
                sql = "CREATE TABLE tombstone(" +
                        " id INT CONSTRAINT tombstone_pk PRIMARY KEY," +
                        " material VARCHAR(255) NOT NULL," +
                        " price INT NOT NULL)";
                statement.execute(sql);

            }catch (SQLException e){
                System.out.println(e.getMessage());
            }
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }

    }
    @Test
    public void dml(){
        int countInserts = 0;
        try{
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO grave (id,familyname,price) VALUES(1,'Mustermann',300)";
            countInserts += statement.executeUpdate(sql);
            sql = "INSERT INTO grave (id,familyname,price) VALUES(2,'Musterfrau',5000)";
            countInserts += statement.executeUpdate(sql);
            sql = "INSERT INTO grave (id,familyname,price) VALUES(3,'Mustersohn',100)";
            countInserts += statement.executeUpdate(sql);

            sql = "INSERT INTO graveyardkeeper (id,familyname,firstname,salary) VALUES(1,'Mustermann','Max',1000)";
            countInserts += statement.executeUpdate(sql);
            sql = "INSERT INTO graveyardkeeper (id,familyname,firstname,salary) VALUES(2,'Mustertochter','Erika',800)";
            countInserts += statement.executeUpdate(sql);

            sql = "INSERT INTO tombstone (id,material,price) VALUES(1,'Granit',1000)";
            countInserts += statement.executeUpdate(sql);
            sql = "INSERT INTO tombstone (id,material,price) VALUES(2,'Mamor',2500)";
            countInserts += statement.executeUpdate(sql);
            sql = "INSERT INTO tombstone (id,material,price) VALUES(3,'Kalkstein',1500)";
            countInserts += statement.executeUpdate(sql);
        }catch(SQLException e){
            System.out.println(e.getMessage());
        }
        assertThat(countInserts,is(8));

        try {
            PreparedStatement prepstate = conn.prepareStatement("SELECT id,familyname,firstname,salary from graveyardkeeper");
            ResultSet rs = prepstate.executeQuery();

            rs.next();
            assertThat(rs.getString("familyname"),is("Mustermann"));
            assertThat(rs.getString("firstname"),is("Max"));
            assertThat(rs.getInt("salary"),is(1000));

            rs.next();
            assertThat(rs.getString("familyname"),is("Mustertochter"));
            assertThat(rs.getString("firstname"),is("Erika"));
            assertThat(rs.getInt("salary"),is(800));
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            PreparedStatement prepstate = conn.prepareStatement("SELECT id,material,price from tombstone");
            ResultSet rs = prepstate.executeQuery();

            rs.next();
            assertThat(rs.getString("material"),is("Granit"));
            assertThat(rs.getInt("price"),is(1000));
            rs.next();
            assertThat(rs.getString("material"),is("Mamor"));
            assertThat(rs.getInt("price"),is(2500));
            rs.next();
            assertThat(rs.getString("material"),is("Kalkstein"));
            assertThat(rs.getInt("price"),is(1500));

        }catch (SQLException e){
            e.printStackTrace();
        }
    }
    @Test
    public void metaDataTest()
    {
        String catalog = null;
        String shemaPattern= null;
        String columnNamePattern= null;
        String[] types= null;
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            ResultSet rs =databaseMetaData.getTables(catalog,shemaPattern,"GRAVE",types);
            rs.next();
            assertThat(rs.getString(3),is("GRAVE"));
            rs = databaseMetaData.getTables(catalog,shemaPattern,"GRAVEYARDKEEPER",types);
            rs.next();
            assertThat(rs.getString(3),is("GRAVEYARDKEEPER"));
            rs =databaseMetaData.getTables(catalog,shemaPattern,"TOMBSTONE",types);
            rs.next();
            assertThat(rs.getString(3),is("TOMBSTONE"));

            rs = databaseMetaData.getColumns(catalog,shemaPattern,"GRAVE",columnNamePattern);
            rs.next();
            assertThat(rs.getString(4),is("ID"));
            assertThat(rs.getInt(5),is(Types.INTEGER));
            rs.next();
            assertThat(rs.getString(4),is("FAMILYNAME"));
            assertThat(rs.getInt(5),is(Types.VARCHAR));
            rs.next();
            assertThat(rs.getString(4),is("PRICE"));
            assertThat(rs.getInt(5),is(Types.INTEGER));

            rs = databaseMetaData.getColumns(catalog,shemaPattern,"GRAVEYARDKEEPER",columnNamePattern);
            rs.next();
            assertThat(rs.getString(4),is("ID"));
            assertThat(rs.getInt(5),is(Types.INTEGER));
            rs.next();
            assertThat(rs.getString(4),is("FAMILYNAME"));
            assertThat(rs.getInt(5),is(Types.VARCHAR));
            rs.next();
            assertThat(rs.getString(4),is("FIRSTNAME"));
            assertThat(rs.getInt(5),is(Types.VARCHAR));
            rs.next();
            assertThat(rs.getString(4),is("SALARY"));
            assertThat(rs.getInt(5),is(Types.INTEGER));

            rs = databaseMetaData.getColumns(catalog,shemaPattern,"TOMBSTONE",columnNamePattern);
            rs.next();
            assertThat(rs.getString(4),is("ID"));
            assertThat(rs.getInt(5),is(Types.INTEGER));
            rs.next();
            assertThat(rs.getString(4),is("MATERIAL"));
            assertThat(rs.getInt(5),is(Types.VARCHAR));
            rs.next();
            assertThat(rs.getString(4),is("PRICE"));
            assertThat(rs.getInt(5),is(Types.INTEGER));


            rs = databaseMetaData.getPrimaryKeys(catalog,shemaPattern,"GRAVE");
            rs.next();
            assertThat(rs.getString(4),is("ID"));

            rs = databaseMetaData.getPrimaryKeys(catalog,shemaPattern,"GRAVEYARDKEEPER");
            rs.next();
            assertThat(rs.getString(4),is("ID"));

            rs = databaseMetaData.getPrimaryKeys(catalog,shemaPattern,"TOMBSTONE");
            rs.next();
            assertThat(rs.getString(4),is("ID"));
        }catch (SQLException e){
            e.printStackTrace();
        }

    }
}
