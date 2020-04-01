package ddlog;

import com.google.common.base.Splitter;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import ddlogapi.*;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class DynamicTest {
    /*
     * Sets up an H2 in-memory database that we use for all tests.
     */
    private DSLContext setup() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            conn.setSchema("PUBLIC");
            return DSL.using(conn, SQLDialect.H2);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDynamic() {
        final DSLContext conn = setup();
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";
        conn.execute(createStatement);
        conn.execute(viewStatement);
    }

    @Test
    public void testDynamicLoading() throws IOException, DDlogException, IllegalAccessException, NoSuchFieldException {
        String ddlogProgram = "input relation R(v: bit<16>)\n" +
                "output relation O(v: bit<16>)\n" +
                "O(v) :- R(v).";
        String filename = "program.dl";
        File file = new File(filename);
        file.deleteOnExit();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write(ddlogProgram);
        bw.close();
        DDlogAPI api = Translator.compileAndLoad(file.getName());
        if (api == null)
            throw new RuntimeException("Could not load program");

        DDlogRecord field = new DDlogRecord(10);
        DDlogRecord[] fields = { field };
        DDlogRecord record = DDlogRecord.makeStruct("R", fields);
        int id = api.getTableId("R");
        DDlogRecCommand command = new DDlogRecCommand(
                DDlogCommand.Kind.Insert, id, record);
        DDlogRecCommand[] ca = new DDlogRecCommand[1];
        ca[0] = command;

        System.err.println("Executing " + command.toString());
        api.transactionStart();
        api.applyUpdates(ca);
        api.transactionCommitDumpChanges(s -> Assert.assertEquals("From 0 Insert O{10}", s.toString()));
    }

    @Test
    public void testFullSchemaCompilation() {
        final InputStream resourceAsStream = DynamicTest.class.getResourceAsStream("/test_tables.sql");
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final Translator t = new Translator(null);
            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);
            semiColonSeparated // remove SQL comments
                    .forEach(s -> {
                        System.out.println(s);
                        t.translateSqlStatement(s);
                    });
            final DDlogProgram dDlogProgram = t.getDDlogProgram();
            final String ddlogProgramAsString = dDlogProgram.toString();
            final String filename = "program.dl";
            final Path path = Files.write(Paths.get(filename), ddlogProgramAsString.getBytes());
            path.toFile().deleteOnExit();
            try {
                final DDlogAPI dDlogAPI = Translator.compileAndLoad(filename, "..", "../sql/lib/");
                Assert.assertNotNull(dDlogAPI);
            } catch (DDlogException | NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
