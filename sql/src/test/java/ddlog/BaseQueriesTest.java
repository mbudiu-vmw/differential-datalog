package ddlog;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import org.h2.store.fs.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;

public class BaseQueriesTest {
    // TODO: this should only be done once, but it is not clear how this can be achieved.
    @BeforeClass
    public static void createLibrary() throws FileNotFoundException {
        Translator t = new Translator(null);
        DDlogProgram lib = t.generateSqlLibrary();
        System.out.println("Current directory " + System.getProperty("user.dir"));
        lib.toFile("lib/sqlop.dl");
    }
    
    // These strings are part of almost all expected outputs
    protected final String imports = "import fp\n" +
            "import time\n" +
            "import sql\n" +
            "import sqlop\n";
    protected final String tables =
            "typedef Tt1 = Tt1{column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
            "typedef Tt2 = Tt2{column1:signed<64>}\n" +
            "typedef Tt3 = Tt3{d:Date, t:Time, dt:DateTime}\n";
    protected final String tablesWNull =
            "typedef Tt1 = Tt1{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column4:Option<double>}\n" +
            "typedef Tt2 = Tt2{column1:Option<signed<64>>}\n" +
            "typedef Tt3 = Tt3{d:Option<Date>, t:Option<Time>, dt:Option<DateTime>}\n";

    /**
     * The expected string the generated program starts with.
     * @param withNull  True if the tables can contain nulls.
     */
    protected String header(boolean withNull) {
        if (withNull)
            return this.imports + "\n" + this.tablesWNull;
        return this.imports + "\n" + this.tables;
    }

    /**
     * The expected string for the declared relations
     */
    @SuppressWarnings("unused")
    protected String relations(boolean withNull) {
        return "\n" +
            "input relation Rt1[Tt1]\n" +
            "input relation Rt2[Tt2]\n" +
            "input relation Rt3[Tt3]\n";
    }

    protected Translator createInputTables(boolean withNulls) {
        String nulls = withNulls ? "" : " not null";
        String createStatement = "create table t1(column1 integer " + nulls + ",\n" +
                " column2 varchar(36) " + nulls + ",\n" +
                " column3 boolean " + nulls + ",\n" +
                " column4 real " + nulls + ")";
        Translator t = new Translator(null);
        DDlogIRNode create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        String s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt1[Tt1]", s);

        createStatement = "create table t2(column1 integer " + nulls + ")";
        create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt2[Tt2]", s);

        createStatement = "create table t3(d date " + nulls + ",\n" +
                " t time " + nulls + ",\n" +
                " dt datetime " + nulls + ")";
        create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt3[Tt3]", s);

        return t;
    }

    /**
     * Compile the DDlog program given as a string.
     * @param programBody  Program to compile.
     */
    protected void compiledDDlog(String programBody) {
        try {
            File tmp = this.writeProgramToFile(programBody);
            boolean success = DDlogAPI.compileDDlogProgramToRust(tmp.getName(), true,"../lib", "./lib");
            if (!success) {
                String[] lines = programBody.split("\n");
                for (int i = 0; i < lines.length; i++) {
                    System.out.print(String.format("%3s ", i+1));
                    System.out.println(lines[i]);
                }
            }

            String basename = tmp.getName();
            basename = basename.substring(0, basename.lastIndexOf('.'));
            String tempDir = System.getProperty("java.io.tmpdir");
            String dir = tempDir + "/" + basename + "_ddlog";
            FileUtils.deleteRecursive(dir, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void testTranslation(String query, String program, boolean withNulls) {
        Translator t = this.createInputTables(withNulls);
        DDlogIRNode view = t.translateSqlStatement(query);
        Assert.assertNotNull(view);
        String s = view.toString();
        Assert.assertNotNull(s);
        DDlogProgram ddprogram = t.getDDlogProgram();
        Assert.assertNotNull(ddprogram);
        s = ddprogram.toString();
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    protected void testTranslation(String query, String program) {
        this.testTranslation(query, program, false);
    }

    public File writeProgramToFile(String programBody) throws IOException {
        File tmp = new File("program.dl");
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(programBody);
        bw.close();
        return tmp;
    }

    /**
     * Compile the specified file from the resources folder.
     */
    public void testFileCompilation(String file) {
        final InputStream resourceAsStream = DynamicTest.class.getResourceAsStream(file);
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
                    .forEach(t::translateSqlStatement);
            final DDlogProgram dDlogProgram = t.getDDlogProgram();
            final String ddlogProgramAsString = dDlogProgram.toString();
            File tmp = this.writeProgramToFile(ddlogProgramAsString);
            boolean success = DDlogAPI.compileDDlogProgram(tmp.toString(), false,"..", "lib");
            Assert.assertTrue(success);
        } catch (IOException | DDlogException e) {
            throw new RuntimeException(e);
        }
    }
}