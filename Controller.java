import java.io.*;
import java.util.HashMap;

class Controller
{
    BufferedWriter out;
    String line;
    Database database = null;
    HashMap<String, Database> dbList = new HashMap<>();

    public Controller()
    {
        //new File("output").mkdirs();

        File output = new File("output");
        if(output.exists())
        {
            readExistDbAndTb(output);
        }
        else
        {
            output.mkdirs();
        }
    }

    public void readExistDbAndTb(File output)
    {
        String[] databases = output.list();

        if(databases == null)
        {
            return;
        }

        for(int i = 0; i<databases.length; i++)
        {
            Database db = new Database(databases[i]);
            this.dbList.put(databases[i], db);

            File path = new File("output/" + databases[i]);
            String[] tables = path.list();

            if(tables == null)
            {
                continue;
            }

            for(int j=0; j<tables.length; j++)
            {
                Table tb;
                BufferedReader reader;
                tables[j] = tables[j].replaceAll(".txt", "");

                try
                {
                    reader = new BufferedReader(new FileReader("output/" + databases[i] + File.separator + tables[j] + ".txt"));
                    String line = reader.readLine();

                    tb = new Table(tables[j], line);
                    this.dbList.get(databases[i]).tbList.put(tables[j], tb);
                    line = reader.readLine();

                    while (line != null)
                    {
                        this.dbList.get(databases[i]).tbList.get(tables[j]).createRow(line);
                        line = reader.readLine();
                    }
                    reader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    public void getOutAndLine(BufferedWriter out, String line)
    {
        this.out = out;
        this.line = line;
    }

    public void createDatabase() throws IOException
    {
        String[] splitLineBySpace = this.line.split(" ");
        String dbName = splitLineBySpace[2];

        new File("output/" + dbName).mkdirs();
        Database db = new Database(dbName);
        this.dbList.put(dbName, db);
        out.write("OK\n\4\n");
    }

    public void setDatabase() throws IOException
    {
        String[] splitLineBySpace = this.line.split(" ");
        String dbName = splitLineBySpace[1];

        if(this.dbList.get(dbName) == null)
        {
            out.write("ERROR Unknown database '" + dbName + "'\n\4\n");
        }
        else
        {
            this.database = this.dbList.get(dbName);
            out.write("OK\n\4\n");
        }
    }

    public void dropDatabase() throws IOException
    {
        String[] splitLineBySpace = this.line.split(" ");
        String dbName = splitLineBySpace[2];
        File folder = new File("output/" + dbName);
        this.dbList.remove(dbName);

        String[] tables = folder.list();
        if(tables != null)
        {
            for(int i=0; i<tables.length; i++)
            {
                File currentFile = new File(folder.getPath(), tables[i]);
                currentFile.delete();
            }
        }

        if(folder.delete())
        {
            out.write("OK\n\4\n");
        }
        else
        {
            out.write("Database '" + dbName + "' do no exist\n\4\n");
        }
    }

    public boolean noDatabaseIsUsing()
    {
        return this.database == null;
    }
}