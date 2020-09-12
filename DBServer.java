import java.io.*;
import java.net.*;

class DBServer
{
    public static void main(String args[])
    {
        new DBServer(8888);
    }

    public DBServer(int portNumber)
    {
        try {
            ServerSocket ss = new ServerSocket(portNumber);
            System.out.println("Server Listening");
            acceptConnection(ss);
        } catch(IOException ioe) {
            System.err.println(ioe);
        }
    }

    private void acceptConnection(ServerSocket ss) throws IOException
    {
            Socket socket = ss.accept();
            Controller controller = new Controller();
            while(true)
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                processNextCommand(in, out, controller);
                out.flush();
            }

    }

    private void processNextCommand(BufferedReader in, BufferedWriter out, Controller controller) throws IOException
    {
        String line = in.readLine();
        if(line.length()>0)
        {
            line = line.trim();
            Command command = new Command(line);

            if(command.doesNotEndWith(";"))
            {
                out.write("ERROR: Missing ;" + "\n\4\n");
            }
            else if(command.doesNotContainsEvenQuotes(line))
            {
                out.write("ERROR: Invalid query\n\4\n");
            }
            else
            {
                line = line.substring(0, line.length()-1);
                controller.getOutAndLine(out, line);

                if(command.startsWith("CREATE DATABASE"))
                {
                    controller.createDatabase();
                }
                else if(command.startsWith("DROP DATABASE"))
                {
                    controller.dropDatabase();
                }
                else if(command.startsWith("USE"))
                {
                    controller.setDatabase();
                }
                else if(controller.noDatabaseIsUsing())
                {
                    out.write("ERROR No database is using\n\4\n");
                }
                else if(command.startsWith("DROP TABLE"))
                {
                    controller.database.dropTable(out, line);
                }
                else if(command.startsWith("CREATE TABLE"))
                {
                    controller.database.createTable(out, line);
                }
                else if(command.startsWith("INSERT INTO") && command.contains("VALUES"))
                {
                    controller.database.insertValues(out, line);
                }
                else if(command.startsWith("SELECT") && command.contains("FROM"))
                {
                    if(command.contains("WHERE"))
                    {
                        controller.database.printMultiConditionTable(out, line);
                    }
                    else
                    {
                        controller.database.printTable(out, line);
                    }
                }
                else if(command.startsWith("DELETE FROM") && command.contains("WHERE"))
                {
                    controller.database.deleteValues(out, line);
                }
                else if(command.startsWith("UPDATE"))
                {
                    controller.database.updateTable(out, line);
                }
                else if(command.startsWith("ALTER"))
                {
                    controller.database.addDropColumn(out, line);
                }
                else if(command.startsWith("JOIN"))
                {
                    controller.database.joinTables(out, line);
                }
                else
                {
                    out.write("ERROR: Invalid query\n\4\n");
                }
            }
        }
        else
        {
            out.write("\4\n");
        }

    }
}
