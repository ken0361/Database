import java.io.*;

class Command
{
    String line;

    public Command(String line) throws IOException
    {
        this.line = line;
    }

    public boolean doesNotEndWith(String s)
    {
        if(line.length() == 0 || this.line.substring(line.length() - 1).equals(s))
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    public boolean startsWith(String s)
    {
        String[] commands = this.line.split(" ");
        String[] startStrings = s.split(" ");

        for(int i=0; i<startStrings.length; i++)
        {
            if(!commands[i].equals(startStrings[i]))
            {
                return false;
            }
        }

        return true;
    }

    public boolean contains(String s)
    {
        return this.line.contains(s);
    }

    public boolean doesNotContainsEvenQuotes(String s)
    {
        long num = s.chars().filter(ch -> ch == '\'').count();
        return num % 2 == 1;
    }
}