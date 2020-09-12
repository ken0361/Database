import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Stack;

class Database
{
    String name;
    HashMap<String, Table> tbList = new HashMap<>();

    public Database(String name)
    {
        this.name = name;
    }

    public void createTable(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split(" ");
        String tbName = splitLineBySpace[2];
        String[] splitLineByBrackets = line.split("(\\()|(\\))");
        String columns = splitLineByBrackets[1];

        Table tb = new Table(tbName, "id, " + columns);
        this.tbList.put(tbName, tb);

        createFile(tbName, columns);

        out.write("OK\n\4\n");
    }

    public void createFile(String tbName, String columns) throws IOException
    {
        File fileToOpen = new File("output/" + this.name + File.separator + tbName+".txt");
        FileWriter writer = new FileWriter(fileToOpen);
        writer.write("id, " + columns + "\n");
        writer.flush();
        writer.close();
    }

    public void insertValues(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineByBrackets = line.split("(\\()|(\\))");
        String[] splitLineBySpace = line.split(" ");
        String tb = splitLineBySpace[2];
        String values = splitLineByBrackets[1];

        if(tbList.get(tb) == null)
        {
            out.write("ERROR Unknown table '" + tb + "'\n\4\n");
        }
        else
        {
            tbList.get(tb).createRow(tbList.get(tb).rowId + ", " + values.replaceAll("'", ""));

            writeFile(tb, values);

            tbList.get(tb).rowId++;

            out.write("OK\n\4\n");
        }
    }

    public void writeFile(String tbName, String values) throws IOException
    {
        File fileToOpen = new File("output/" + this.name + File.separator + tbName+".txt");
        FileWriter writer = new FileWriter(fileToOpen, true);
        writer.write(tbList.get(tbName).rowId + ", " + values.replaceAll("'", "") + "\n");
        writer.flush();
        writer.close();
    }

    public void printTable(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split(" ");
        int fromPlace = -1;
        String[] columnChosen = line.split("(SELECT)|(FROM)");

        /*Find FROM and column wanted.*/
        for(int i=0; i<splitLineBySpace.length; i++)
        {
            if(splitLineBySpace[i].equals("FROM"))
            {
                fromPlace = i;
                break;
            }
        }

        Table tb = this.tbList.get(splitLineBySpace[fromPlace+1]);

        if(tb == null)
        {
            out.write("ERROR: Unknown table '" + splitLineBySpace[fromPlace+1] + "'\n\4\n");
        }
        else if(fromPlace+2 != splitLineBySpace.length) /*command has something after tb which is error*/
        {
            out.write("ERROR: Invalid query\n\4\n");
        }
        else if(splitLineBySpace[1].equals("*"))
        {
            /*print column*/
            printInFormat(out, tb.columns);

            /*print row*/
            for(int i=0; i<tb.rows.size(); i++)
            {
                printInFormat(out, tb.rows.get(i));
            }
            out.write("\n\4\n");
        }
        else if(columnChosen[1].split(" ").length != columnChosen[1].split(", ").length + 1)
        {
            /*command didn't split columns by ,*/
            out.write("ERROR: Invalid query\n\4\n");
        }
        else /*print selected column*/
        {
            String[] allColumn = tb.columns.split(", ");
            String newColumn = "";

            for (String s : allColumn)
            {
                if (columnChosen[1].contains(s)) {
                    newColumn = newColumn.concat(s + ", ");
                }
            }

            if(newColumn.equals(""))
            {
                out.write("ERROR: Unknown attributes\n\4\n");
            }
            else
            {
                printInFormat(out, newColumn);

                for(int i=0; i<tb.rows.size(); i++)
                {
                    String[] eachRow = tb.rows.get(i).split(", ");
                    String newRow = "";
                    for(int j=0; j<allColumn.length; j++)
                    {
                        if(columnChosen[1].contains(allColumn[j]))
                        {
                            newRow = newRow.concat(eachRow[j] + ", ");
                        }
                    }

                    printInFormat(out, newRow);
                }

                out.write("\n\4\n");
            }
        }
    }

    public boolean judgeCondition(String operator, String rowValue, String[][] rows, int columnNum, int i)
    {
        rowValue = rowValue.replaceAll("'", "");

        if(operator.equals("==") && rows[i][columnNum].equals(rowValue))
        {
            return true;
        }
        else if(operator.equals("!=") && !rows[i][columnNum].equals(rowValue))
        {
            return true;
        }
        else if(operator.equals("<") && Float.parseFloat(rows[i][columnNum]) < Float.parseFloat(rowValue))
        {
            return true;
        }
        else if(operator.equals(">") && Float.parseFloat(rows[i][columnNum]) > Float.parseFloat(rowValue))
        {
            return true;
        }
        else if(operator.equals("<=") && (rows[i][columnNum].equals(rowValue) || Float.parseFloat(rows[i][columnNum]) < Float.parseFloat(rowValue)))
        {
            return true;
        }
        else if(operator.equals(">=") && (rows[i][columnNum].equals(rowValue) || Float.parseFloat(rows[i][columnNum]) > Float.parseFloat(rowValue)))
        {
            return true;
        }
        else if(operator.equals("LIKE") && rows[i][columnNum].contains(rowValue))
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean isValidOperator(String operator)
    {
        return operator.equals("==") || operator.equals(">") || operator.equals("<") ||
                operator.equals(">=") || operator.equals("<=") || operator.equals("!=") ||
                operator.equals("LIKE");
    }

    public void deleteValues(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split("'?( |$)(?=(([^']*'){2})*[^']*$)'?");
        String tbName = splitLineBySpace[2];
        String operator = splitLineBySpace[5];
        String columnName = splitLineBySpace[4];
        String rowValue = splitLineBySpace[6].replaceAll("'", "");
        int columnNum = -1;

        if(tbList.get(tbName) == null)
        {
            out.write("ERROR Unknown table '" + tbName + "'\n\4\n");
        }
        else if(!isValidOperator(operator))
        {
            out.write("ERROR incorrect operator\n\4\n");
        }
        else
        {
            String[] columns = tbList.get(tbName).columns.split(", ");
            String[][] rows = new String[tbList.get(tbName).rows.size()][];

            /*Find the column*/
            for(int i=0; i<columns.length; i++)
            {
                if(columnName.equals(columns[i]))
                {
                    columnNum = i;
                    break;
                }
            }

            /*No need to process*/
            if(columnNum == -1)
            {
                out.write("ERROR Unknown column '" + columnName + "'\n\4\n");
            }

            /*Split the row*/
            for(int i=0; i<tbList.get(tbName).rows.size(); i++)
            {
                rows[i] = tbList.get(tbName).rows.get(i).split(", ");
            }


            /*Dealing with Judgment*/
            int NewRowSize = tbList.get(tbName).rows.size();
            int originalRowSize = tbList.get(tbName).rows.size();

            for(int i=0, j=0; i<NewRowSize && j<originalRowSize; i++, j++)
            {
                if(judgeCondition(operator, rowValue, rows, columnNum, j))
                {
                    tbList.get(tbName).removeRow(i);
                    NewRowSize--;
                    i--;
                }
            }

            reWriteFile(tbName);

            out.write("OK\n\4\n");
        }
    }

    public void reWriteFile(String tbName) throws IOException
    {
        Table tb = tbList.get(tbName);

        File fileToOpen = new File("output/" + this.name + File.separator + tbName+".txt");
        FileWriter writer = new FileWriter(fileToOpen);

        writer.write(tb.columns + "\n");

        for(int i=0; i<tb.rows.size(); i++)
        {
            writer.write(tb.rows.get(i).replaceAll("'", "") + "\n");
        }

        writer.flush();
        writer.close();
    }

    public void updateTable(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split("'?( |$)(?=(([^']*'){2})*[^']*$)'?");
        Table tb = this.tbList.get(splitLineBySpace[1]);
        String columnName = splitLineBySpace[7];
        String targetColumn = splitLineBySpace[3];
        String targetValue = splitLineBySpace[5];
        String rowValue = splitLineBySpace[9].replaceAll("'", "");

        int columnNum = -1;
        int rowNum = -1;
        int targetNum = -1;

        if(tb == null)
        {
            out.write("ERROR Unknown table '" + splitLineBySpace[1] + "'\n\4\n");
        }
        else
        {
            String[] columns = tb.columns.split(", ");
            String[][] rows = new String[tb.rows.size()][];

            /*Find the column*/
            for(int i=0; i<columns.length; i++)
            {
                if(columnName.equals(columns[i]))
                {
                    columnNum = i;
                }
                if(targetColumn.equals(columns[i]))
                {
                    targetNum = i;
                }
            }

            /*No need to process*/
            if(columnNum == -1)
            {
                out.write("ERROR Unknown column '" + columnName + "'\n\4\n");
            }
            else if(targetNum == -1)
            {
                out.write("ERROR Unknown column '" + targetColumn + "'\n\4\n");
            }
            else
            {
                /*Split the row*/
                for(int i=0; i<tb.rows.size(); i++)
                {
                    rows[i] = tb.rows.get(i).split(", ");
                }

                /*Find the row*/
                for(int i=0; i<rows.length; i++)
                {
                    if (rows[i][columnNum].equals(rowValue))
                    {
                        rowNum = i;
                        break;
                    }
                }

                /*No need to process*/
                if(rowNum == -1)
                {
                    out.write("\n\4\n");
                }
                else
                {
                    /*Update to rows*/
                    rows[rowNum][targetNum] = targetValue;

                    /*Update to table*/
                    tb.rows.set(rowNum, String.join(", ", rows[rowNum]));

                    reWriteFile(splitLineBySpace[1]);

                    out.write("OK\n\4\n");
                }
            }
        }
    }

    public void dropTable(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split(" ");
        String tbName = splitLineBySpace[2];

        File table = new File("output/" + this.name + File.separator + tbName+".txt");
        this.tbList.remove(tbName);

        if(table.delete())
        {
            out.write("OK\n\4\n");
        }
        else
        {
            out.write("Table '" + tbName + "' do no exist\n\4\n");
        }
    }

    public void addDropColumn(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split(" ");
        Table tb = this.tbList.get(splitLineBySpace[2]);

        if(tb == null)
        {
            out.write("Table '" + splitLineBySpace[2] + "' do no exist\n\4\n");
        }
        else if(splitLineBySpace[3].equals("ADD"))
        {
            tb.columns = tb.columns.concat(", " + splitLineBySpace[4]);

            for(int i=0; i<tb.rows.size(); i++)
            {
                String row = tb.rows.get(i).concat(", " + " ");
                tb.rows.set(i, row);
            }

            reWriteFile(splitLineBySpace[2]);
            out.write("OK\n\4\n");
        }
        else if(splitLineBySpace[3].equals("DROP"))
        {
            String[] allColumn = tb.columns.split(", ");
            int columnNum = -1;

            /*find the column num to delete*/
            for(int i=0; i<tb.columns.length(); i++)
            {
                if(allColumn[i].equals(splitLineBySpace[4]))
                {
                    columnNum = i;
                    break;
                }
            }

            if(columnNum == -1)
            {
                out.write("ERROR Unknown column '" + splitLineBySpace[4] + "'\n\4\n");
            }
            else
            {
                tb.columns = tb.columns.replaceAll(", " + splitLineBySpace[4], "");

                for(int i=0; i<tb.rows.size(); i++)
                {
                    String[] allRow = tb.rows.get(i).split(", ");
                    String row = tb.rows.get(i).replaceAll(", " + allRow[columnNum], "");
                    tb.rows.set(i, row);
                }
                reWriteFile(splitLineBySpace[2]);
                out.write("OK\n\4\n");
            }
        }
        else
        {
            out.write("ERROR: Invalid query\n\4\n");
        }
    }

    public void printInFormat(BufferedWriter out, String string) throws IOException
    {
        String[] strings = string.split(", ");

        for (String s : strings)
        {
            out.write(String.format("%-18s", s.replaceAll("(')|(,)", "")) + " ");
        }
        out.write("\n");
    }

    public void joinTables(BufferedWriter out, String line)throws IOException
    {
        String[] splitLineBySpace = line.split(" ");
        String tableName1 = splitLineBySpace[1];
        String tableName2 = splitLineBySpace[3];
        String columnName1 = splitLineBySpace[5];
        String columnName2 = splitLineBySpace[7];
        Table tb1 = this.tbList.get(tableName1);
        Table tb2 = this.tbList.get(tableName2);

        if(tb1 == null)
        {
            out.write("Table '" + tableName1 + "' do no exist\n\4\n");
        }
        else if(tb2 == null)
        {
            out.write("Table '" + tableName2 + "' do no exist\n\4\n");
        }
        else
        {
            String newColumn;
            String newColumnPart1 = "";
            String newColumnPart2 = "";
            String[] tb1Columns;
            String[] tb2Columns;
            int joinBasedColumn1 = -1;
            int joinBasedColumn2 = -1;

            /*print column*/
            /*deal with tb1*/
            tb1Columns = tb1.columns.split(", ");
            for(int i=0; i<tb1Columns.length; i++)
            {
                /*to avoid column id*/
                if(i > 0)
                {
                    newColumnPart1 = newColumnPart1.concat(tableName1 + "." + tb1Columns[i]);
                    if(i+1 < tb1Columns.length)
                    {
                        newColumnPart1 = newColumnPart1 + ", ";
                    }
                }
                if(columnName1.equals(tb1Columns[i]))
                {
                    joinBasedColumn1 = i;
                }
            }

            /*deal with tb2*/
            tb2Columns = tb2.columns.split(", ");
            for(int i=0; i<tb2Columns.length; i++)
            {
                /*to avoid column id*/
                if(i > 0)
                {
                    newColumnPart2 = newColumnPart2.concat(tableName2 + "." + tb2Columns[i]);
                    if(i+1 < tb2Columns.length)
                    {
                        newColumnPart2 = newColumnPart2 + ", ";
                    }
                }
                if(columnName2.equals(tb2Columns[i]))
                {
                    joinBasedColumn2 = i;
                }
            }


            newColumn = "id, " + newColumnPart1 + ", " + newColumnPart2;
            printInFormat(out, newColumn);

            if(joinBasedColumn1 == -1)
            {
                out.write("ERROR Unknown column '" + columnName1 + "'\n\4\n");
            }
            else if(joinBasedColumn2 ==-1)
            {
                out.write("ERROR Unknown column '" + columnName2 + "'\n\4\n");
            }
            else
            {
                /*print row*/
                String[][] tb1Rows = new String[tb1.rows.size()][];
                String[][] tb2Rows = new String[tb2.rows.size()][];
                int idNum = 1;

                /*Split the row*/
                for(int i=0; i<tb1.rows.size(); i++)
                {
                    tb1Rows[i] = tb1.rows.get(i).split(", ");
                }
                for(int i=0; i<tb2.rows.size(); i++)
                {
                    tb2Rows[i] = tb2.rows.get(i).split(", ");
                }

                /*find and print rows*/
                for(int i=0; i<tb1Rows.length; i++)
                {
                    for(int j=0; j<tb2Rows.length; j++)
                    {
                        if(tb1Rows[i][joinBasedColumn1].equals(tb2Rows[j][joinBasedColumn2]))
                        {
                            String newRow = idNum + ", " + tb1.rows.get(i).substring(3) + ", " +tb2.rows.get(j).substring(3);
                            printInFormat(out, newRow);
                            idNum++;
                        }
                    }
                }

                out.write("\n\4\n");
            }
        }
    }

    public void printMultiConditionTable(BufferedWriter out, String line) throws IOException
    {
        String[] splitLineBySpace = line.split(" ");
        String[] splitLineByWHERE = line.split("WHERE ");
        Table tb = null;
        int tbPlaceNum = -1;
        boolean printAllColumn = splitLineBySpace[1].equals("*");

        String condition = splitLineByWHERE[1];
        condition = "(" + condition + ")";
        condition = condition.replaceAll("\\(", "\\( ");
        condition = condition.replaceAll("\\)", " \\)");
        String[] conditions = condition.split("'?( |$)(?=(([^']*'){2})*[^']*$)'?");
        Stack<String> ops  = new Stack<>();
        Stack<String> values = new Stack<>();

        /*find FROM in line*/
        for(int i=0; i<splitLineBySpace.length; i++)
        {
            if(splitLineBySpace[i].equals("FROM"))
            {
                tbPlaceNum = i+1;
                tb = this.tbList.get(splitLineBySpace[tbPlaceNum]);
                break;
            }
        }

        if(tb == null)
        {
            out.write("ERROR: Unknown table '" + splitLineBySpace[3] + "'\n\4\n");
        }
        else if(hasIllegalCondition(line, tb, tbPlaceNum)) /*compare different type of value*/
        {
            out.write("ERROR: Compare incorrect type of attributes \n\4\n");
        }
        else
        {
            String[] columns = tb.columns.split(", ");
            String[][] rows = new String[tb.rows.size()][];
            for(int i=0; i<tb.rows.size(); i++)
            {
                rows[i] = tb.rows.get(i).split(", ");
            }

            /*print the column*/
            if(printAllColumn)
            {
                printInFormat(out, tb.columns);
            }
            else /*print selected column*/
            {
                String[] columnChosen = line.split("(SELECT)|(FROM)");
                String[] allColumn = tb.columns.split(", ");
                String newColumn = "";

                for (String s : allColumn)
                {
                    if (columnChosen[1].contains(s))
                    {
                        newColumn = newColumn.concat(s + ", ");
                    }
                }

                printInFormat(out, newColumn);
            }

            /*parsing the condition and print rows*/
            for(int j=0; j<tb.rows.size(); j++)
            {
                for(int i=0; i<conditions.length; i++)
                {
                    if(conditions[i].equals("(")) ;
                    else if(conditions[i].equals("==")) ops.push(conditions[i]);
                    else if(conditions[i].equals("!=")) ops.push(conditions[i]);
                    else if(conditions[i].equals(">")) ops.push(conditions[i]);
                    else if(conditions[i].equals("<")) ops.push(conditions[i]);
                    else if(conditions[i].equals(">=")) ops.push(conditions[i]);
                    else if(conditions[i].equals("<=")) ops.push(conditions[i]);
                    else if(conditions[i].equals("LIKE")) ops.push(conditions[i]);
                    else if(conditions[i].equals("AND")) ops.push(conditions[i]);
                    else if(conditions[i].equals("OR")) ops.push(conditions[i]);
                    else if(conditions[i].equals(")"))
                    {
                        String op = ops.pop();
                        String value = values.pop();
                        String previousValue = values.pop();

                        int columnNum = -1;

                        if(op.equals("AND"))
                        {
                            if(value.equals("TRUE") && previousValue.equals("TRUE"))
                            {
                                value = "TRUE";
                            }
                            else
                            {
                                value = "FALSE";
                            }
                        }
                        else if(op.equals("OR"))
                        {
                            if(value.equals("FALSE") && previousValue.equals("FALSE"))
                            {
                                value = "FALSE";
                            }
                            else
                            {
                                value = "TRUE";
                            }
                        }
                        else
                        {
                            for(int k=0; k<columns.length; k++)
                            {
                                if(previousValue.equals(columns[k]))
                                {
                                    columnNum = k;
                                    break;
                                }
                            }

                            if(columnNum == -1)
                            {
                                out.write("ERROR Unknown column\n\4\n");
                            }
                            else
                            {
                                if(judgeCondition(op, value, rows, columnNum, j))
                                {
                                    value = "TRUE";
                                }
                                else
                                {
                                    value = "FALSE";
                                }
                            }
                        }
                        values.push(value);
                    }
                    else
                    {
                        values.push(conditions[i].replaceAll("'", ""));
                    }
                }

                if(values.pop().equals("TRUE"))
                {
                    if(printAllColumn)
                    {
                        printInFormat(out, tb.rows.get(j));
                    }
                    else
                    {
                        String[] columnChosen = line.split("(SELECT)|(FROM)");
                        String[] eachRow = tb.rows.get(j).split(", ");
                        String[] allColumn = tb.columns.split(", ");
                        String newRow = "";

                        for(int m=0; m<allColumn.length; m++)
                        {
                            if(columnChosen[1].contains(allColumn[m]))
                            {
                                newRow = newRow.concat(eachRow[m] + ", ");
                            }
                        }

                        printInFormat(out, newRow);
                    }
                }
            }
            out.write("\n\4\n");
        }
    }

    public boolean isNumeric(String s)
    {
        return s.matches("-?\\d+(\\.\\d+)?");
    }

    public boolean hasIllegalCondition(String line, Table tb, int tbPlaceNum)
    {
        String[] allStringInCommand = line.split("'?( |$)(?=(([^']*'){2})*[^']*$)'?");
        String[] columns = tb.columns.split(", ");
        String[] rows = tb.rows.get(0).split(", ");;

        for(int i=tbPlaceNum; i<allStringInCommand.length; i++)
        {
            if(isValidOperator(allStringInCommand[i]))
            {
                for(int j=0; j<columns.length; j++)
                {
                    if(allStringInCommand[i-1].equals(columns[j]))
                    {
                        if(isNumeric(rows[j])^isNumeric(allStringInCommand[i+1]))
                        {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}