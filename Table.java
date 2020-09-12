import java.util.ArrayList;

class Table
{
    String name;
    String columns;
    int rowId;
    ArrayList<String> rows = new ArrayList();

    public Table(String name, String columns)
    {
        this.name = name;
        this.columns = columns;
        this.rowId = 1;
    }

    public void createRow(String row)
    {
        this.rows.add(row);
    }

    public void removeRow(int num)
    {
        this.rows.remove(num);
    }

}