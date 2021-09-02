package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    private class Table{
        private DbFile dbFile;
        private String name;
        private String pkeyField;
        public Table(DbFile dbFile,String name,String pkeyField)
        {
            this.dbFile=dbFile;
            this.name=name;
            this.pkeyField=pkeyField;
        }
        public DbFile getDbFile()
        {
            return dbFile;
        }
        public String getName()
        {
            return name;
        }
        public String getPkeyField()
        {
            return pkeyField;
        }
    }
    private HashMap<Integer,Table> id2Tb=new HashMap<>();
    private HashMap<String,Integer> name2TbID=new HashMap<>();
    public Catalog() {

        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if(name==null)
            return;
        if(this.name2TbID.containsKey(name))
        {
            Table table=new Table(file,name,pkeyField);
            int id=file.getId();

            id2Tb.remove(this.name2TbID.get(name));
            //添加新的
            id2Tb.put(id,table);
            name2TbID.put(name,id);
        }
        else
        {
            Table table=new Table(file,name,pkeyField);
            int id=file.getId();
            name2TbID.put(name,id);
            id2Tb.put(id,table);
        }
        // some code goes here
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(this.name2TbID.containsKey(name))
            return this.name2TbID.get(name);
        else
        {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if(id2Tb.containsKey(tableid))
        {
            return id2Tb.get(tableid).getDbFile().getTupleDesc();
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if(id2Tb.containsKey(tableid))
        {
            return id2Tb.get(tableid).getDbFile();
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if(id2Tb.containsKey(tableid))
        {
            return id2Tb.get(tableid).getName();
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here

        return new cataLog(this.name2TbID);
    }
    private class cataLog implements Iterator<Integer> {
        private int name;
        private Iterator<HashMap.Entry<String,Integer>> name2TbIDIt;
        public cataLog(HashMap<String,Integer> name2TbID)
        {
            name2TbIDIt=name2TbID.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return name2TbIDIt.hasNext();
        }

        @Override
        public Integer next() {
            return name2TbIDIt.next().getValue();
        }
    }

    public String getTableName(int id) {
        // some code goes here
        if(id2Tb.containsKey(id))
        {
            return id2Tb.get(id).getName();
        }
        else
        {
            return null;
        }
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        id2Tb.clear();
        name2TbID.clear();
        // some code goes here
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

