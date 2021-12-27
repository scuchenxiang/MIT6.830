package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */

public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    private List<Field> tuple;//table每行存的数据
    private TupleDesc tupleDesc;
    private RecordId recordId=new RecordId(new PageId() {
        @Override
        public int[] serialize() {
            return new int[0];
        }

        @Override
        public int getTableId() {
            return 0;
        }

        @Override
        public int getPageNumber() {
            return 0;
        }
    },0);
    public Tuple(TupleDesc td) {
        tupleDesc=td;
        tuple=new ArrayList<>(Collections.nCopies(td.getSize(),null));
        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if(i>=0&&i<this.tuple.size())
            this.tuple.set(i,f);
        // some code goes here
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if((i>=0)&&(i<this.tuple.size()))
            return this.tuple.get(i);
        else
            return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder res=new StringBuilder("");
        for(int i=0;i<this.tuple.size();i++){
            res.append(this.tuple.get(i).toString());
            if(i!=this.tuple.size()-1)
                res.append(" ");
        }
        return res.toString();
//        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new tupleTtr(this.tuple);
    }
    private class tupleTtr implements Iterator<Field> {
        private List<Field> list;
        private int position=0;

        tupleTtr(List<Field> list)
        {
            this.list=list;
        }


        public boolean hasNext() {
            return position <list.size();
        }


        public Field next() {
            int i = position;
            if (i >= list.size())
                throw new NoSuchElementException();
            Field item=list.get(i);
            position = i + 1;
            return item;
        }
    }
    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupleDesc=td;
        // some code goes here
    }
}
