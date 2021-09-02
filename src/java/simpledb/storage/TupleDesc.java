package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    private List<TDItem> itemList;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {

        // some code goes here
        return itemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        itemList=new ArrayList<>(typeAr.length);
        for(int i=0;i<typeAr.length;i++){
            TDItem tdItem=new TDItem(typeAr[i],fieldAr[i]);
            itemList.add(tdItem);
        }
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr,new String[typeAr.length]);
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return itemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if((i>=this.itemList.size()))
            throw new NoSuchElementException();
        return itemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {

        if((i>=this.itemList.size()))
            throw new NoSuchElementException();
        return itemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i=0;i<itemList.size();i++)
        {
            String name1=itemList.get(i).fieldName;
            if(name1!=null&&itemList.get(i).fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < itemList.size(); i++) {
            size += itemList.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int len1=td1.numFields();
        int len2=td2.numFields();

        Type []fieldtype =new Type[len1+len2];
        String []fieldname=new String[len1+len2];

        for(int i=0;i<td1.numFields();i++)
        {
            fieldname[i]=td1.getFieldName(i);
            fieldtype[i]=td1.getFieldType(i);
        }
        for(int i=0;i<td2.numFields();i++)
        {
            fieldname[i+td1.numFields()]=td2.getFieldName(i);
            fieldtype[i+td1.numFields()]=td2.getFieldType(i);
        }
        // some code goes here
        return new TupleDesc(fieldtype,fieldname);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(!(o instanceof  TupleDesc)||((TupleDesc) o).numFields()!=this.numFields())
            return false;
        TupleDesc newOne=(TupleDesc) o;
        for(int i=0;i<this.numFields();i++)
        {
            if(this.getFieldType(i).equals(newOne.getFieldType(i))&&this.getFieldName(i)==null&&newOne.getFieldName(i)==null)
                continue;
            if(this.getFieldName(i)==null&&newOne.getFieldName(i)!=null)
                return false;
            if(!(this.getFieldType(i).equals(newOne.getFieldType(i))&&this.getFieldName(i).equals(newOne.getFieldName(i))))
                return false;
        }
        return true;
        // some code goes here
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder res=new StringBuilder("");
        int i=0;
        for(;i<numFields()-1;i++)
        {
            String tmp=itemList.get(i).toString()+",";
            res.append(tmp);
        }
        String tmp=itemList.get(i).toString();
        res.append(tmp);
        // some code goes here
        return res.toString();
    }
}
