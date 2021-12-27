package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.io.File;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Object agg;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what!=Op.COUNT)
        {
            throw new IllegalArgumentException("stringAggregator only support count");
        }

        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;

        if(this.gbfield==Aggregator.NO_GROUPING)
        {
            agg=0;
        }else
        {
            if(gbfieldtype==Type.INT_TYPE)
            {
                agg=(Object) new TreeMap<Integer, ArrayList<Integer>>();
            }else
            {
                agg=(Object)new TreeMap<String, ArrayList<Integer>>();
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield==Aggregator.NO_GROUPING)
        {
            agg=(Integer)agg+1;
        }
        else
        {
            if(gbfieldtype==Type.INT_TYPE)
            {
                TreeMap<Integer,Integer> myagg=(TreeMap<Integer,Integer>) agg;
                //
                Integer gbkey=((IntField)tup.getField(gbfield)).getValue();
                if(myagg.containsKey(gbkey))
                {
                    myagg.put(gbkey,myagg.get(gbkey)+1);
                }else
                {
                    myagg.put(gbkey,1);
                }
            }
            else
            {
                TreeMap<String,Integer> myagg=(TreeMap<String,Integer>) agg;
                //
                String gbkey=((StringField)tup.getField(gbfield)).getValue();
                if(myagg.containsKey(gbkey))
                {
                    myagg.put(gbkey,myagg.get(gbkey)+1);
                }else
                {
                    myagg.put(gbkey,1);
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new Iter();
    }
    private class Iter implements OpIterator
    {
        private Iterator<Tuple> it;
        private List<Tuple> tuples;
        public Iter()
        {
            assert what==Op.COUNT;
            it=null;
            tuples=new ArrayList<>();
            if(gbfield==Aggregator.NO_GROUPING)
            {
                Tuple tmp=new Tuple(getTupleDesc());
                tmp.setField(0,new IntField((Integer) agg));
                tuples.add(tmp);
            }
            else
            {
                for(Map.Entry e: ((TreeMap<Integer,ArrayList<Integer>>) agg).entrySet())
                {
                    Tuple tmp=new Tuple(getTupleDesc());
                    Field groupbyVal=null;
                    if(gbfieldtype==Type.INT_TYPE)
                    {
                        groupbyVal=new IntField((int)e.getKey()) ;
                    }else
                    {
                        groupbyVal=new StringField((String) e.getKey(),((String) e.getKey()).length());
                    }
                    tmp.setField(0,groupbyVal);
                    Field aggreVal=new IntField((int)e.getValue());
                    tmp.setField(1,aggreVal);
                    tuples.add(tmp);
                }
            }
        }
        @Override
        public TupleDesc getTupleDesc() {
            if (gbfield == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
        }
        @Override
        public boolean hasNext() {

            return it.hasNext();
        }

        @Override
        public Tuple next() throws NoSuchElementException {
            return it.next();
        }

        @Override
        public void open() {
            it=tuples.iterator();
        }

        @Override
        public void rewind() {
            close();
            open();
        }

        @Override
        public void close() {
            it=null;
        }
    }

}
