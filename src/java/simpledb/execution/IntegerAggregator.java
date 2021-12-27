package simpledb.execution;

import jdk.jshell.spi.ExecutionControl;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Object agg;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;

        if(gbfield==Aggregator.NO_GROUPING)
        {
            agg=(Object) new ArrayList<Integer>();
        }
        else
        {
            assert gbfieldtype!=null;
            if(gbfieldtype==Type.INT_TYPE)
            {
                agg=(Object) new TreeMap<Integer, ArrayList<Integer>>();
            }
            else
            {
                agg=(Object) new TreeMap<String, ArrayList<Integer>>();
            }

        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield==Aggregator.NO_GROUPING)
        {
            ((ArrayList<Integer>)agg).add(((IntField)tup.getField(afield)).getValue());
        }
        else
        {
            if(gbfieldtype==Type.INT_TYPE)
            {
                TreeMap<Integer,ArrayList<Integer>> myagg=(TreeMap<Integer,ArrayList<Integer>>)this.agg;
                Integer groupbyVal=((IntField)tup.getField(gbfield)).getValue();
                Integer aggval=((IntField)tup.getField(afield)).getValue();
                if(!myagg.containsKey(groupbyVal))
                {
                    myagg.put(groupbyVal,new ArrayList<Integer>(1));
                }
                myagg.get(groupbyVal).add(aggval);

            }
            else
            {
                TreeMap<String,ArrayList<Integer>> myagg=(TreeMap<String,ArrayList<Integer>>)this.agg;
                String groupbyVal=((StringField)tup.getField(gbfield)).getValue();
                Integer afieldVal=((IntField)tup.getField(afield)).getValue();
                if(!myagg.containsKey(groupbyVal))
                {
                    myagg.put(groupbyVal,new ArrayList<>(1));
                }
                myagg.get(groupbyVal).add(afieldVal);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new Iter();
    }
    private class Iter implements OpIterator
    {
        private List<Tuple> tuples;
        private Iterator<Tuple> tupleIterator;
        private int calAggrRes(ArrayList<Integer> arrayList)
        {
            assert !arrayList.isEmpty();
            int res=0;
            switch (what)
            {
                case AVG:
                    for(Integer integer:arrayList)
                    {
                        res+=integer;
                    }
                    res/=arrayList.size();
                    break;
                case MAX:
                    for(Integer integer:arrayList)
                    {
                        res=Math.max(res,integer);
                    }
                    break;
                case MIN:
                    res=arrayList.get(0);
                    for(Integer integer:arrayList)
                    {
                        res=Math.min(res,integer);
                    }
                    break;
                case SUM:
                    for(Integer integer:arrayList)
                    {
                        res+=integer;
                    }
                    break;
                case COUNT:
                    res+=arrayList.size();
                    break;
                case SUM_COUNT:
                    System.out.println("not implement");//not
                case SC_AVG:
                    System.out.println("not implement");
            }
            return res;
        }
        public Iter()
        {
            tuples=new ArrayList<>();
            tupleIterator=null;
            if(gbfield==Aggregator.NO_GROUPING)
            {
                Tuple tmp=new Tuple(getTupleDesc());
                Field field=new IntField(this.calAggrRes((ArrayList<Integer>) agg));
                tmp.setField(0,field);
                tuples.add(tmp);
            }
            else
            {
                for(Map.Entry e:((TreeMap<Integer,ArrayList<Integer>>) agg).entrySet())
                {
                    Tuple tmp=new Tuple(getTupleDesc());
                    Field groupbyVal=null;
                    if(gbfieldtype==Type.INT_TYPE)
                    {
                        groupbyVal =new IntField((Integer) e.getKey());
                    }else
                    {
                        groupbyVal =new StringField ((String) e.getKey(),((String)e.getKey()).length());
                    }

                    tmp.setField(0,groupbyVal);
                    Field aggrateVal=new IntField(this.calAggrRes((ArrayList<Integer>) e.getValue()));
                    tmp.setField(1,aggrateVal);
                    tuples.add(tmp);
                }
            }
        }
        @Override
        public Tuple next() throws NoSuchElementException {
            return tupleIterator.next();
        }

        @Override
        public boolean hasNext() {
            return tupleIterator.hasNext();
        }

        @Override
        public void open() {
            tupleIterator=tuples.iterator();
        }

        @Override
        public void close() {
            tupleIterator=null;
        }

        @Override
        public void rewind() {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if(gbfield==Aggregator.NO_GROUPING)
            {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            }
            else
            {
                return new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
            }
        }
    }

}
