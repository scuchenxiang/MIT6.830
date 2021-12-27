package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.rmi.server.ExportException;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private final File file;//
    private final TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    //return the absolutePath's hashcode as the tableId
    public int getId() {
        return file.getAbsoluteFile().hashCode();
        // some code goes here
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
        // some code goes here
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

        int pageNum=pid.getPageNumber();
        final int pageSize=BufferPool.getPageSize();
        byte[] data=new byte[pageSize];
        RandomAccessFile randomAccessFile=null;
        try {
             randomAccessFile = new RandomAccessFile(file,"r");
            randomAccessFile.seek(pageSize*pageNum);
            randomAccessFile.read(data);
            HeapPage page=new HeapPage((HeapPageId)pid,data);
            randomAccessFile.close();
            return page;
        }catch  (FileNotFoundException e)
        {
            throw new IllegalArgumentException("page Not found");
        }catch (IOException e)
        {
            throw new IllegalArgumentException("IOException error");
        }
        finally {
            try {
                randomAccessFile.close();
            }catch (Exception e)
            {
                throw new IllegalArgumentException("Exception error");
            }
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] pageData=page.getPageData();
        RandomAccessFile randomAccessFile=new RandomAccessFile(file,"rws");
        final int pagesize=Database.getBufferPool().getPageSize();
        int pageNo=page.getId().getPageNumber();
        randomAccessFile.skipBytes(pageNo*pagesize);
        randomAccessFile.write(pageData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long fileSize=file.length();
        int pagesize=BufferPool.getPageSize();
        return (int)fileSize/pagesize;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> res=new ArrayList<>();
        for(int i=0;i<this.numPages()+1;i++)
        {
            HeapPage heapPage=null;
            HeapPageId heapPageId=new HeapPageId(this.getId(),i);
            if(i<this.numPages())
                heapPage=(HeapPage)Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
            else
                heapPage=new HeapPage(heapPageId,HeapPage.createEmptyPageData());
            if(heapPage.getNumEmptySlots()>0)
            {
                heapPage.insertTuple(t);
                if(i<this.numPages())
                    res.add(heapPage);
                else
                    writePage(heapPage);
                return res;
            }
        }
        throw new DbException("heap file full");
        // not necessary for lab1

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId recordId=t.getRecordId();
        ArrayList<Page> res=new ArrayList<>();
        if(this.getId()==recordId.getPageId().getTableId())
        {
            HeapPage page=(HeapPage)Database.getBufferPool().getPage(tid,recordId.getPageId(),Permissions.READ_WRITE);
            page.deleteTuple(t);
            res.add(page);
            return res;
        }
        throw new DbException("heap file error: wrong table");
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new Itr(tid);
    }
    private class Itr implements DbFileIterator
    {
        private final TransactionId tid;
        private Integer pagePosition;
        private Iterator<Tuple> tupleIter;
        private final int tableId;
        private final int numPages;
        public Itr(TransactionId transactionId)
        {
            pagePosition=null;
            tupleIter=null;
            tableId=getId();
            numPages=numPages();
            tid=transactionId;
        }
        private Iterator<Tuple> getTupleIter(int pgNo) throws TransactionAbortedException,DbException
        {
            HeapPageId pageId=new HeapPageId(tableId,pgNo);
            BufferPool bufferPool=Database.getBufferPool();
            HeapPage heapPage=(HeapPage) (bufferPool.getPage(tid,pageId,Permissions.READ_ONLY));
            return heapPage.iterator();
        }
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException
        {
            if(pagePosition!=null)
            {
                while (pagePosition<numPages-1)
                {
                    if(tupleIter.hasNext())
                    {
                        return true;
                    }
                    else
                    {
                        pagePosition+=1;
                        tupleIter=getTupleIter(pagePosition);
                    }
                }
                return tupleIter.hasNext();
            }
            return false;
        }
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
        {

            if (hasNext())
            {
                return tupleIter.next();
            }
            else
            {
                throw new NoSuchElementException("HeadFile Iterator error");
            }
        }
        @Override
        public void rewind() throws DbException, TransactionAbortedException
        {
            close();
            open();
        }
        @Override
        public void close()
        {
            pagePosition=null;
            tupleIter=null;
        }
        @Override
        public void open()throws DbException, TransactionAbortedException
        {
            pagePosition=0;
            tupleIter=getTupleIter(pagePosition);
        }
    }
}
