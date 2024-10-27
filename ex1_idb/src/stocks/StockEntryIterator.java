package stocks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class StockEntryIterator implements Iterator<StockEntry> {
    //iterator class
    private long pos;
    private final RandomAccessFile file;

    public StockEntryIterator(RandomAccessFile file) {
        // TODO
        this.file = file;
        this.pos=0;
        try {
            file.seek(pos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        // TODO
        try {
            return file.getFilePointer()<file.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public StockEntry next() {
        // TODO
        try {
            long id=file.readLong();
            short nameLength=file.readShort();
            byte[] byteName=new byte[nameLength];
            file.read(byteName);
            String companyName=new String(byteName, StandardCharsets.UTF_8);
            long ts=file.readLong();
            double value=file.readDouble();
            return new StockEntry(id,companyName,ts,value);

        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
