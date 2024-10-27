package stocks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Stocks implements Iterable<StockEntry> {

    private final RandomAccessFile file;

    Stocks(String path) throws FileNotFoundException {
        // TODO
        this.file = new RandomAccessFile(path,"r");
    }

    public StockEntry get(int i) {
        // TODO
        try {
            file.seek(recordParser(i));
            long id=file.readLong();
            short nameLength=file.readShort();
            byte[] byteName =new byte[nameLength];
            file.read(byteName);
            String companyName=new String(byteName, StandardCharsets.UTF_8);
            long timestamp=file.readLong();
            double marketValue=file.readDouble();
            return new StockEntry(id,companyName,timestamp,marketValue);

        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    private long recordParser(int i) throws IOException {
        long size=0;
        for(int j=0;j<i;j++){
            size+=8;
            file.seek(size);
            short nameLength =file.readShort();
            size+=nameLength+18;

        }
        return size;
    }

    @Override
    public Iterator<StockEntry> iterator() {
        return new StockEntryIterator(file);
    }
}