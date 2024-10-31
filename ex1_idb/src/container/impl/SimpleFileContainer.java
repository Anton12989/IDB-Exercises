package container.impl;

import container.Container;
import io.FixedSizeSerializer;
import util.MetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

public class SimpleFileContainer<Value> implements Container<Long, Value> {

	private Path directory;
	private String filenamePrefix;
	private FixedSizeSerializer<Value> serializer;
	private ByteBuffer byteBuffer;
	private boolean isContainerOpen = false;
	private long lastUsedKey = -1;
	private MetaData metaData = new MetaData();

	public SimpleFileContainer(Path directory, String filenamePrefix, FixedSizeSerializer<Value> serializer) {
		// TODO
		this.directory = directory;
		this.filenamePrefix = filenamePrefix;
		this.serializer = serializer;
	}
	
	@Override
	public MetaData getMetaData() {
		// TODO
		MetaData metaData = new MetaData();
		return null;
	}

	@Override
	public void open() {
		// TODO
		if(isContainerOpen == false){
			isContainerOpen = true;
		}

	}

	@Override
	public void close() {
		// TODO
		if(isContainerOpen == true){
			isContainerOpen = false;
		}
	}
	
	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		if(isContainerOpen == false){
			throw new IllegalStateException();
		}
		Long newKey = lastUsedKey + 1L;
		lastUsedKey = lastUsedKey + 1L;
		return newKey;
	}
	
	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
	}


	@Override
	public Value get(Long key) throws NoSuchElementException {
		// TODO
		return null;
	}

	@Override
	public void remove(Long key) throws NoSuchElementException {
		// TODO
	}

	private ByteBuffer readAValueIntoTheBufferFromTheFileUsingKeys(Long key, RandomAccessFile file, int sizeOfValue)
	{
		ByteBuffer buffer = ByteBuffer.allocate(sizeOfValue);
        try {
			long position = 0;
            position = (key - 1L) * sizeOfValue;
			file.seek(position);
			byte[] byteArray = new byte[sizeOfValue];
            file.read(byteArray);
			buffer.put(byteArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer;
	}


}
