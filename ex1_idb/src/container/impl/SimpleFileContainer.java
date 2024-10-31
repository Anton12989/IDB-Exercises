package container.impl;

import container.Container;
import io.FixedSizeSerializer;
import util.MetaData;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class SimpleFileContainer<Value> implements Container<Long, Value> {
	private final Path containerDataFilePath;
	private final Path metaDataFilePath;
	private final FixedSizeSerializer<Value> typeSerializer;
	private long keyCounter;
	private MetaData metaData;
	private RandomAccessFile containerDataFile;


	public SimpleFileContainer(Path directory, String filenamePrefix, FixedSizeSerializer<Value> serializer) {
		// TODO
		this.containerDataFilePath=directory.resolve(filenamePrefix+"_container_data.bin");
		this.metaDataFilePath=directory.resolve(filenamePrefix+"_metadata.dat");
		this.typeSerializer=serializer;
		this.metaData=new MetaData();
		this.keyCounter=-1;
	}

	@Override
	public MetaData getMetaData() {
		// TODO
		return this.metaData;
	}

	@Override
	public void open() {
		// TODO
		if(!Files.exists(containerDataFilePath)){
			try {
				Files.createFile(containerDataFilePath);
			} catch (IOException e) {
				throw new RuntimeException("Failed to create container file: ",e);
			}
		}
		if(!Files.exists(metaDataFilePath)){
			try {
				Files.createFile(metaDataFilePath);
				keyCounter=metaData.getLongProperty("keyCounter",0);
			} catch (IOException e) {
				throw new RuntimeException("Failed to create metadata file: ",e);
			}
		}
		else{
			keyCounter=0;
//			metaData.setLongProperty("keyCounter",keyCounter);
		}
		try {
			containerDataFile=new RandomAccessFile(containerDataFilePath.toFile(),"rw");
			metaData.readFrom(metaDataFilePath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		// TODO
		try {
			metaData.setLongProperty("keyCounter",keyCounter);
			metaData.setIntProperty("containerSize",typeSerializer.getSerializedSize()+1);
			metaData.setLongProperty("containerFileSize",containerDataFile.length());
//			System.out.println("key counter "+metaData.getLongProperty("keyCounter"));
			metaData.writeTo(metaDataFilePath);
			containerDataFile.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		return keyCounter++;
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
		try {
			long seekValue= key*(typeSerializer.getSerializedSize()+1);
			ByteBuffer byteStream=ByteBuffer.allocate(typeSerializer.getSerializedSize()+1);
			containerDataFile.seek(seekValue);
			containerDataFile.writeByte(0);
			typeSerializer.serialize(value,byteStream);
			containerDataFile.write(byteStream.array());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
}
