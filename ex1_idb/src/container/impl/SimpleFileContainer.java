package container.impl;

import container.Container;
import io.FixedSizeSerializer;
import util.MetaData;

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
	private final int containerBlockSize;
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
		this.containerBlockSize=typeSerializer.getSerializedSize()+1;
	}

	@Override
	public MetaData getMetaData() {
		// TODO
		if(keyCounter==-1){
			throw new IllegalStateException("The file was not opened");
		}
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
		if(keyCounter==-1){
			throw new IllegalStateException("The file was not opened");
		}
		try {
			metaData.setLongProperty("keyCounter",keyCounter);
			metaData.setIntProperty("containerSize",containerBlockSize);
			metaData.setLongProperty("containerFileSize",containerDataFile.length());
			metaData.writeTo(metaDataFilePath);
			containerDataFile.close();
			keyCounter=-1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		if(keyCounter==-1){
			throw new IllegalStateException("The file was not opened");
		}
		return keyCounter++;
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
		if(keyCounter==-1){
			throw new IllegalStateException("The file was not opened");
		}
		try {
			long seekValue= key*(containerBlockSize);
			ByteBuffer byteStream=ByteBuffer.allocate(containerBlockSize);
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
		if(keyCounter==-1){
			throw new IllegalStateException("The file was not opened");
		}
		try {
			long totalNoKeys =(containerDataFile.length()-1)/containerBlockSize;
			if(key>=totalNoKeys){
				throw new NoSuchElementException("No value for the key exist");
			}
			long pos = key * (containerBlockSize);
			containerDataFile.seek(pos);
			if (containerDataFile.readByte() != 0) {
				throw new NoSuchElementException("The value was deleted");
			}
			byte[] bytes = new byte[containerBlockSize-1];
			containerDataFile.read(bytes);
			ByteBuffer byteStream=ByteBuffer.wrap(bytes);
			return typeSerializer.deserialize(byteStream);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove(Long key) throws NoSuchElementException {
		// TODO
		if(keyCounter==-1){
			throw new IllegalStateException("The file was not opened");
		}
        try {
			long pos=key*(containerBlockSize);
            containerDataFile.seek(pos);
			byte deleteByte=containerDataFile.readByte();
			if(deleteByte==1){
				throw new NoSuchElementException("The value was already deleted");
			}
			else{
				containerDataFile.seek(pos);
				containerDataFile.writeByte(1);
				System.out.println("The value is deleted");
			}
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
