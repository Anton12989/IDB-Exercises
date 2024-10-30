package container.impl;

import container.Container;
import util.MetaData;

import java.util.ArrayList;
import java.util.List;

import java.util.NoSuchElementException;

public class MapContainer<Value> implements Container<Long, Value> {
	private boolean isContainerOpen = false;
	private enum statusOfKeys  {reserved, used, removed};
	private List<statusOfKeys> listOfKeys = new ArrayList<>();
	private List<Value> listOfValues = new ArrayList<>();
	private int lastUsedKey;
	private MetaData metaData = new MetaData();



	public MapContainer() {
		// TODO
	}
	
	@Override
	public MetaData getMetaData() {
		// TODO
		if(isContainerOpen == false)
		{
			throw new IllegalStateException();
		}
		return metaData;
	}
	
	@Override
	public void open() {
		// TODO
		if(isContainerOpen == false)
		{
			isContainerOpen = true;
		}
	}

	@Override
	public void close() {
		// TODO
		if(isContainerOpen == true)
		{
			isContainerOpen = false;
		}
	}
	
	@Override
	public Long reserve() throws IllegalStateException {
		// TODO
		if(isContainerOpen == false){
			throw new IllegalStateException();
		}
		Long newKey = (long) (lastUsedKey + 1);
		lastUsedKey = lastUsedKey + 1;
		listOfKeys.add(Math.toIntExact(newKey), statusOfKeys.reserved);
		return newKey;
	}
	

	@Override
	public Value get(Long key) throws NoSuchElementException {
		// TODO
		if(isContainerOpen == false){
			throw new IllegalStateException();
		}
		if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.used){
			return listOfValues.get(Math.toIntExact(key));
		}else{
			throw new NoSuchElementException();
		}
	}

	@Override
	public void update(Long key, Value value) throws NoSuchElementException {
		// TODO
		if(isContainerOpen == false){
			throw new IllegalStateException();
		}
		if(key > lastUsedKey){
			throw new NoSuchElementException();
		} else if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.reserved){
			listOfValues.add(Math.toIntExact(key), value);
		} else if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.used){
			listOfValues.set(Math.toIntExact(key), value);
		} else if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.removed){
			throw new NoSuchElementException("Key was deleted");
		}


	}

	@Override
	public void remove(Long key) throws NoSuchElementException {
		// TODO
		if(isContainerOpen == false){
			throw new IllegalStateException();
		}
		if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.removed){
			throw new NoSuchElementException("Value was removed already");
		} else if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.reserved){
			throw new NoSuchElementException();
		} else if(listOfKeys.get(Math.toIntExact(key)) == statusOfKeys.used)
		{
			listOfValues.remove(Math.toIntExact(key));
			listOfKeys.set(Math.toIntExact(key), statusOfKeys.removed);

		}
	}
}
