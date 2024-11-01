package container.impl;

import io.IntSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SimpleFileContainerRun {
    public static void main(String[] args) {

        Path directory = Paths.get("data");
        String filenamePrefix = "integer_container";
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        IntSerializer serializer = new IntSerializer();
        SimpleFileContainer<Integer> container = new SimpleFileContainer<>(directory, filenamePrefix, serializer);

        container.open();

        container.insert(4000); // 0
        container.insert(29);   // 1
        container.insert(50);   // 2
        container.insert(800);  // 3
        container.insert(250);  // 4
//        container.update(3L,50);

        long pos=3L;
//        container.remove(pos);
        int a=container.get(pos);
        System.out.println("Value at pos "+pos+" is : " +a);


//        MetaData metaData = container.getMetaData();
//        System.out.println("Metadata:");
//        System.out.println("Key counter: " + metaData.getLongProperty("keyCounter"));
        container.close();
    }
}