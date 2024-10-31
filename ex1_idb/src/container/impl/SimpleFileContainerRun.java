package container.impl;

import io.IntSerializer;
import util.MetaData;

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

        Long key1 = 0L;
        container.update(key1, 22);
        Long key2 = container.reserve();
        container.update(key2, 7000);
        Long key3 = container.reserve();
        container.update(key3, 256);

        container.close();

        MetaData metaData = container.getMetaData();
        System.out.println("Metadata:");
        System.out.println("Key counter: " + metaData.getLongProperty("keyCounter"));
    }
}