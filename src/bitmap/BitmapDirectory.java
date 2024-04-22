package bitmap;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class BitmapDirectory {
    private Set<String> bitmapNames;
    private String filePath;

    public BitmapDirectory(String filePath) {
        this.filePath = filePath;
        loadBitmapNames();
    }

    @SuppressWarnings("unchecked")
    private void loadBitmapNames() {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            bitmapNames = (HashSet<String>) ois.readObject();
        } catch (FileNotFoundException e) {
            bitmapNames = new HashSet<>(); // No existing file, create a new set.
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load bitmap names from file", e);
        }
    }

    public boolean addBitmapName(String name) {
        boolean isAdded = bitmapNames.add(name);
        if (isAdded) {
            saveBitmapNames(); // Save updates to disk only if there was a change.
        }
        return isAdded;
    }

    public Set<String> getBitmapNames() {
        return new HashSet<>(bitmapNames); // Return a copy to maintain encapsulation
    }

    private void saveBitmapNames() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(bitmapNames);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to save bitmap names to file", e);
        }
    }

    // method to remove names if necessary
    public boolean removeBitmapName(String name) {
        boolean isRemoved = bitmapNames.remove(name);
        if (isRemoved) {
            saveBitmapNames();
        }
        return isRemoved;
    }
}