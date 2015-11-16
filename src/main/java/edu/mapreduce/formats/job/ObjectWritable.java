package edu.mapreduce.formats.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;

/**
 * Abstract implementation of a hadoop's writable using ObjectOutputStream. This indicates hadoop how it should
 * serialize the object between jobs stages (inter MapReduce).
 */
public abstract class ObjectWritable<T> implements Writable {
    T object;

    public ObjectWritable() {
        // Default constructor used by hadoop.
    }

    public ObjectWritable(T object) {
        this.object = object;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(object);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        dataOutput.writeInt(bytes.length);
        dataOutput.write(bytes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        byte[] bytes = new byte[length];
        dataInput.readFully(bytes);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        try {
            this.object = (T) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("could not deserialize user rating data");
        }
    }

    public T get() {
        return object;
    }
}
