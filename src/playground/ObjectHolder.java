package playground;

public class ObjectHolder<T> {
    public T object;
    
    public String toString() {
        return "ObjectHolder<" + object.toString() + ">";
    }
}
