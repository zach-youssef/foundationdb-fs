package foundationdb_fslayer.fdb.object;

public class Attr {
    private ObjectType objectType;

    public Attr setObjectType(ObjectType objectType){
        this.objectType = objectType;
        return this;
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }
}
