package foundationdb_fslayer.fdb.object;

public class Attr {
    private ObjectType objectType;
    private Long timestamp;

    public Attr setObjectType(ObjectType objectType){
        this.objectType = objectType;
        return this;
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    public Attr setTimestamp(Long timestamp){
        this.timestamp = timestamp;
        return this;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }
}
