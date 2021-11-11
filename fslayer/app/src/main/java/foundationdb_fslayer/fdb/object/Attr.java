package foundationdb_fslayer.fdb.object;

public class Attr {
    private ObjectType objectType;
    private Long timestamp;
    private Long mode;

    public Attr setObjectType(ObjectType objectType){
        this.objectType = objectType;
        return this;
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    public Attr setTimestamp(long timestamp){
        this.timestamp = timestamp;
        return this;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public Attr setMode(long mode){
        this.mode = mode;
        return this;
    }

    public Long getMode(){
        return this.mode;
    }
}
