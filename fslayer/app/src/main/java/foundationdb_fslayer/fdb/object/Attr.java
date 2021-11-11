package foundationdb_fslayer.fdb.object;

public class Attr {
    private ObjectType objectType;
    private Long timestamp;
    private Long mode;
    private Long uid;
    private Long gid;

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

    public Long getGid() {
        return gid;
    }

    public Attr setGid(Long gid) {
        this.gid = gid;
        return this;
    }

    public Long getUid() {
        return uid;
    }

    public Attr setUid(Long uid) {
        this.uid = uid;
        return this;
    }
}
