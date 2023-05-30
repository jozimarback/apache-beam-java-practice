package org.apachebeam.samples.pipeline1;

public class CustomerEntity {

    public CustomerEntity(){}
    public CustomerEntity(String pId, String pName){
        id = pId;
        name = pName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String id;
    private String name;
}
