package com.my.model;
public enum Field
{
    ID("id"),
    TYPEID("typeId"),
    CONTENT("content"),
    CREATIONDATE("creationDate"),
    STATUS("status");
    private String fieldName;

    private Field(String fieldName)
    {
        this.fieldName = fieldName;
    }

    public String getFieldName()
    {
        return fieldName;
    }

}
