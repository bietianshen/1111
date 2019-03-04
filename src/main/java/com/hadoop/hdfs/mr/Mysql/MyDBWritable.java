package com.hadoop.hdfs.mr.Mysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyDBWritable implements Writable, DBWritable {
    private Integer id;
    private String name;
    private String text;
    private String word;
    private Integer number;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(text);
        dataOutput.writeUTF(word);
        dataOutput.writeInt(number);
    }

    public void readFields(DataInput dataInput) throws IOException {
        id =  dataInput.readInt();
        name = dataInput.readUTF();
        text = dataInput.readUTF();
        word = dataInput.readUTF();
        number = dataInput.readInt();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,word);
        preparedStatement.setInt(2,number);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);
        text = resultSet.getString(3);
    }
}
