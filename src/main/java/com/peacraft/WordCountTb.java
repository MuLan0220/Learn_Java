package com.peacraft;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class WordCountTb implements Writable, DBWritable {
	String name;
	int value;
	public WordCountTb(String name,int value) {
		this.name=name;
		this.value=value;
	}
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		this.name=resultSet.getString(1);
		this.value = resultSet.getInt(2);
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setString(1, this.name);
		statement.setInt(2, this.value);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeInt(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		name=in.readUTF();
		value=in.readInt();
	}

}
