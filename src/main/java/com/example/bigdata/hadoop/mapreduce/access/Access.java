package com.example.bigdata.hadoop.mapreduce.access;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义access类需要完成下面几步：
 * 1）implement Writeable接口
 * 2）override write & readFields 方法
 * 3）定义一个默认的无参构造方法
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Access implements Writable {

    private String phone;
    private Double up;
    private Double down;
    private Double sum;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeDouble(up);
        out.writeDouble(down);
        out.writeDouble(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readDouble();
        this.down = in.readDouble();
        this.sum = in.readDouble();
    }

    @Override
    public String toString() {
        return "(" + phone + "," + up + "," + down + "," + sum + ')';
    }
}
