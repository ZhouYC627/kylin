package org.apache.kylin.storage.parquet.steps;

import org.apache.hadoop.io.Text;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by answer on 11/2/18.
 */
public class TempRDDFunction implements PairFunction<Tuple2<Text, Text>, Void, Group> {
    private GroupFactory factory;

    @Override
    public Tuple2<Void, Group> call(Tuple2<Text, Text> textTextTuple2) throws Exception {
        Group group = factory.newGroup();
        group.append("test", Binary.fromConstantByteArray(textTextTuple2._2().getBytes(), 0, textTextTuple2._2().getLength()));

        return new Tuple2<>(null, group);
    }
}
