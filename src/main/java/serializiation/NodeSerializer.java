package serializiation;

import GHS.Neighbour;
import GHS.Node;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NodeSerializer extends Serializer<Node> {
    @Override
    public void write(Kryo kryo, Output output, Node node) {
        output.writeInt(node.id);
    }

    @Override
    public Node read(Kryo kryo, Input input, Class<? extends Node> aClass) {
        return null;
    }
}
