package GHS;

public class Message {
    public static final byte ACCEPT = 0X01;
    public static final byte REJECT = 0X02;
    public static final byte TEST = 0X03;
    public static final byte INITIATE = 0X04;
    public static final byte CONNECT = 0X05;
    public static final byte REPORT = 0X06;
    public static final byte CHANGE_CORE = 0X07;
    public int senderID;
    public int receiverID;
    public byte type;
    public int level; // used for connect, initiate, test
    public byte state; // used for initiate
    public Edge fragmentID; // used for initiate, test
    public Edge edge; // used for report

    public long timestamp;

    public Message(int senderID, int receiverID, byte type, long timestamp) {
        this.type = type;
        this.senderID = senderID;
        this.receiverID = receiverID;
        this.timestamp = timestamp;
    }

    public Message() {

    }

    @Override
    public String toString() {
        return "{" + " senderID='" + senderID + "'" + ", receiverID='" + receiverID + "'" + ", type='"
                + getTypeName(type) + "'" + ", level='" + level + "'" + ", state='" + Node.getStateName(state) + "'"
                + ", fragmentID='" + fragmentID + "'" + ", edge='" + edge + "'" + ", timestamp='" + timestamp + "'"
                + "}";
    }

    public static String getTypeName(byte type) {
        switch (type) {
        case ACCEPT:
            return "Accept";
        case REJECT:
            return "Reject";
        case CONNECT:
            return "Connect";
        case TEST:
            return "Test";
        case INITIATE:
            return "Initiate";
        case REPORT:
            return "Report";
        case CHANGE_CORE:
            return "Change-core";
        default:
            return "none";
        }
    }
}

/*

                        **** Message structure ****

___________________________________________________________________________________________
|                    |                       |                 |                          |
|  sender(4 bytes)   |   receiver(4 bytes)   |  type(1 byte)   |  optional(max 21 bytes)  |
|____________________|_______________________|_________________|__________________________| 

______________________________________________________________________________________________________________________
|             |                                                                                                       |
|    type     |  optional(max 21 bytes)                                                                               |
|_____________|_______________________________________________________________________________________________________|
|             |                      |                       |                   |                  |                 | 
| accept      |  <empty>             |                       |                   |                  |                 | 
| reject      |  <empty>             |                       |                   |                  |                 |
| change_core |  <empty>             |                       |                   |                  |                 |
| connect     |  level(4 bytes)      |                       |                   |                  |                 |
| report      |  smallerID(4 bytes)  |  biggerID(4 bytes)    |  edge(8 bytes)  |                  |                 |
| test        |  smallerID(4 bytes)  |  biggerID(4 bytes)    |  edge(8 bytes)  |  level(4 bytes)  |                 |
| initiate    |  smallerID(4 bytes)  |  biggerID(4 bytes)    |  edge(8 bytes)  |  level(4 bytes)  |  state(1 byte)  |
|_____________|______________________|_______________________|___________________|__________________|_________________|

*/
