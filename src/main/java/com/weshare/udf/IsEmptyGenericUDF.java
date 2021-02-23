package com.weshare.udf;

import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

/**
 * @author ximing.wei
 */
@Description(
        name = "is_empty",
        value = "_FUNC_(PRIMITIVE primitive [, String string || Integer int])",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('aa') as t;\n" +
                "    aa\n" +
                "  SELECT _FUNC_(1,0) as t;\n" +
                "    1\n" +
                "  SELECT _FUNC_('null', 0) as t;\n" +
                "    0"
)
public class IsEmptyGenericUDF extends GenericUDF {

    private BooleanObjectInspector F_1_BOOLEAN;
    private ByteObjectInspector F_1_BYTE;
    private ShortObjectInspector F_1_SHORT;
    private IntObjectInspector F_1_INT;
    private LongObjectInspector F_1_LONG;
    private FloatObjectInspector F_1_FLOAT;
    private DoubleObjectInspector F_1_DOUBLE;
    private StringObjectInspector F_1_STRING;
    private DateObjectInspector F_1_DATE;
    private TimestampObjectInspector F_1_TIMESTAMP;
    private BinaryObjectInspector F_1_BINARY;
    private HiveDecimalObjectInspector F_1_DECIMAL;
    private HiveVarcharObjectInspector F_1_VARCHAR;
    private HiveCharObjectInspector F_1_CHAR;

    private StringObjectInspector S_2_STRING;
    private IntObjectInspector S_2_INT;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        int length = objectInspectors.length;

        if (length != 1 && length != 2)
            throw new UDFArgumentLengthException("Arguments needs (1,2), but: " + objectInspectors.length);

        if (length == 2) {
            ObjectInspector s_2 = objectInspectors[1];
            switch (((PrimitiveObjectInspector) s_2).getPrimitiveCategory()) {
                case STRING:
                    this.S_2_STRING = (StringObjectInspector) s_2;
                    break;
                case INT:
                    this.S_2_INT = (IntObjectInspector) s_2;
                    break;
                default:
                    throw new UDFArgumentTypeException(1, "Error Type (" + s_2.getTypeName() + "), Needs(Int、String)");
            }
        }

        ObjectInspector f_1 = objectInspectors[0];

        switch (f_1.getCategory()) {
            case PRIMITIVE:
                switch (((PrimitiveObjectInspector) f_1).getPrimitiveCategory()) {
                    case BOOLEAN:
                        this.F_1_BOOLEAN = (BooleanObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case BYTE:
                        this.F_1_BYTE = (ByteObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case SHORT:
                        this.F_1_SHORT = (ShortObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case INT:
                        this.F_1_INT = (IntObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case LONG:
                        this.F_1_LONG = (LongObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case FLOAT:
                        this.F_1_FLOAT = (FloatObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case DOUBLE:
                        this.F_1_DOUBLE = (DoubleObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case STRING:
                        this.F_1_STRING = (StringObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case DATE:
                        this.F_1_DATE = (DateObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case TIMESTAMP:
                        this.F_1_TIMESTAMP = (TimestampObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case BINARY:
                        this.F_1_BINARY = (BinaryObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case DECIMAL:
                        this.F_1_DECIMAL = (HiveDecimalObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case VARCHAR:
                        this.F_1_VARCHAR = (HiveVarcharObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    case CHAR:
                        this.F_1_CHAR = (HiveCharObjectInspector) f_1;
                        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                    default:
                        throw new UDFArgumentException("未定义的基础参数，请修改 UDF 函数！");
                }
            case LIST:
                break;
            case MAP:
                break;
            case STRUCT:
                break;
            case UNION:
                break;
            default:
                throw new UDFArgumentTypeException(0, "传入的是未知数据类型！");
        }
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object s_1 = deferredObjects[0].get();

        if (EmptyUtil.isEmpty(s_1)) {
            switch (deferredObjects.length) {
                case 1:
                    return null;
                case 2:
                    Object s_2 = deferredObjects[1].get();
                    if (EmptyUtil.isEmpty(s_2)) return null;
                    else {
                        if (this.S_2_STRING != null) return this.S_2_STRING.getPrimitiveJavaObject(s_2);
                        if (this.S_2_INT != null) return this.S_2_INT.getPrimitiveJavaObject(s_2);
                    }
                default:
                    throw new UDFArgumentLengthException("Arguments length is Error");
            }
        }
        return this.F_1_STRING.getPrimitiveJavaObject(s_1);
    }

    @Override
    public String getDisplayString(String[] strings) {
        if (strings.length == 1) {
            return "is_empty('" + strings[0] + "')";
        }
        return "is_empty('" + strings[0] + "', '" + strings[1] + "')";
    }
}
