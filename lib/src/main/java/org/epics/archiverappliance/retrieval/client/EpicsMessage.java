package org.epics.archiverappliance.retrieval.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Similar to DBRTimeEvent but much more lightweight.
 * Wraps a Protocol Buffer GeneratedMessage and return EPICS data.
 * We would expect to see one of these per Event in the stream.
 * @author mshankar
 *
 */
public class EpicsMessage {
    private final Message message;
    private final PayloadInfo info;
    private Timestamp ts;

    public EpicsMessage(Message message, PayloadInfo info) {
        this.message = message;
        this.info = info;
        int secondsIntoYear =
                (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(1));
        int nanos = (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(2));
        int year = info.getYear();
        ts = new Timestamp((startOfYearInEpochSeconds.get(year) + secondsIntoYear) * 1000);
        ts.setNanos(nanos);
    }

    public EpicsMessage(EpicsMessage otherMessage) {
        this(otherMessage.message, otherMessage.info);
    }

    public Timestamp getTimestamp() {
        return ts;
    }

    public int getElementCount() {
        return info.getElementCount();
    }

    @SuppressWarnings("unchecked")
    public Number getNumberValue() {
        switch (info.getType()) {
            case SCALAR_BYTE -> {
                return ((ByteString)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .toByteArray()[0];
            }
            case SCALAR_DOUBLE -> {
                return (Double) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case SCALAR_ENUM, SCALAR_INT, SCALAR_SHORT -> {
                return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case SCALAR_FLOAT -> {
                return (Float) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case SCALAR_STRING -> {
                return Double.parseDouble(
                        (String) message.getField(message.getDescriptorForType().findFieldByNumber(3)));
            }
            case WAVEFORM_BYTE -> {
                return ((ByteString)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .toByteArray()[0];
            }
            case WAVEFORM_DOUBLE -> {
                return ((List<Double>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0);
            }
            case WAVEFORM_ENUM -> {
                return ((List<Integer>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0);
            }
            case WAVEFORM_FLOAT -> {
                return ((List<Float>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0);
            }
            case WAVEFORM_INT -> {
                return ((List<Integer>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0);
            }
            case WAVEFORM_SHORT -> {
                return ((List<Integer>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0);
            }
            case WAVEFORM_STRING -> {
                return Double.parseDouble(((List<String>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0));
            }
            case V4_GENERIC_BYTES -> {
                throw new UnsupportedOperationException("Can't cast V4_GENERIC_BYTES to Number");
            }
            default -> throw new UnsupportedOperationException("Unknown type " + info.getType());
        }
    }

    @SuppressWarnings("unchecked")
    public Number getNumberAt(int index) {
        switch (info.getType()) {
            case SCALAR_BYTE -> {
                return ((ByteString)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .toByteArray()[0];
            }
            case SCALAR_DOUBLE -> {
                return (Double) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case SCALAR_ENUM, SCALAR_SHORT, SCALAR_INT -> {
                return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case SCALAR_FLOAT -> {
                return (Float) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case SCALAR_STRING -> {
                return Double.parseDouble(
                        (String) message.getField(message.getDescriptorForType().findFieldByNumber(3)));
            }
            case WAVEFORM_BYTE -> {
                return ((ByteString)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .toByteArray()[index];
            }
            case WAVEFORM_DOUBLE -> {
                return ((List<Double>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(index);
            }
            case WAVEFORM_ENUM, WAVEFORM_INT, WAVEFORM_SHORT -> {
                return ((List<Integer>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(index);
            }
            case WAVEFORM_FLOAT -> {
                return ((List<Float>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(index);
            }
            case WAVEFORM_STRING -> {
                return Double.parseDouble(((List<String>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(index));
            }
            case V4_GENERIC_BYTES -> {
                throw new UnsupportedOperationException("Can't cast V4_GENERIC_BYTES to Number");
            }
            default -> throw new UnsupportedOperationException("Unknown type " + info.getType());
        }
    }

    @SuppressWarnings("unchecked")
    public String getStringValue() {
        String notSupportedMessage = "Conversion to string not supported yet";
        switch (info.getType()) {
            case SCALAR_BYTE,
                    SCALAR_SHORT,
                    SCALAR_INT,
                    SCALAR_FLOAT,
                    SCALAR_ENUM,
                    SCALAR_DOUBLE,
                    WAVEFORM_BYTE,
                    WAVEFORM_DOUBLE,
                    WAVEFORM_ENUM,
                    WAVEFORM_FLOAT,
                    WAVEFORM_INT,
                    WAVEFORM_SHORT -> {
                throw new UnsupportedOperationException(notSupportedMessage);
            }
            case SCALAR_STRING -> {
                return (String) message.getField(message.getDescriptorForType().findFieldByNumber(3));
            }
            case WAVEFORM_STRING -> {
                return ((List<String>)
                                message.getField(message.getDescriptorForType().findFieldByNumber(3)))
                        .get(0);
            }
            case V4_GENERIC_BYTES -> {
                throw new UnsupportedOperationException("Can't cast V4_GENERIC_BYTES to Number");
            }
            default -> throw new UnsupportedOperationException("Unknown type " + info.getType());
        }
    }

    public Message getMessage() {
        return message;
    }

    static HashMap<Integer, Long> startOfYearInEpochSeconds = new HashMap<Integer, Long>();

    static {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        for (int year = 1970; year < 16000; year++) {
            Calendar cal = Calendar.getInstance(timeZone);
            cal.set(year, 0, 1, 0, 0, 0);
            startOfYearInEpochSeconds.put(year, cal.getTimeInMillis() / 1000);
        }
    }

    public int getSeverity() {
        FieldDescriptor fdesc = message.getDescriptorForType().findFieldByNumber(4);
        if (message.hasField(fdesc)) {
            return (Integer) message.getField(fdesc);
        }
        return 0;
    }

    public int getStatus() {
        FieldDescriptor fdesc = message.getDescriptorForType().findFieldByNumber(5);
        if (message.hasField(fdesc)) {
            return (Integer) message.getField(fdesc);
        }
        return 0;
    }

    public boolean hasFieldValues() {
        FieldDescriptor fdesc = message.getDescriptorForType().findFieldByNumber(7);
        return message.getRepeatedFieldCount(fdesc) > 0;
    }

    public Map<String, String> getFieldValues() {
        HashMap<String, String> ret = new HashMap<String, String>();
        FieldDescriptor fdesc = message.getDescriptorForType().findFieldByNumber(7);
        if (message.getRepeatedFieldCount(fdesc) > 0) {
            @SuppressWarnings("unchecked")
            List<FieldValue> fieldValues = (List<FieldValue>) message.getField(fdesc);
            for (FieldValue fieldValue : fieldValues) {
                ret.put(fieldValue.getName(), fieldValue.getVal());
            }
        }
        return ret;
    }

    public void setTimestamp(Timestamp ts) {
        this.ts = ts;
    }
}
