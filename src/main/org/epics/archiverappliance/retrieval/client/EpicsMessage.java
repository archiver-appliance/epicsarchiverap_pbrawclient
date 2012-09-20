package org.epics.archiverappliance.retrieval.client;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;

public class EpicsMessage {
	private GeneratedMessage message;
	private PayloadInfo info;
	public EpicsMessage(GeneratedMessage message, PayloadInfo info) { 
		this.message = message;
		this.info = info;
	}
	
	public Timestamp getTimestamp() { 
		int secondsIntoYear = (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(1));
		int nanos = (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(2));
		int year = info.getYear();
		Timestamp ret = new Timestamp((startOfYearInEpochSeconds.get(year) + secondsIntoYear)*1000);
		ret.setNanos(nanos);
		return ret;
	}

	public int getElementCount() { 
		return info.getElementCount();
	}

	@SuppressWarnings("unchecked")
	public Number getNumberValue() throws IOException { 
		switch(info.getType()) { 
		case SCALAR_BYTE: { 
			return ((ByteString) message.getField(message.getDescriptorForType().findFieldByNumber(3))).toByteArray()[0];
		}
		case SCALAR_DOUBLE: { 
			return (Double) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_ENUM: { 
			return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_FLOAT: { 
			return (Float) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_INT: { 
			return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_SHORT: { 
			return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_STRING: { 
			return Double.parseDouble((String) message.getField(message.getDescriptorForType().findFieldByNumber(3)));
		}
		case WAVEFORM_BYTE: { 
			return ((ByteString) message.getField(message.getDescriptorForType().findFieldByNumber(3))).toByteArray()[0];
		}
		case WAVEFORM_DOUBLE: { 
			return ((List<Double>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(0);
		}
		case WAVEFORM_ENUM: { 
			return ((List<Integer>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(0);
		}
		case WAVEFORM_FLOAT: { 
			return ((List<Float>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(0);
		}
		case WAVEFORM_INT: { 
			return ((List<Integer>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(0);
		}
		case WAVEFORM_SHORT: { 
			return ((List<Integer>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(0);
		}
		case WAVEFORM_STRING: { 
			return Double.parseDouble(((List<String>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(0));
		}
		case V4_GENERIC_BYTES: { 
			throw new IOException("Can't cast V4_GENERIC_BYTES to Number");
		}
		default:
			throw new IOException("Unknown type " + info.getType());
		}
	}

	@SuppressWarnings("unchecked")
	public Number getNumberAt(int index) throws IOException {  
		switch(info.getType()) { 
		case SCALAR_BYTE: { 
			return ((ByteString) message.getField(message.getDescriptorForType().findFieldByNumber(3))).toByteArray()[0];
		}
		case SCALAR_DOUBLE: { 
			return (Double) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_ENUM: { 
			return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_FLOAT: { 
			return (Float) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_INT: { 
			return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_SHORT: { 
			return (Integer) message.getField(message.getDescriptorForType().findFieldByNumber(3));
		}
		case SCALAR_STRING: { 
			return Double.parseDouble((String) message.getField(message.getDescriptorForType().findFieldByNumber(3)));
		}
		case WAVEFORM_BYTE: { 
			return ((ByteString) message.getField(message.getDescriptorForType().findFieldByNumber(3))).toByteArray()[index];
		}
		case WAVEFORM_DOUBLE: { 
			return ((List<Double>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(index);
		}
		case WAVEFORM_ENUM: { 
			return ((List<Integer>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(index);
		}
		case WAVEFORM_FLOAT: { 
			return ((List<Float>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(index);
		}
		case WAVEFORM_INT: { 
			return ((List<Integer>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(index);
		}
		case WAVEFORM_SHORT: { 
			return ((List<Integer>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(index);
		}
		case WAVEFORM_STRING: { 
			return Double.parseDouble(((List<String>) message.getField(message.getDescriptorForType().findFieldByNumber(3))).get(index));
		}
		case V4_GENERIC_BYTES: { 
			throw new IOException("Can't cast V4_GENERIC_BYTES to Number");
		}
		default:
			throw new IOException("Unknown type " + info.getType());
		}
	}

	public GeneratedMessage getMessage() { 
		return message;
	}
	
	static HashMap<Integer, Long> startOfYearInEpochSeconds = new HashMap<Integer, Long>(); 
	static {
		TimeZone timeZone = TimeZone.getTimeZone("UTC");
		for(int year = 1970; year < 16000; year++) { 
			Calendar cal = Calendar.getInstance(timeZone);
			cal.set(year, 0, 1, 0, 0, 0);
			startOfYearInEpochSeconds.put(year, cal.getTimeInMillis()/1000);
		}
	}
}
