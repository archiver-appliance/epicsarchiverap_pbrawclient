package org.epics.archiverappliance.retrieval.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.protobuf.ByteString;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarByte;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarDouble;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarEnum;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarFloat;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarInt;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarShort;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarString;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.V4GenericBytes;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorChar;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorDouble;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorEnum;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorFloat;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorInt;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorShort;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorString;

/**
 * Generate a sequence of GeneratedMessage given an input stream...
 * @author mshankar
 *
 */
public class InputStreamBackedGenMsg implements GenMsgIterator, Closeable {
	private InputStream is;
	PayloadInfo info;
	// The size of the ByteBuffer here is related to the MAX_LINE sizes in LineByteStream...
	ByteBuffer buf = ByteBuffer.allocate(16*1024*1024); 
	EpicsMessage nextMsg = null;
	
	public InputStreamBackedGenMsg(InputStream is) throws IOException { 
		this.is = is;
		readAndUnescapeLine(buf);
		info = PayloadInfo.parseFrom(ByteString.copyFrom(buf));
		readAndUnescapeLine(buf);
		if(buf.hasRemaining()) { 
			nextMsg = parseNextMessage();
		} else { 
			nextMsg = null;
		}
	}
	
	@Override
	public PayloadInfo getPayLoadInfo() {
		return info;
	}

	@Override
	public Iterator<EpicsMessage> iterator() {
		return new Iterator<EpicsMessage>() {

			@Override
			public boolean hasNext() {
				return nextMsg != null;
			}

			@Override
			public EpicsMessage next() {
				try { 
					EpicsMessage ret = nextMsg;
					readAndUnescapeLine(buf);
					if(buf.hasRemaining()) { 
						nextMsg = parseNextMessage();
					} else { 
						nextMsg = null;
					}
					return ret;
				} catch(IOException ex) { 
					throw new RuntimeIOException(ex);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		}; 
	}
	
	private void readAndUnescapeLine(ByteBuffer buf) throws IOException {
		buf.clear();
		int next = is.read(); 
		while(next != -1) {
			byte b = (byte) next;
			if(b == ESCAPE_CHAR) {
				next = is.read();
				if(next == -1) { throw new IOException("Escape character terminated early"); }
				b = (byte) next;
				switch(b) {
				case ESCAPE_ESCAPE_CHAR: buf.put(ESCAPE_CHAR);break;
				case NEWLINE_ESCAPE_CHAR: buf.put(NEWLINE_CHAR);break;
				case CARRIAGERETURN_ESCAPE_CHAR:buf.put(CARRIAGERETURN_CHAR);break;
				default: buf.put(b);break;
				}
			} else if(b == NEWLINE_CHAR) {
				buf.flip();
				return;
			} else {
				buf.put(b);
			}
			
			next = is.read();
		}

		buf.flip();
		return;
	}

	private static final byte ESCAPE_CHAR = 0x1B;
	private static final byte ESCAPE_ESCAPE_CHAR = 0x01;
	private static final byte NEWLINE_CHAR = 0x0A;
	private static final byte NEWLINE_ESCAPE_CHAR = 0x02;
	private static final byte CARRIAGERETURN_CHAR = 0x0D;
	private static final byte CARRIAGERETURN_ESCAPE_CHAR = 0x03;
	
	private EpicsMessage parseNextMessage() throws IOException {
		switch(info.getType()) { 
		case SCALAR_BYTE: { 
			return new EpicsMessage(ScalarByte.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case SCALAR_DOUBLE: { 
			return new EpicsMessage(ScalarDouble.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case SCALAR_ENUM: { 
			return new EpicsMessage(ScalarEnum.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case SCALAR_FLOAT: { 
			return new EpicsMessage(ScalarFloat.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case SCALAR_INT: { 
			return new EpicsMessage(ScalarInt.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case SCALAR_SHORT: { 
			return new EpicsMessage(ScalarShort.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case SCALAR_STRING: { 
			return new EpicsMessage(ScalarString.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_BYTE: { 
			return new EpicsMessage(VectorChar.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_DOUBLE: { 
			return new EpicsMessage(VectorDouble.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_ENUM: { 
			return new EpicsMessage(VectorEnum.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_FLOAT: { 
			return new EpicsMessage(VectorFloat.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_INT: { 
			return new EpicsMessage(VectorInt.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_SHORT: { 
			return new EpicsMessage(VectorShort.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case WAVEFORM_STRING: { 
			return new EpicsMessage(VectorString.parseFrom(ByteString.copyFrom(buf)), info);
		}
		case V4_GENERIC_BYTES: { 
			return new EpicsMessage(V4GenericBytes.parseFrom(ByteString.copyFrom(buf)), info);
		}
		default:
			throw new IOException("Unknown type " + info.getType());
		}
	}

	@Override
	public void close() throws IOException {
		if(is != null) { 
			is.close();
			is = null;
		}
	}
}
