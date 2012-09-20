package org.epics.archiverappliance.retrieval.client;

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
public class InputStreamBackedGenMsg implements GenMsgIterator {
	private InputStream is;
	PayloadInfo info;
	// The size of the ByteBuffer here is related to the MAX_LINE sizes in LineByteStream...
	ByteBuffer buf = ByteBuffer.allocate(16*1024*1024); 
	EpicsMessage nextMsg = null;
	int currentLine = 0;
	
	public InputStreamBackedGenMsg(InputStream is) throws IOException { 
		this.is = is;
		readAndUnescapeLine(buf);
		info = PayloadInfo.parseFrom(ByteString.copyFrom(buf));
		readLineAndParseNextMessage();
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
					readLineAndParseNextMessage();
					return ret;
				} catch(IOException ex) { 
					throw new RuntimeIOException("Exception near line " + currentLine, ex);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		}; 
	}
	
	/**
	 * Read a line into buf. Return true if we are exiting because of a newline; else return false.
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	private boolean readAndUnescapeLine(ByteBuffer buf) throws IOException {
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
				currentLine++;
				return true;
			} else {
				buf.put(b);
			}
			
			next = is.read();
		}

		buf.flip();
		currentLine++;
		return false;
	}

	private static final byte ESCAPE_CHAR = 0x1B;
	private static final byte ESCAPE_ESCAPE_CHAR = 0x01;
	private static final byte NEWLINE_CHAR = 0x0A;
	private static final byte NEWLINE_ESCAPE_CHAR = 0x02;
	private static final byte CARRIAGERETURN_CHAR = 0x0D;
	private static final byte CARRIAGERETURN_ESCAPE_CHAR = 0x03;
	
	private boolean loopInfoLine() throws IOException {
		int loopCount = 0;
		boolean haveNewline = readAndUnescapeLine(buf);
		while(loopCount++ < 1000) { 
			if(!haveNewline && !buf.hasRemaining()) { 
				// This is the end of the stream
				return false;
			} else if(haveNewline && !buf.hasRemaining()) { 
				// We encountered an empty line. We expect a header next and data after that
				readAndUnescapeLine(buf);
				if(!buf.hasRemaining()) { 
					// We encountered an empty line and there was not enough info for a payload.
					// We treat this as the end of the stream
					return false;
				}
				info = PayloadInfo.parseFrom(ByteString.copyFrom(buf));
				haveNewline = readAndUnescapeLine(buf);
			} else { 
				// Regardless of whether the line ended in a newline or not, we have data in buf
				return true;
			}
		}
		throw new IOException("We are unable to determine next event in " + loopCount + " loops");
	}
	
	private void readLineAndParseNextMessage() throws IOException {
		boolean processNextMsg = loopInfoLine();
		if(!processNextMsg) { 
			nextMsg = null;
			return;
		}
		
		switch(info.getType()) { 
		case SCALAR_BYTE: { 
			nextMsg = new EpicsMessage(ScalarByte.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case SCALAR_DOUBLE: { 
			nextMsg = new EpicsMessage(ScalarDouble.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case SCALAR_ENUM: { 
			nextMsg = new EpicsMessage(ScalarEnum.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case SCALAR_FLOAT: { 
			nextMsg = new EpicsMessage(ScalarFloat.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case SCALAR_INT: { 
			nextMsg = new EpicsMessage(ScalarInt.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case SCALAR_SHORT: { 
			nextMsg = new EpicsMessage(ScalarShort.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case SCALAR_STRING: { 
			nextMsg = new EpicsMessage(ScalarString.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_BYTE: { 
			nextMsg = new EpicsMessage(VectorChar.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_DOUBLE: { 
			nextMsg = new EpicsMessage(VectorDouble.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_ENUM: { 
			nextMsg = new EpicsMessage(VectorEnum.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_FLOAT: { 
			nextMsg = new EpicsMessage(VectorFloat.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_INT: { 
			nextMsg = new EpicsMessage(VectorInt.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_SHORT: { 
			nextMsg = new EpicsMessage(VectorShort.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case WAVEFORM_STRING: { 
			nextMsg = new EpicsMessage(VectorString.parseFrom(ByteString.copyFrom(buf)), info);
			return;
		}
		case V4_GENERIC_BYTES: { 
			nextMsg = new EpicsMessage(V4GenericBytes.parseFrom(ByteString.copyFrom(buf)), info);
			return;
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
