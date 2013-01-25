package org.epics.archiverappliance.retrieval.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

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
	private static Logger logger = Logger.getLogger(InputStreamBackedGenMsg.class.getName());
	private InputStream is;
	private byte[] isBuf = new byte[256*1024];
	private int currentReadPointer = 0;
	private int bytesRead = -1;
	private long filePos = 0;
	PayloadInfo info;
	InfoChangeHandler infoChangeHandler = null;
	// The size of the ByteBuffer here is related to the MAX_LINE sizes in LineByteStream...
	ByteBuffer buf = ByteBuffer.allocate(16*1024*1024); 
	EpicsMessage nextMsg = null;
	int currentLine = 0;
	
	public InputStreamBackedGenMsg(InputStream is) throws IOException { 
		this.is = is;
		readAndUnescapeLine(buf);
		info = PayloadInfo.parseFrom(ByteString.copyFrom(buf));
		if(this.infoChangeHandler != null) this.infoChangeHandler.handleInfoChange(info);
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
	 * Calls to InputStream read are replaced with calls to this method instead
	 */
	private void fetchData() throws IOException {
		currentReadPointer = 0;
		filePos += bytesRead;
		bytesRead = is.read(isBuf);
	}
	
	/**
	 * Read a line into buf. Return true if we are exiting because of a newline; else return false.
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	private boolean readAndUnescapeLine(ByteBuffer buf) throws IOException {
		buf.clear();
		byte next = -1;
		boolean hasNext = true;
		// This is equivalent to an is.read()
		if(currentReadPointer < bytesRead) { 
			next = isBuf[currentReadPointer++];
		} else { 
			fetchData();
			if(currentReadPointer < bytesRead) { 
				next = isBuf[currentReadPointer++];
			} else { 
				hasNext = false;
			}
		}
		// End of is.read()

		while(hasNext) {
			byte b = next;
			if(b == ESCAPE_CHAR) {
				// This is equivalent to an is.read()
				if(currentReadPointer < bytesRead) { 
					next = isBuf[currentReadPointer++];
				} else { 
					fetchData();
					if(currentReadPointer < bytesRead) { 
						next = isBuf[currentReadPointer++];
					} else { 
						hasNext = false;
					}
				}
				// End of is.read()
				if(!hasNext) { throw new IOException("Escape character terminated early"); }
				b = next;
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
			
			// This is equivalent to an is.read()
			if(currentReadPointer < bytesRead) { 
				next = isBuf[currentReadPointer++];
			} else { 
				fetchData();
				if(currentReadPointer < bytesRead) { 
					next = isBuf[currentReadPointer++];
				} else { 
					hasNext = false;
				}
			}
			// End of is.read()
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
				if(this.infoChangeHandler != null) this.infoChangeHandler.handleInfoChange(info);
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
		
		ByteString byteString = ByteString.copyFrom(buf);
		try { 
			switch(info.getType()) { 
			case SCALAR_BYTE: { 
				nextMsg = new EpicsMessage(ScalarByte.parseFrom(byteString), info);
				return;
			}
			case SCALAR_DOUBLE: { 
				nextMsg = new EpicsMessage(ScalarDouble.parseFrom(byteString), info);
				return;
			}
			case SCALAR_ENUM: { 
				nextMsg = new EpicsMessage(ScalarEnum.parseFrom(byteString), info);
				return;
			}
			case SCALAR_FLOAT: { 
				nextMsg = new EpicsMessage(ScalarFloat.parseFrom(byteString), info);
				return;
			}
			case SCALAR_INT: { 
				nextMsg = new EpicsMessage(ScalarInt.parseFrom(byteString), info);
				return;
			}
			case SCALAR_SHORT: { 
				nextMsg = new EpicsMessage(ScalarShort.parseFrom(byteString), info);
				return;
			}
			case SCALAR_STRING: { 
				nextMsg = new EpicsMessage(ScalarString.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_BYTE: { 
				nextMsg = new EpicsMessage(VectorChar.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_DOUBLE: { 
				nextMsg = new EpicsMessage(VectorDouble.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_ENUM: { 
				nextMsg = new EpicsMessage(VectorEnum.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_FLOAT: { 
				nextMsg = new EpicsMessage(VectorFloat.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_INT: { 
				nextMsg = new EpicsMessage(VectorInt.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_SHORT: { 
				nextMsg = new EpicsMessage(VectorShort.parseFrom(byteString), info);
				return;
			}
			case WAVEFORM_STRING: { 
				nextMsg = new EpicsMessage(VectorString.parseFrom(byteString), info);
				return;
			}
			case V4_GENERIC_BYTES: { 
				nextMsg = new EpicsMessage(V4GenericBytes.parseFrom(byteString), info);
				return;
			}
			default:
				throw new IOException("Unknown type " + info.getType());
			}
		} catch(InvalidProtocolBufferException ex) { 
			logger.log(Level.WARNING, "Exception processing bytestring of size " + byteString.size() + " at position " + currentReadPointer + " with bytesRead " + bytesRead + " and filePos " + filePos, ex);
			throw ex;
		}
	}

	@Override
	public void close() throws IOException {
		if(is != null) { 
			is.close();
			is = null;
		}
	}

	@Override
	public void onInfoChange(InfoChangeHandler handler) {
		this.infoChangeHandler = handler;
	}
}
