package com.amazon.kinesis.streaming.agent.tailing;

import java.nio.ByteBuffer;

import com.amazon.kinesis.streaming.agent.ByteBuffers;

/**
 * Returns a record for several lines.
 * TODO: Limitation of this splitter is that it requires a newline at the end
 *       of the file, otherwise it will miss the last record. We should be able
 *       to handle this at the level of the {@link IParser} implementation.
 * TODO: Should we parametrize the line delimiter?
 */
public class MultiLineSplitter implements ISplitter {
    public static final char LINE_DELIMITER = '\n';
    private final int maxLinesPerRecord;

    public MultiLineSplitter(int maxLinesPerRecord) {
      this.maxLinesPerRecord = maxLinesPerRecord;
    }

    @Override
    public int locateNextRecord(ByteBuffer buffer) {
        // TODO: Skip empty records, commented records, header lines, etc...
        //       (based on FileFlow configuration?).
        int startPosition = buffer.position();
        int lastPosition = -1;
        for (int i=0; i < maxLinesPerRecord; i++) {
          int currentPosition = ByteBuffers.advanceBufferToNextLine(buffer);
          if (currentPosition == -1) {
            if (lastPosition == -1) {
              return -1;
            } else {
              buffer.position(lastPosition);
              return lastPosition;
            }
          }
          int length = currentPosition - startPosition;
          if (length >= 1000000) {
            if (lastPosition == -1) {
              return currentPosition;
            } else {
              buffer.position(lastPosition);
              return lastPosition;
            }
          }
          lastPosition = currentPosition;
        }
        return lastPosition;
      }
    }
}
