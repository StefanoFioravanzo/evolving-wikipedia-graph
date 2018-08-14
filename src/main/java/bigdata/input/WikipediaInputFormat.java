/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package bigdata.input;

import java.io.InputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

/*
 * NOTE: Custom modifications
 *
 * - Added multiple level tag matching. Instead of just matching init and end tags,
 * the reader memorized when it is inside a page block and returns single revisions blocks
 * with the associated title as key
 * - Modified the readUntilMatch() method to always check the end of a page block
 */


/**
 * A simple {@link org.apache.hadoop.mapreduce.InputFormat} for XML documents ({@code
 * org.apache.hadoop.mapreduce} API). The class recognizes begin-of-document and end-of-document
 * tags only: everything between those delimiting tags is returned in an uninterpreted {@code Text}
 * object.
 *
 * @author Jimmy Lin
 */
public class WikipediaInputFormat extends FileInputFormat<Text, Text> {
     /**
     * Create a record reader for a given split. The framework will call
     * {@link RecordReader#initialize(InputSplit, TaskAttemptContext)} before
     * the split is used.
     *
     * @param split the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new XMLRecordReader();
    }

    /**
     * Simple {@link org.apache.hadoop.mapreduce.RecordReader} for XML documents ({@code
     * org.apache.hadoop.mapreduce} API). Recognizes begin-of-document and end-of-document tags only:
     * everything between those delimiting tags is returned in a {@link Text} object.
     *
     * @author Jimmy Lin
     */
    public static class XMLRecordReader extends RecordReader<Text, Text> {
        private static final Logger LOG = Logger.getLogger(XMLRecordReader.class);

        private byte[] pageStartTag;
        private byte[] pageEndTag;
        private byte[] revisionStartTag;
        private byte[] revisionEndTag;
        private byte[] titleStartTag;
        private byte[] titleEndTag;
        private long start;
        private long end;
        private long pos;
        private InputStream fsin = null;
        private DataOutputBuffer buffer = new DataOutputBuffer();

//        private byte[] currentTitle;
        private boolean insidePage = false;

        private CompressionCodec codec = null;
        private Decompressor decompressor = null;

        private Text currentTitle = new Text();
        private final Text key = new Text();
        private final Text value = new Text();

        /**
         * Called once at initialization.
         *
         * @param input the split that defines the range of records to read
         * @param context the information about the task
         * @throws IOException
         */
        @Override
        public void initialize(InputSplit input, TaskAttemptContext context) throws IOException {
            // BasicConfiguration for Log4j
            BasicConfigurator.configure();
            Configuration conf = context.getConfiguration();

            pageStartTag = "<page>".getBytes(StandardCharsets.UTF_8);
            pageEndTag = "</page>".getBytes(StandardCharsets.UTF_8);
            titleStartTag = "<title>".getBytes(StandardCharsets.UTF_8);
            titleEndTag = "</title>".getBytes(StandardCharsets.UTF_8);
            revisionStartTag = "<revision>".getBytes(StandardCharsets.UTF_8);
            revisionEndTag = "</revision>".getBytes(StandardCharsets.UTF_8);

            FileSplit split = (FileSplit) input;
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();

            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
            codec = compressionCodecs.getCodec(file);

            FileSystem fs = file.getFileSystem(conf);

            if (isCompressedInput()) {
                LOG.info("Reading compressed file " + file + "...");
                FSDataInputStream fileIn = fs.open(file);
                decompressor = CodecPool.getDecompressor(codec);
                if (codec instanceof SplittableCompressionCodec) {
                    // We can read blocks
                    final SplitCompressionInputStream cIn =
                            ((SplittableCompressionCodec)codec)
                                    .createInputStream(
                                            fileIn,
                                            decompressor,
                                            start,
                                            end,
                                            SplittableCompressionCodec.READ_MODE.BYBLOCK);
                    fsin = cIn;
                    start = cIn.getAdjustedStart();
                    end = cIn.getAdjustedEnd();
                } else {
                    // We cannot read blocks, we have to read everything
                    LOG.info("Cannot read file into block. Reading the whole input file...");
                    fsin = new DataInputStream(codec.createInputStream(fileIn, decompressor));

                    end = Long.MAX_VALUE;
                }
            } else {
                LOG.info("Reading uncompressed file " + file + "...");
                FSDataInputStream fileIn = fs.open(file);

                fileIn.seek(start);
                fsin = fileIn;

                end = start + split.getLength();
            }

            // Because input streams of gzipped files are not seekable, we need to keep track of bytes
            // consumed ourselves.
            pos = start;
        }

        /**
         * Read the next key, value pair.
         *
         * @return {@code true} if a key/value pair was read
         * @throws IOException Exception
         */
        @Override
        public boolean nextKeyValue() throws IOException {
            if (getFilePosition() < end) {

                //TODO: Improve this logic
                while(true) {
                    if (!insidePage) {
                        // read the next page title
                        // in case the previous page was the last one
                        // the function will return false and the stream
                        // will end
                        if (!findNextPage())
                            return false;
                    }

                    //TODO: Assuming there are no pages without revisions
                    // get next revision in page
                    int res = readUntilMatch(revisionStartTag, false);
                    if (res == 1) {
                        try {
                            buffer.write(revisionStartTag);
                            if (readUntilMatch(revisionEndTag, true) == 1) {
                                System.out.println("\tRevision found");
                                key.set(currentTitle);
                                value.set(buffer.getData(), 0, buffer.getLength());
                                return true;
                            }
                        } finally {
                            buffer.reset();
                        }
                    } else if (res == 2) {
                        // page is finished
                        buffer.reset();
                        insidePage = false;
                        System.out.println("End of article: " + currentTitle);

                        // Since we have now parsed the end of the previous page,
                        // the logic goes back to the top of the loop and will start
                        // parsing the next page
                    } else if (res == 0) {
                        // this should never happen
                        // here we are looping through the revisions
                        // of a page, so the stream should not stop without
                        // encountering a </page> tag
                        throw new IOException("Input stream ended with no closing </paga> tag");
                    }
                }
            }
            return false;
        }

        // TODO: Include parsing of <redirect> tag to automatically exclude pages that are just redirects to other pages?
        private boolean findNextPage() throws IOException {
            int res = readUntilMatch(pageStartTag, false);
            if (res == 1) {
                // started new page
                // get the page title
                if (readUntilMatch(titleStartTag, false) == 1) {
                    if (readUntilMatch(titleEndTag, true) == 1) {
                        // remove end tag '</title>' from title
                        currentTitle.set(buffer.getData(), 0, buffer.getLength() - titleEndTag.length);
//                        currentTitle = Arrays.copyOfRange(
//                                buffer.getData(),
//                                0,
//                                // need to trim the resulting string because of trailing empty bytes
//                                new String(buffer.getData()).trim().length() - titleEndTag.length);
                        System.out.println("Streaming Article: " + currentTitle);
                        buffer.reset();
                        insidePage = true;
                    }
                }
            } else if (res == 0){
                // file stream ended
                System.out.println("Input stream ended.");
                return false;
            }
            return true;
        }

        /**
         * Returns the current key.
         *
         * @return the current key or {@code null} if there is no current key
         */
        @Override
        public Text getCurrentKey() {
            return new Text(currentTitle);
        }

        /**
         * Returns the current value.
         *
         * @return current value
         */
        @Override
        public Text getCurrentValue() {
            return value;
        }

        /**
         * Closes the record reader.
         */
        @Override
        public void close() throws IOException {
            fsin.close();
        }

        /**
         * The current progress of the record reader through its data.
         *
         * @return a number between 0.0 and 1.0 that is the fraction of the data read
         * @throws IOException Exception
         */
        @Override
        public float getProgress() throws IOException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
            }
        }

        private boolean isCompressedInput() {
            return (codec != null);
        }

        protected long getFilePosition() throws IOException {
            long retVal;
            if (isCompressedInput() && null != fsin && fsin instanceof Seekable) {
                retVal = ((Seekable)fsin).getPos();
            } else {
                retVal = pos;
            }
            return retVal;
        }

        /**
         * Reads byte by byte the input stream matching either
         * the match input byte array or the end page tag
         *
         * @param match byte array with the matching tag
         * @param withinBlock whether to include the text into the buffer
         * @return 0: Error, 1: match array was matched, 2: pageEndTag was matched
         * @throws IOException Exception
         */
        private int readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {
            int i = 0;
            int j = 0;
            while (true) {
                int b = fsin.read();

                // end of file:
                if (b == -1)
                    return 0;

                // increment position (bytes consumed)
                pos++;

                // save to buffer:
                if (withinBlock)
                    buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length)
                        return 1;
                } else
                    i = 0;

                // check if this page block has ended
                if (b == pageEndTag[j]) {
                    j++;
                    if (j >= pageEndTag.length)
                        return 2;
                } else {
                    j = 0;
                }

                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && getFilePosition() >= end)
                    return 0;
            }
        }
    }
}