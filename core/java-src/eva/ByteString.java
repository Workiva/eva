// Copyright 2015-2019 Workiva Inc.
// 
// Licensed under the Eclipse Public License 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://opensource.org/licenses/eclipse-1.0.php
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eva;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import com.google.common.io.ByteStreams;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Immutable wrapper for a byte-array.
 */
public final class ByteString implements IObj, Serializable {
    final byte[] bytes;
    final long crc32;
    final IPersistentMap _meta;

    private static long crc32(byte[] b) {
        CRC32 crc = new CRC32();
        crc.update(b);
        return crc.getValue();
    }
    ByteString(ByteBuffer b) {
        this.bytes = toByteArray(b);
        this.crc32 = crc32(this.bytes);
        this._meta = null;
    }

    ByteString(byte[] b, boolean copy) {
        this.bytes = copy ? Arrays.copyOf(b, b.length) : b;
        this.crc32 = crc32(this.bytes);
        this._meta = null;
    }

    private ByteString(byte[] b, long crc32, IPersistentMap meta) {
        this.bytes = b;
        this.crc32 = crc32 < 0 ? crc32(b) : crc32;
        this._meta = meta;
    }

    /**
     * Constructs an immutable ByteString from a ByteBuffer.
     *
     * The entire contents of the ByteBuffer will be copied, but the
     * state (position, mark, etc) of the ByteBuffer will not be modified.
     *
     * @param b the ByteBuffer to copy
     * @return ByteString containing a copy of the contents of the ByteBuffer
     */
    public static ByteString copyFrom(ByteBuffer b) {
        return new ByteString(b);
    }

    public static ByteString copyFrom(ByteBuffer b, int length) {
        byte[] bytes = new byte[length];
        b.get(bytes);
        return ByteString.wrapping(bytes);
    }

    /**
     * Constructs an immutable ByteString from a ByteBuffer.
     *
     * The entire contents of the ByteBuffer will be copied;
     * the passed byte-array will not be modified.
     *
     * @param b the byte-array to copy
     * @return ByteString containing a copy of the contents of the ByteBuffer
     */
    public static ByteString copyFrom(byte[] b) {
        return new ByteString(b, true);
    }

    public static ByteString copyFrom(byte[] b, int offset, int length) {
        byte[] bytes = Arrays.copyOfRange(b, offset, length);
        return ByteString.wrapping(bytes);
    }

    public static ByteString readFrom(InputStream in) throws IOException {
        return ByteString.wrapping(ByteStreams.toByteArray(in));
    }
    /**
     * Constructs an immutable ByteString containing the UTF-8 encoding of the String.
     *
     * If encoding the string to UTF-8 fails, a RuntimeException will be raised which
     * contains the original encoding-exception.
     *
     * @param s the String to copy as UTF-8 bytes
     * @return the UTF-8 encoded ByteString
     */
    public static ByteString copyFromUTF8(String s) {
        try {
            return new ByteString(s.getBytes("UTF-8"), false);
        } catch (UnsupportedEncodingException e) {
            throw(new RuntimeException(e));
        }
    }

    /**
     * Constructs a ByteString wrapping the passed byte-array.
     *
     * WARNING: the caller MUST ensure that passed byte-array is never modified once
     *          wrapped by the ByteString. This is an optimization to reduce unnecessary
     *          copying, but relies on the caller to ensure correctness.
     * @param b the byte-array to wrap as a ByteString
     * @return the ByteString containing the passed byte-array
     */
    public static ByteString wrapping(byte[] b) {
        return new ByteString(b, false);
    }

    public static ByteString intoByteString(Object source) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("eva.byte-string"));
        IFn toByteString = Clojure.var("eva.byte-string", "to-byte-string");
        return (ByteString)toByteString.invoke(source);
    }

    public int size() {
        return bytes.length;
    }

    /**
     * Returns a read-only ByteBuffer view of the contents of the ByteString.
     *
     * This view is not a copy and is the most efficient way to access the contents
     * of a ByteString.
     *
     * @return read-only ByteBuffer of ByteString contents
     */
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(this.bytes).asReadOnlyBuffer();
    }


    /**
     * Returns a copy of the ByteString as a byte-array.
     *
     * @return byte-array copy of the ByteString contents
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    static byte[] toByteArray(ByteBuffer buf) {
        ByteBuffer b = buf.asReadOnlyBuffer();
        b.clear();
        byte[] ba = new byte[b.capacity()];
        b.get(ba, 0, ba.length);
        return ba;
    }

    static boolean byteStringEquals(ByteString bs, Object o) {
        if (bs == o) {
            return true;
        } else
            return o instanceof ByteString && bs.toByteBuffer().equals(((ByteString) o).toByteBuffer());
//return Arrays.equals(bs.toByteArray(), ((ByteString)o).toByteArray());

    }

    static int byteStringCalcHash(ByteString bs, int seed) {
        int h = seed;

        if (h == 0) {
            final ByteBuffer b = bs.toByteBuffer();
            final int size = b.capacity();

            h = size;
            for (int i=0; i<size; i++) {
                h = h * 31 + b.get();
            }
            if (h == 0) {
                h = 1;
            }
        }
        return h;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ByteString && this.toByteBuffer().equals(((ByteString) o).toByteBuffer());
    }

    private volatile int hash = 0;

    @Override
    public int hashCode() {
        int h = ByteString.byteStringCalcHash(this, hash);
        hash = h;
        return h;
    }

    /**
     * Returns a ByteString of the digest hash of the ByteString.
     * @param algorithm name of the MessageDigest algorithm
     * @return ByteString containing the digest hash
     */
    public ByteString digest(String algorithm) {
        try {
            final MessageDigest md = MessageDigest.getInstance(algorithm);
            md.update(bytes);
            return ByteString.wrapping(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteString digest(String algorithm, java.security.Provider provider) {
        try {
            final MessageDigest md = MessageDigest.getInstance(algorithm, provider);
            md.update(bytes);
            return ByteString.wrapping(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the CRC32 checksum of the ByteString
     * @return CRC32 checksum;
     */
    public long checksum() {
        return this.crc32;
    }

    @Override
    public IObj withMeta(IPersistentMap meta) {
        return new ByteString(this.bytes, this.crc32, meta);
    }

    @Override
    public IPersistentMap meta() {
        return this._meta;
    }

//    public Iterable<ByteString> split(int chunkSize) {
//        if(chunkSize <= 0) {
//            throw new IllegalArgumentException("chunkSize must be greater than 0");
//        }
//        else if(chunkSize >= this.size()) {
//            return clojureList(this);
//        } else {
//            TransientList<ByteString> v = Collider.<ByteString>clojureList().asTransient();
//            ByteBuffer buf = this.toByteBuffer();
//            int remaining = buf.remaining();
//            while(remaining > 0) {
//                int amount = remaining >= chunkSize ? chunkSize : remaining;
//                byte[] chunk = new byte[amount];
//                buf.get(chunk);
//                v.append(ByteString.wrapping(chunk));
//                remaining = buf.remaining();
//            }
//            return v.toPersistent();
//        }
//
//    }

    public static ByteString join(Iterable<ByteString> chunks) {
        int totalSize = 0;
        for(ByteString chunk : chunks) {
            totalSize += chunk.size();
        }
        byte[] joined = new byte[totalSize];
        ByteBuffer joinedBuffer = ByteBuffer.wrap(joined);
        for(ByteString chunk : chunks) {
            joinedBuffer.put(chunk.toByteBuffer());
        }
        return ByteString.wrapping(joined);
    }
}
