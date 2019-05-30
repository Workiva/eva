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


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * InputStream implementation that reads bytes from an immutable ByteString
 */
public class ByteStringInputStream extends InputStream {
    ByteBuffer buf;

    public ByteStringInputStream(ByteString b) {
        this.buf = b.toByteBuffer();
    }

    @Override
    public int read() throws IOException {
        if(!buf.hasRemaining()) {
            return -1;
        }
        return buf.get() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(!buf.hasRemaining()) {
            return -1;
        }
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }
}
