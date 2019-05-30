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

package eva.storage;

import clojure.lang.IFn;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ChunkCallbackOutputStream extends OutputStream {

    private final ByteBuffer buffer;
    private final IFn chunkCallback;
    private final IFn closedCallback;
    private boolean closed;

    public ChunkCallbackOutputStream(int chunkSize, IFn chunkCallback, IFn closedCallback) {
        this.buffer = ByteBuffer.allocate(chunkSize);
        this.chunkCallback = chunkCallback;
        this.closedCallback = closedCallback;
        this.closed = false;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if(closed) {
            throw new IllegalStateException(String.format("output stream is closed: %s", this));
        }
        if(!buffer.hasRemaining()) {
            flush();
        }
        buffer.put((byte) b);
    }

    private void _flush() {
        if(!closed && buffer.position() > 0) {
            buffer.flip();
            byte[] ba = new byte[buffer.limit()];
            buffer.get(ba);
            this.chunkCallback.invoke(ba);
        }
        buffer.clear();
    }

    @Override
    public synchronized void flush() {
        _flush();
    }

    @Override
    public synchronized void close() {
        flush();
        this.closedCallback.invoke();
        closed = true;
    }
}
