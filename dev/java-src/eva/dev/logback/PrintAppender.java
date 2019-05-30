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

package eva.dev.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class PrintAppender<E> extends AppenderBase<E> {

    private IFn print;

    @Override
    public void start() {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("eva.dev.logback.print-appender"));
        this.print = Clojure.var("eva.dev.logback.print-appender", "print-logging-event");
        super.start();
    }

    @Override
    protected void append(E e) {
        assert (e instanceof ILoggingEvent);
        ILoggingEvent evt = (ILoggingEvent)e;
        print.invoke(evt);
    }
}
