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

import ch.qos.logback.classic.LoggerContext;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Server {
    static Logger LOG = LoggerFactory.getLogger(Server.class);

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("eva.server.v2"));
    }

    private static void shutdown(int delayMs) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.stop();
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            // continue if interrupted
        }
        System.exit(1);
    }

    public static void main(String[] args) {
        try {
            IFn startServer = Clojure.var("eva.server.v2", "start-server!");
            startServer.invoke(args);
        }
        catch(Throwable t) {
            System.out.println("Unhandled server error; printed this message to stdout; should also see an error log :/");
            LOG.error("Unhandled Server error; Aborting!", t);
            shutdown(5000);
        }
    }
}
