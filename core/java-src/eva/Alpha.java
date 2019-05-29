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
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.ExecutionException;
import java.util.Map;

// We make no promises about the contract, stability, or lifetime of functions
// provided in this namespace

public final class Alpha {

    private static IFn require = Clojure.var("clojure.core", "require");

    static {
        require.invoke(Clojure.read("eva.alpha"));
    }

    public static Map getTransactionExceptionData(ExecutionException e){
        IFn getData = Clojure.var("eva.alpha", "to-ex-data");
        return (Map) getData.invoke(e);
    }

    public static String getTransactionExceptionCause(ExecutionException e){
        IFn getCause = Clojure.var("eva.alpha", "to-ex-cause");
        return (String) getCause.invoke(e);
    }

    public static MetricRegistry internalMetricRegistry() {
        require.invoke(Clojure.read("eva.api"));
        require.invoke(Clojure.read("barometer.core"));
        IFn defaultRegistry = Clojure.var("barometer.core", "default-registry");
        return (MetricRegistry) defaultRegistry.invoke();
    }

    @SuppressWarnings("unchecked")
    public static Map<Id, Long> txResultToEntityIDTempIDMap (Map txResult){
        require.invoke(Clojure.read("eva.api"));
        IFn entityObjects = Clojure.var("eva.alpha", "tx-result->EntityID-tempids");
        return (Map<Id, Long>) entityObjects.invoke(txResult);
    }
}
