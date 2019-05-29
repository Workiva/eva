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

package eva.error.v1;

import recide.sanex.ISanitized;
import java.lang.Throwable;
import java.lang.StackTraceElement;
import clojure.lang.IPersistentMap;

public class SanitizedEvaException extends EvaException implements ISanitized {
    private final EvaException unsuppressed;
    private final IPersistentMap suppressedData;
    private final Throwable suppressedCause;
    private final StackTraceElement[] suppressedStack;
    private final String suppressedMessage;

    public SanitizedEvaException(EvaException original,
				 IPersistentMap data,
				 Throwable cause,
				 StackTraceElement[] stack,
				 String message) {
	super(original.getErrorCode(), message, data, cause);
	this.unsuppressed = original;
	this.suppressedData = data;
	this.suppressedCause = cause;
	this.suppressedStack = stack;
	this.suppressedMessage = message;
    }

    public ISanitized getSanitized(IPersistentMap suppression) {
	return unsuppressed.getSanitized(suppression);
    }

    @Override
    public Throwable getUnsanitized() {
	return unsuppressed;
    }

    @Override
    public Throwable getCause() {
	return suppressedCause;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
	return suppressedStack;
    }

    @Override
    public String getMessage() {
	return suppressedMessage;
    }

    @Override
    public IPersistentMap getData() {
	return suppressedData;
    }

    @Override
    public String toString() {
	return "eva.error.v2.SuppressedEvaException: "
	    + this.getMessage()
	    + " "
	    + this.getData();
    }
}
