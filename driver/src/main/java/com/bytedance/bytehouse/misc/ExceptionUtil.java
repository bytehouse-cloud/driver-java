/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance.bytehouse.misc;

import com.bytedance.bytehouse.exception.ByteHouseException;
import com.bytedance.bytehouse.exception.ByteHouseSQLException;
import com.bytedance.bytehouse.exception.TransactionLabelExistsException;
import com.bytedance.bytehouse.settings.ByteHouseErrCode;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public class ExceptionUtil {

    public static RuntimeException unchecked(Exception checked) {
        return new RuntimeException(checked);
    }

    public static <T, R> Function<T, R> unchecked(CheckedFunction<T, R> checked) {
        return t -> {
            try {
                return checked.apply(t);
            } catch (Exception rethrow) {
                throw unchecked(rethrow);
            }
        };
    }

    public static <T, U, R> BiFunction<T, U, R> unchecked(CheckedBiFunction<T, U, R> checked) {
        return (t, u) -> {
            try {
                return checked.apply(t, u);
            } catch (Exception rethrow) {
                throw unchecked(rethrow);
            }
        };
    }

    public static <T> Supplier<T> unchecked(CheckedSupplier<T> checked) {
        return () -> {
            try {
                return checked.get();
            } catch (Exception rethrow) {
                throw unchecked(rethrow);
            }
        };
    }

    public static void rethrowSQLException(CheckedRunnable checked) throws ByteHouseSQLException {
        rethrowSQLException((CheckedSupplier<Void>) () -> {
            checked.run();
            return null;
        });
    }

    public static <T> T rethrowSQLException(CheckedSupplier<T> checked) throws ByteHouseSQLException {
        try {
            return checked.get();
        } catch (Exception rethrow) {
            final ByteHouseSQLException sqlex = ExceptionUtil
                    .recursiveFind(rethrow, ByteHouseSQLException.class);
            if (sqlex != null) {
                throw translate(sqlex);
            }
            final ByteHouseException ex = ExceptionUtil
                    .recursiveFind(rethrow, ByteHouseException.class);
            if (ex != null) {
                throw new ByteHouseSQLException(ex.errCode(), rethrow.getMessage(), rethrow);
            }

            throw new ByteHouseSQLException(
                    ByteHouseErrCode.UNKNOWN_ERROR.code(),
                    rethrow.getMessage(),
                    rethrow
            );
        }
    }

    /**
     * based on the error code, translate to specific exception.
     */
    public static ByteHouseSQLException translate(final ByteHouseSQLException exception) {
        int code = exception.getErrorCode();
        if (code == ByteHouseErrCode.INSERTION_LABEL_ALREADY_EXISTS.code()) {
            return new TransactionLabelExistsException(exception.getMessage(), exception);
        } else {
            return exception;
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> T recursiveFind(final Throwable th, final Class<T> expectedClz) {
        Throwable nest = th;
        while (nest != null) {
            if (expectedClz.isAssignableFrom(nest.getClass())) {
                return (T) nest;
            }
            nest = nest.getCause();
        }
        return null;
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {

        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> {

        R apply(T t, U u) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedBiConsumer<T, U> {

        void accept(T t, U u) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedRunnable {

        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {

        T get() throws Exception;
    }
}
