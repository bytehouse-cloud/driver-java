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
package com.bytedance.bytehouse.client;

import javax.annotation.concurrent.Immutable;

/**
 * A Context binding the client's identity({@link ClientContext}) and the server's identity
 * ({@link ServerContext}) with a bridge ({@link NativeClient}).
 */
@Immutable
public class NativeContext {

    private final ClientContext clientCtx;

    private final ServerContext serverCtx;

    private final NativeClient nativeClient;

    public NativeContext(
            final ClientContext clientCtx,
            final ServerContext serverCtx,
            final NativeClient nativeClient
    ) {
        this.clientCtx = clientCtx;
        this.serverCtx = serverCtx;
        this.nativeClient = nativeClient;
    }

    public ClientContext clientCtx() {
        return clientCtx;
    }

    public ServerContext serverCtx() {
        return serverCtx;
    }

    public NativeClient nativeClient() {
        return nativeClient;
    }
}
