package com.bytedance.bytehouse.client;

import com.bytedance.bytehouse.settings.ByteHouseConfig;
import java.time.ZoneId;

/**
 * A Context object describing who the server is
 */
public class ServerContext {

    private final long majorVersion;

    private final long minorVersion;

    private final long reversion;

    private final ZoneId timeZone;

    private final String displayName;

    private final long versionPatch;

    private final ByteHouseConfig configure;

    public ServerContext(
            final long majorVersion,
            final long minorVersion,
            final long reversion,
            final ByteHouseConfig configure,
            final ZoneId timeZone,
            final String displayName,
            final long versionPatch
    ) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.reversion = reversion;
        this.configure = configure;
        this.timeZone = timeZone;
        this.displayName = displayName;
        this.versionPatch = versionPatch;
    }

    public long majorVersion() {
        return majorVersion;
    }

    public long minorVersion() {
        return minorVersion;
    }

    public long reversion() {
        return reversion;
    }

    public String version() {
        return majorVersion + "." + minorVersion + "." + reversion;
    }

    public ZoneId timeZone() {
        return timeZone;
    }

    public String displayName() {
        return displayName;
    }

    public long versionPatch() {
        return versionPatch;
    }

    public ByteHouseConfig getConfigure() {
        return configure;
    }
}
